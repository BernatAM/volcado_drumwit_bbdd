"""Auditoría GET del histórico visible; no exporta filas ni autoriza producción."""
import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from tools.migration_compare import ValidationError


class ReadOnlyDestination:
    def __init__(self, env, max_rows=200000):
        values = dotenv_values(env)
        if not values.get('SUPABASE_URL') or not values.get('SUPABASE_KEY'):
            raise ValidationError('Configuración Supabase incompleta')
        self.url = values['SUPABASE_URL'].rstrip('/') + '/rest/v1/'
        self.headers = {'apikey': values['SUPABASE_KEY'],
                        'Authorization': 'Bearer ' + values['SUPABASE_KEY'],
                        'Prefer': 'count=exact'}
        self.max_rows = max_rows
        self.session = requests.Session()

    def get(self, table, params):
        response = self.session.get(self.url + table, headers=self.headers,
                                    params=params, timeout=(8, 30), allow_redirects=False)
        if response.status_code not in (200, 206):
            raise ValidationError(f'{table}: lectura HTTP {response.status_code}')
        content_range = response.headers.get('Content-Range', '')
        total = content_range.rsplit('/', 1)[-1]
        if not total.isdigit():
            raise ValidationError(f'{table}: conteo exacto no disponible')
        rows = response.json()
        if not isinstance(rows, list):
            raise ValidationError(f'{table}: respuesta inesperada')
        return rows, int(total)

    def read_table(self, table, columns, order):
        params = {'select': columns, 'order': order, 'limit': 0}
        _, total = self.get(table, params)
        if total > self.max_rows:
            raise ValidationError(f'{table}: supera max_rows; no se auditan muestras como histórico completo')
        rows = []
        while len(rows) < total:
            batch, current_total = self.get(table, dict(params, limit=1000, offset=len(rows)))
            if current_total != total or not batch:
                raise ValidationError(f'{table}: cambió el conteo o la paginación quedó incompleta')
            rows.extend(batch)
        _, final_total = self.get(table, params)
        if len(rows) != total or final_total != total:
            raise ValidationError(f'{table}: histórico cambió durante la lectura')
        print(f'{table}: {total} filas leídas', flush=True)
        return rows


def summarize(clients, reservations, queue, normalize_phone):
    phone_counts = Counter(normalize_phone(row.get('telefono')) for row in clients)
    client_ids = {row['cliente_id'] for row in clients}
    pairs = Counter((row['cliente_id'], row['id_reserva']) for row in reservations)
    reservation_ids = {str(row['id_reserva']) for row in reservations if row.get('id_reserva')}
    queue_keys = Counter(row['id'] for row in queue)
    absent_queue = [row for row in queue if row.get('reserva_id') and str(row['reserva_id']) not in reservation_ids]
    return {
        'clientes': {
            'rows': len(clients),
            'duplicate_client_ids': len(clients) - len(client_ids),
            'without_normalizable_phone': phone_counts.pop(None, 0),
            'duplicate_normalized_phones': sum(n - 1 for n in phone_counts.values()),
        },
        'reservas': {
            'rows': len(reservations),
            'duplicate_conflict_keys': sum(n - 1 for n in pairs.values()),
            'without_identifier': sum(not str(row.get('id_reserva') or '').strip() for row in reservations),
            'unknown_cliente_id': sum(row['cliente_id'] not in client_ids for row in reservations),
            'gift_rows': sum(row.get('regalo') in (1, True, '1') for row in reservations),
            'gift_rows_with_trip_dates': sum(row.get('regalo') in (1, True, '1') and bool(row.get('fecha_ida') or row.get('fecha_vuelta')) for row in reservations),
        },
        'cola': {
            'rows': len(queue),
            'duplicate_primary_keys': sum(n - 1 for n in queue_keys.values()),
            'rows_with_reservation_reference': sum(bool(row.get('reserva_id')) for row in queue),
            'references_absent_from_visible_reservations': sum(bool(row.get('reserva_id')) and str(row['reserva_id']) not in reservation_ids for row in queue),
            'active_unsent_references_absent': sum(row.get('is_send') is False and row.get('is_canceled') is False for row in absent_queue),
            'absent_reference_flow_types': dict(Counter(str(row.get('flow_type')) for row in absent_queue)),
            'flow_type_counts': dict(Counter(str(row.get('flow_type')) for row in queue)),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env', type=Path, default=REPO/'.env')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--max-rows', type=int, default=200000)
    args = parser.parse_args()
    if args.max_rows <= 0:
        parser.error('max-rows debe ser positivo')
    report = {'started_at': datetime.now(timezone.utc).isoformat(), 'read_only': True,
              'status': 'INCOMPLETE', 'blockers': [], 'review_items': [], 'checks': {},
              'notes': [
                  'Sólo filas visibles con la clave actual. Los conteos no acreditan restricciones UNIQUE ni permisos de escritura.',
                  'REST no ofrece snapshot común entre tablas; se verifica que el conteo no cambie, pero no se descartan actualizaciones concurrentes.',
                  'Las referencias de cola ausentes pueden pertenecer a histórico archivado o flujos manuales: requieren revisión, nunca borrado automático.',
                  'La reconciliación de códigos nuevos y enlaces reales con cliente_id requiere acceso a MySQL.',
              ]}
    source = None
    try:
        source = ReadOnlyDestination(args.env, args.max_rows)
        clients = source.read_table('clientes', 'cliente_id,telefono', 'cliente_id.asc')
        reservations = source.read_table('cliente_reservas', 'cliente_id,id_reserva,regalo,fecha_ida,fecha_vuelta', 'cliente_id.asc,id_reserva.asc')
        queue = source.read_table('opcionesinicio_client', 'id,reserva_id,flow_type,is_send,is_canceled', 'id.asc')
        from tools.validate_migration import load_etl
        etl, _ = load_etl((REPO/'volcado_reservas.py').read_text(encoding='utf-8'), 'audit_phone', 24)
        report['checks'] = summarize(clients, reservations, queue, etl.normalize_phone)
        for section, checks in report['checks'].items():
            for name in ('duplicate_client_ids', 'duplicate_normalized_phones', 'duplicate_conflict_keys', 'without_identifier', 'unknown_cliente_id', 'duplicate_primary_keys'):
                if checks.get(name):
                    report['blockers'].append(f'{section}.{name} = {checks[name]}')
        if report['checks']['cola']['references_absent_from_visible_reservations']:
            report['review_items'].append('Hay referencias de cola ausentes del histórico visible; revisar causa y mensajes pendientes antes de activar planificadores')
        report['status'] = 'REVIEW_REQUIRED' if report['blockers'] or report['review_items'] else 'HISTORY_READ_COMPLETED'
    except Exception as exc:
        report['blockers'].append(str(exc) if isinstance(exc, ValidationError) else type(exc).__name__)
    finally:
        if source:
            source.session.close()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'status': report['status'], 'blockers': report['blockers']}, ensure_ascii=False))
    return 3 if report['status'] == 'INCOMPLETE' else 2 if report['blockers'] else 0


if __name__ == '__main__':
    raise SystemExit(main())
