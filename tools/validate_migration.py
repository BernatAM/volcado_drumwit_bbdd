"""Ensayo legacy/TEST: SELECT en snapshots y captura local de payloads, sin upserts.

Ejecutar desde la raíz del servicio. Códigos de salida: 0=paridad comprobada,
2=diferencias/invariantes, 3=no se pudo completar. Nunca autoriza un despliegue.
"""
import argparse
import ast
import hashlib
import html
import json
import logging
import re
import subprocess
import sys
import time as clock
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pymysql
from dotenv import dotenv_values

REPO = Path(__file__).resolve().parents[1]
LEGACY_REF = json.loads((REPO / "tools/legacy_baseline.json").read_text(encoding="utf-8"))["git_ref"]
sys.path.insert(0, str(REPO))
from tools.migration_compare import ValidationError, compare_by_key, logical_reservations, normalize, reconcile_history


class ReadOnlySource:
    def __init__(self, env_path, max_rows=200000):
        settings = dotenv_values(env_path)
        required = ("MYSQL_HOST", "MYSQL_PORT", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE")
        if any(not settings.get(key) for key in required):
            raise ValidationError("Configuración MySQL incompleta")
        self.max_rows = max_rows
        self.metrics = []
        self.conn = pymysql.connect(
            host=settings["MYSQL_HOST"], port=int(settings["MYSQL_PORT"]),
            user=settings["MYSQL_USER"], password=settings["MYSQL_PASSWORD"],
            database=settings["MYSQL_DATABASE"], charset="utf8mb4",
            connect_timeout=8, read_timeout=60, write_timeout=8, autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with self.conn.cursor() as cur:
                cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                cur.execute("SET SESSION TRANSACTION READ ONLY")
                cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
            self.info = self.query("SELECT DATABASE() AS database_name, VERSION() AS version, NOW() AS server_now, @@session.time_zone AS time_zone")[0]
            self.info["tls_cipher"] = self.query("SHOW SESSION STATUS LIKE 'Ssl_cipher'")[0].get("Value")
            self.columns = self.query("SELECT TABLE_NAME AS table_name, COLUMN_NAME AS column_name FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()")
            self.tables = self.query("SELECT TABLE_NAME AS table_name, ENGINE AS engine FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()")
            self.table_names = {row["table_name"] for row in self.tables}
            self.schema = "drumwit8" if {"booking", "gift", "traveler"} <= self.table_names else "drumwit7" if {"reserva", "pasajero", "pasajeros__reserva"} <= self.table_names else "unknown"
        except Exception:
            self.close()
            raise

    def query(self, sql, params=None, bounded=False):
        sql = sql.strip().rstrip(";")
        if not re.match(r"^(SELECT|SHOW|EXPLAIN)\b", sql, re.I):
            raise ValidationError("Se rechazó una consulta que no es de lectura")
        if ";" in sql or re.search(r"\b(INTO\s+(OUTFILE|DUMPFILE)|FOR\s+UPDATE|LOCK\s+IN\s+SHARE|LOAD_FILE)\b", sql, re.I):
            raise ValidationError("Consulta con efectos o bloqueos no permitida")
        if bounded:
            sql = f"SELECT * FROM ({sql}) audit_bounded LIMIT {self.max_rows + 1}"
        start = clock.monotonic()
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        self.metrics.append({"seconds": round(clock.monotonic() - start, 3), "rows": len(rows), "sql_sha256": hashlib.sha256(sql.encode()).hexdigest()})
        if bounded and len(rows) > self.max_rows:
            raise ValidationError("Extracción supera max_rows; no se acepta una comparación truncada")
        return rows

    def close(self):
        try:
            self.conn.rollback()
        finally:
            self.conn.close()


IMPORTS = {"__future__", "os", "sys", "re", "time", "json", "logging", "typing", "datetime", "pandas"}
FUNCTIONS = {
    "normalize_phone", "_to_camel_case_name", "_normalize_lang", "now_utc", "df_dropna_phones",
    "_is_adult", "sanitize_df_for_json", "extract_reservas", "extract_pasajero_reserva", "extract_pasajeros",
    "build_nombre_reserva_por_reserva", "_build_display_name_bounded", "build_clientes_from_reservas",
    "chunked", "combine_date_time", "_is_zero_like", "clean_date_mysql", "clean_datetime_mysql",
    "clean_time_mysql", "_safe_set_fecha_vuelta", "clean_id_reserva_compradora", "build_where",
    "reservas_base_rows", "reservas_enrichment_rows", "_pipeline", "pipeline_incremental", "pipeline_nocturno",
}
CONSTANTS = {"COUNTRY_PREFIX_BY_LANG", "ZERO_DATE_STRS", "SQL_BOOKING_BASE", "SQL_GIFT_BASE", "SQL_RESERVA_BASE", "SQL_BOOKING_ENRICH"}


def load_etl(source, label, hours):
    """Carga sólo definiciones; excluye .env, conexiones, SDK y bloques __main__."""
    selected = []
    for node in ast.parse(source).body:
        if isinstance(node, ast.Import) and all(item.name.split(".")[0] in IMPORTS for item in node.names):
            selected.append(node)
        elif isinstance(node, ast.ImportFrom) and node.module in IMPORTS:
            selected.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in FUNCTIONS:
            selected.append(node)
        elif isinstance(node, ast.Assign) and all(isinstance(target, ast.Name) and target.id in CONSTANTS for target in node.targets):
            selected.append(node)
    namespace = {"__name__": label, "INCREMENTAL_HOURS": hours, "log": logging.getLogger(label)}
    exec(compile(ast.Module(body=selected, type_ignores=[]), label, "exec"), namespace)
    return SimpleNamespace(**namespace), namespace


def source_code(schema, filename, legacy_ref):
    if schema == "drumwit8":
        return (REPO / filename).read_text(encoding="utf-8")
    if schema == "drumwit7":
        result = subprocess.run(["git", "show", f"{legacy_ref}:{filename}"], cwd=REPO, check=True, capture_output=True)
        return result.stdout.decode("utf-8")
    raise ValidationError("Esquema de origen no reconocido")


def fixed_query(source, query, as_of, cutoff_column=None):
    params = {"as_of": as_of}
    query = query.replace("NOW()", "%(as_of)s").replace("CURDATE()", "DATE(%(as_of)s)")
    if cutoff_column:
        query = f"SELECT * FROM ({query.strip()}) audit_cutoff WHERE {cutoff_column} < %(as_of)s"
    return source.query(query, params, bounded=True)


def dataframe_reader(source, as_of):
    def run(query):
        cutoff = "fecha_reserva" if "AS fecha_reserva" in query or "r.fecha_reserva" in query else None
        rows = fixed_query(source, query, as_of, cutoff)
        if rows:
            return pd.DataFrame(rows)
        # Metadatos de columnas sin leer ni truncar resultados adicionales.
        wrapped = f"SELECT * FROM ({query.strip()}) audit_empty LIMIT 0"
        wrapped = wrapped.replace("NOW()", "%(as_of)s").replace("CURDATE()", "DATE(%(as_of)s)")
        with source.conn.cursor() as cur:
            cur.execute(wrapped, {"as_of": as_of})
            return pd.DataFrame(columns=[column[0] for column in cur.description])
    return run


def collect_clients(source, as_of, hours, legacy_ref, modes=("full", "incremental")):
    code = source_code(source.schema, "volcado_clientes.py", legacy_ref)
    module, namespace = load_etl(code, f"audit_clients_{source.schema}", hours)
    namespace["now_utc"] = lambda: as_of.replace(tzinfo=timezone.utc)
    read = dataframe_reader(source, as_of)
    passengers = module.extract_pasajeros(read)
    relations = module.extract_pasajero_reserva(read)
    outputs = {}
    for mode in modes:
        reservations = module.extract_reservas(read, full=mode == "full")
        if reservations.empty:
            outputs[mode] = []
            continue
        names = module.build_nombre_reserva_por_reserva(reservations, relations, passengers)
        clients = module.build_clientes_from_reservas(names)
        outputs[mode] = normalize(module.sanitize_df_for_json(clients).to_dict("records"))
    return outputs, hashlib.sha256(code.encode()).hexdigest()


def collect_reservations(source, as_of, hours, legacy_ref, phone_map, modes=("full", "incremental")):
    code = source_code(source.schema, "volcado_reservas.py", legacy_ref)
    module, namespace = load_etl(code, f"audit_reservations_{source.schema}", hours)
    outputs = {}
    raw = {}
    class PreviewCursor:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, query):
            self.rows = fixed_query(source, query, as_of, "fecha_reserva")
            current_raw.append(self.rows)
        def fetchall(self): return self.rows
    class PreviewConnection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def cursor(self): return PreviewCursor()
    namespace["mysql_conn"] = PreviewConnection
    namespace["load_cliente_map"] = lambda: phone_map
    for mode in modes:
        batches = []
        current_raw = []
        def capture(rows):
            batches.append(normalize(rows))
            return len(rows)
        namespace["upsert_reservas"] = capture
        (module.pipeline_nocturno if mode == "full" else module.pipeline_incremental)()
        if len(batches) != 2 or len(current_raw) != 2:
            raise ValidationError("El pipeline dejó de producir los dos lotes esperados")
        outputs[mode] = {"base": batches[0], "flights": batches[1]}
        raw[mode] = {
            "base_rows": len(current_raw[0]), "flight_rows": len(current_raw[1]),
            "skipped_base_rows": len(current_raw[0]) - len(batches[0]),
            "skipped_flight_rows": len(current_raw[1]) - len(batches[1]),
        }
    return outputs, raw, hashlib.sha256(code.encode()).hexdigest()


NEW_COLUMNS = {
    "booking": {"id", "code", "created_at", "updated_at", "departure_date", "trip_length_in_nights", "language", "status", "gift_id"},
    "gift": {"id", "code", "created_at", "updated_at", "language", "status", "adult_travelers_count", "child_travelers_count", "infant_travelers_count"},
    "booking_contact_details": {"booking_id", "nickname", "phone_country_code", "phone_number", "name", "traveler_id"},
    "gift_contact_details": {"gift_id", "nickname", "phone_country_code", "phone_number", "name", "last_name_1", "last_name_2"},
    "traveler": {"id", "booking_id", "name", "last_name_1", "last_name_2", "type"},
    "booking_flights": {"booking_id", "destination_id", "departure_time", "return_time"},
    "destination": {"id", "name_es"},
}


def preflight(source):
    result = {"schema": source.schema, "info": normalize(source.info), "quality": {}, "warnings": []}
    if source.schema == "unknown":
        raise ValidationError("El origen no contiene el modelo Drumwit7 ni Drumwit8")
    required_tables = set(NEW_COLUMNS) if source.schema == "drumwit8" else {"reserva", "pasajero", "pasajeros__reserva", "booking_flights", "destino"}
    if not required_tables <= source.table_names:
        raise ValidationError("Faltan tablas de la extracción")
    if source.schema == "drumwit8":
        actual = {}
        for row in source.columns:
            actual.setdefault(row["table_name"], set()).add(row["column_name"])
        missing = {table: sorted(columns - actual.get(table, set())) for table, columns in NEW_COLUMNS.items() if columns - actual.get(table, set())}
        result["missing_columns"] = missing
        if missing:
            raise ValidationError("Faltan columnas del nuevo ETL: " + json.dumps(missing, sort_keys=True))
        queries = {
            "invalid_or_duplicate_codes": "SELECT COUNT(*) AS n FROM (SELECT code FROM (SELECT code FROM booking UNION ALL SELECT code FROM gift) codes GROUP BY code HAVING code IS NULL OR TRIM(code)='' OR COUNT(*)>1) bad",
            "paid_bookings_without_contact": "SELECT COUNT(*) AS n FROM booking b LEFT JOIN booking_contact_details c ON c.booking_id=b.id WHERE b.status='PAID' AND c.booking_id IS NULL",
            "paid_gifts_without_contact": "SELECT COUNT(*) AS n FROM gift g LEFT JOIN gift_contact_details c ON c.gift_id=g.id WHERE g.status='PAID' AND c.gift_id IS NULL",
            "duplicate_booking_contacts": "SELECT COUNT(*) AS n FROM (SELECT booking_id FROM booking_contact_details GROUP BY booking_id HAVING COUNT(*)>1) bad",
            "duplicate_gift_contacts": "SELECT COUNT(*) AS n FROM (SELECT gift_id FROM gift_contact_details GROUP BY gift_id HAVING COUNT(*)>1) bad",
            "orphan_travelers": "SELECT COUNT(*) AS n FROM traveler t LEFT JOIN booking b ON b.id=t.booking_id WHERE b.id IS NULL",
            "orphan_flights": "SELECT COUNT(*) AS n FROM booking_flights f LEFT JOIN booking b ON b.id=f.booking_id WHERE b.id IS NULL",
            "invalid_traveler_types": "SELECT COUNT(*) AS n FROM traveler WHERE type IS NULL OR type NOT IN ('ADULT','CHILD','INFANT')",
            "paid_bookings_with_invalid_gift": "SELECT COUNT(*) AS n FROM booking b LEFT JOIN gift g ON g.id=b.gift_id WHERE b.status='PAID' AND b.gift_id IS NOT NULL AND b.gift_id<>0 AND g.id IS NULL",
            "paid_bookings_return_before_departure": "SELECT COUNT(*) AS n FROM booking WHERE status='PAID' AND trip_length_in_nights<0",
            "paid_bookings_without_creation_date": "SELECT COUNT(*) AS n FROM booking WHERE status='PAID' AND created_at IS NULL",
            "paid_gifts_without_creation_date": "SELECT COUNT(*) AS n FROM gift WHERE status='PAID' AND created_at IS NULL",
            "paid_gifts_negative_traveler_count": "SELECT COUNT(*) AS n FROM gift WHERE status='PAID' AND (adult_travelers_count<0 OR child_travelers_count<0 OR infant_travelers_count<0)",
        }
        result["quality"] = {name: source.query(query)[0]["n"] for name, query in queries.items()}
        indexes = source.query("SHOW INDEX FROM traveler")
        result["traveler_booking_id_index"] = any(row.get("Column_name") == "booking_id" and row.get("Seq_in_index") == 1 for row in indexes)
        if not result["traveler_booking_id_index"]:
            result["warnings"].append("traveler.booking_id no tiene índice inicial; revisar el COUNT correlacionado antes del histórico")
    non_transactional = sorted(row["table_name"] for row in source.tables if row["table_name"] in required_tables and row["engine"] != "InnoDB")
    if non_transactional:
        result["warnings"].append("Sin snapshot consistente para tablas no InnoDB: " + ", ".join(non_transactional))
    return result


def classify(rows):
    return [dict(id_reserva=row["id_reserva"], flow_type="regalo_confirmado" if row.get("regalo") else "canjeo" if row.get("id_reserva_compradora_regalo") else "confirmados") for row in rows]


def write_report(report, output):
    output.mkdir(parents=True, exist_ok=True)
    (output / "migration-report.json").write_text(json.dumps(normalize(report), ensure_ascii=False, indent=2), encoding="utf-8")
    checks = "".join(f"<tr><td>{html.escape(name)}</td><td>{c['left_rows']}</td><td>{c['right_rows']}</td><td>{c['matched_rows']}</td><td>{'IGUAL' if c['equal'] else 'DIFERENTE'}</td></tr>" for name, c in report.get("comparisons", {}).items())
    blockers = "".join(f"<li>{html.escape(item)}</li>" for item in report["blockers"])
    notes = "".join(f"<li>{html.escape(item)}</li>" for item in report.get("notes", []))
    diagnosis = "".join(f"<li>{html.escape(item)}</li>" for item in report.get("diagnosis_summary", []))
    diagnosis_html = f"<h2>Resultado explicado</h2><ul>{diagnosis}</ul>" if diagnosis else ""
    evidence = report.get('operational_evidence', {})
    evidence_html = ''
    if evidence:
        evidence_html = '<h2>Controles complementarios ejecutados</h2><ul>' + ''.join(
            f'<li>{html.escape(item)}</li>' for item in evidence.get('summary', [])
        ) + '</ul><p>Detalle: <a href="supabase-history.json">histórico Supabase</a>, <a href="supabase-preflight.json">preflight de lectura</a>.</p>'
        evidence_html += '<h2>Controles pendientes para activar producción</h2><ul>' + ''.join(
            f'<li>{html.escape(item)}</li>' for item in evidence.get('pending_controls', [])
        ) + '</ul>'
    page = f'''<!doctype html><html lang="es"><meta charset="utf-8"><title>Validación Drumwit</title>
<style>body{{font:16px system-ui;max-width:1050px;margin:40px auto;padding:0 24px;color:#192a3c}}h1{{font-size:28px}}table{{border-collapse:collapse;width:100%}}td,th{{padding:10px;text-align:left;border-bottom:1px solid #ddd}}.status{{padding:16px;background:#eef2f6;font-weight:700}}li{{margin:8px 0}}</style>
<h1>Validación del cambio de base de datos</h1><p>{html.escape(report['started_at'])}</p>
<p class="status">{html.escape(report['status'])}</p><p>Transacciones de solo lectura. No se han ejecutado upserts, cambios de esquema, planificadores ni envíos.</p>
{diagnosis_html}
<h2>Bloqueos</h2><ul>{blockers or '<li>Ninguno en la comparación ejecutada.</li>'}</ul>
<h2>Comparación LEGACY / TEST</h2><table><tr><th>Resultado</th><th>LEGACY</th><th>TEST</th><th>Filas iguales</th><th>Estado</th></tr>{checks}</table>
{evidence_html}<h2>Alcance y condiciones</h2><ul>{notes}</ul><p>Las diferencias por campo y huellas SHA-256 están en migration-report.json. No se exportan teléfonos, nombres, contraseñas ni claves.</p></html>'''
    (output / "migration-report.html").write_text(page, encoding="utf-8")


def run(args):
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(), "status": "INCOMPLETE",
        "legacy_git_ref": args.legacy_ref,
        "read_only": True, "blockers": [], "environments": {}, "comparisons": {},
        "notes": [
            "LEGACY es la referencia; TEST debe contener Drumwit8. No se comprueba la futura base PROD. Se comparan multiconjuntos de filas y duplicados.",
            "Se fija el mismo instante para las ventanas y created_at generado; quedan excluidas las altas posteriores a ese instante.",
            "Los cliente_id se simulan con un mapa común de teléfonos. Esta comparación no acredita el enlace real con clientes de Supabase.",
            "En la equivalencia lógica, sólo fecha_ida, fecha_vuelta y compradora ausentes se equiparan a NULL; el payload estricto se informa aparte.",
            "Un clon anterior al corte o escrituras concurrentes entre snapshots puede requerir un corte de negocio aprobado o repetir con la ingesta detenida.",
            "La paridad no despliega el servicio: faltan el preflight de Supabase, la reconciliación del histórico y el ensayo de rollback.",
        ],
    }
    sources = {}
    try:
        for name, path in (("legacy", args.prod_env), ("test", args.test_env)):
            try:
                source = ReadOnlySource(path, args.max_rows)
                sources[name] = source
                report["environments"][name] = preflight(source)
                print(f"{name}: conectado; esquema {source.schema}", flush=True)
            except Exception as exc:
                code = exc.args[0] if exc.args and isinstance(exc.args[0], int) else type(exc).__name__
                report["environments"][name] = {"error_code": code, "complete": False}
                if isinstance(exc, ValidationError):
                    report["environments"][name]["reason"] = str(exc)
                report["blockers"].append(f"{name}: no se pudo completar conexión/esquema ({code})")
        if report["blockers"]:
            return report, 3
        if sources["test"].schema != "drumwit8":
            raise ValidationError("TEST aún no contiene Drumwit8")
        report["comparison_mode"] = f"{sources['legacy'].schema}_to_drumwit8"
        report["legacy_behavior_compared"] = sources["legacy"].schema == "drumwit7"
        if not report["legacy_behavior_compared"]:
            report["notes"].append("La referencia ya usa Drumwit8. Para acreditar el cambio frente al ETL anterior hace falta una copia Drumwit7 previa al cambio.")
        if sources["legacy"].info["time_zone"] != sources["test"].info["time_zone"]:
            raise ValidationError("Zonas horarias MySQL distintas: no comparar ventanas sin reconciliarlas")
        as_of = datetime.fromisoformat(args.as_of) if args.as_of else min(source.info["server_now"] for source in sources.values())
        if as_of.tzinfo is not None:
            raise ValidationError("--as-of debe ser fecha/hora sin offset, en la zona del servidor MySQL")
        report["as_of_server_time"] = as_of.isoformat()
        clients = {}
        for name, source in sources.items():
            clients[name], digest = collect_clients(source, as_of, args.hours, args.legacy_ref)
            report["environments"][name]["client_source_sha256"] = digest
        phones = sorted({row["telefono"] for output in clients.values() for row in output["full"]})
        phone_map = {phone: index + 1 for index, phone in enumerate(phones)}
        reservations = {}
        for name, source in sources.items():
            reservations[name], raw, digest = collect_reservations(source, as_of, args.hours, args.legacy_ref, phone_map)
            report["environments"][name].update({"reservation_source_sha256": digest, "raw_counts": raw})
        strict = {}
        for mode in ("full", "incremental"):
            report["comparisons"][f"{mode}.clientes"] = compare_by_key(clients["legacy"][mode], clients["test"][mode], "telefono")
            for stage in ("base", "flights"):
                left, right = reservations["legacy"][mode][stage], reservations["test"][mode][stage]
                strict[f"{mode}.{stage}"] = compare_by_key(left, right, "id_reserva")
                report["comparisons"][f"{mode}.{stage}"] = compare_by_key(logical_reservations(left), logical_reservations(right), "id_reserva")
            report["comparisons"][f"{mode}.clasificacion"] = compare_by_key(classify(reservations["legacy"][mode]["base"]), classify(reservations["test"][mode]["base"]), "id_reserva")
        report["strict_payload_comparisons"] = strict
        if getattr(args, "supabase_env", None) and not getattr(args, "source_only", False):
            from tools.audit_supabase import ReadOnlyDestination
            destination = ReadOnlyDestination(args.supabase_env, args.max_rows)
            try:
                actual_clients = destination.read_table('clientes', 'cliente_id,telefono', 'cliente_id.asc')
                existing_reservations = destination.read_table('cliente_reservas', 'cliente_id,id_reserva', 'cliente_id.asc,id_reserva.asc')
                queue = destination.read_table('opcionesinicio_client', 'id,reserva_id', 'id.asc')
                phone_etl, _ = load_etl(source_code('drumwit8', 'volcado_reservas.py', args.legacy_ref), 'history_phone', args.hours)
                report['history_reconciliation'] = {}
                for name in sources:
                    check = reconcile_history(reservations[name]['full']['base'], phone_map, actual_clients, existing_reservations, queue, phone_etl.normalize_phone)
                    report['history_reconciliation'][name] = check
                    for field in ('existing_code_linked_to_different_client', 'rows_with_ambiguous_normalized_phone', 'existing_codes_linked_to_multiple_clients'):
                        if check[field]:
                            report['blockers'].append(f'{name}.history: {field} = {check[field]}')
                report['notes'].append('La reconciliación real usa GET de Supabase sin snapshot común. Códigos/teléfonos nuevos o histórico ausente del preview PAID requieren revisión de negocio; pueden ser altas recientes, cancelaciones o archivos.')
            finally:
                destination.session.close()
        for name, comparison in report["comparisons"].items():
            if not comparison["equal"]:
                report["blockers"].append(f"Diferencias en {name}")
            if comparison["left_duplicate_keys"] or comparison["right_duplicate_keys"]:
                report["blockers"].append(f"Claves duplicadas en {name}")
        for name, environment in report["environments"].items():
            for check, count in environment.get("quality", {}).items():
                if count:
                    report["blockers"].append(f"{name}: {check} = {count}")
            for mode, counts in environment.get("raw_counts", {}).items():
                for check in ("skipped_base_rows", "skipped_flight_rows"):
                    if counts[check]:
                        report["blockers"].append(f"{name}.{mode}: {check} = {counts[check]}")
            if environment.get("warnings"):
                report["blockers"].extend(f"{name}: {warning}" for warning in environment["warnings"])
        report["status"] = "DIFFERENCES_FOUND" if report["blockers"] else "SOURCE_PARITY_VERIFIED"
        return report, 2 if report["blockers"] else 0
    except Exception as exc:
        safe_message = str(exc) if isinstance(exc, ValidationError) else type(exc).__name__
        report["blockers"].append(safe_message)
        return report, 3
    finally:
        for name, source in sources.items():
            report["environments"][name]["queries"] = source.metrics
            try:
                source.close()
            except Exception:
                report["blockers"].append(f"{name}: no se pudo cerrar limpiamente la transacción de lectura")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--legacy-env", "--prod-env", dest="prod_env", type=Path, default=REPO / ".env.legacy", help="Conexión de referencia al modelo anterior; --prod-env es un alias antiguo")
    parser.add_argument("--test-env", type=Path, default=REPO / ".env.test")
    parser.add_argument("--supabase-env", type=Path, default=REPO / ".env", help="Destino para contrastar enlaces reales del histórico, sólo GET")
    parser.add_argument("--source-only", action="store_true", help="Comparar sólo LEGACY y TEST, sin incluir el histórico actual de Supabase")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--max-rows", type=int, default=200000)
    parser.add_argument("--as-of", help="Corte en hora del servidor: YYYY-MM-DDTHH:MM:SS")
    parser.add_argument("--legacy-ref", default=LEGACY_REF, help="Commit inmutable del código previo, para origen Drumwit7")
    args = parser.parse_args()
    if args.hours <= 0 or args.max_rows <= 0:
        parser.error("hours y max-rows deben ser positivos")
    report, code = run(args)
    if report["blockers"] and code == 0:
        report["status"], code = "INCOMPLETE", 3
    write_report(report, args.output)
    print(json.dumps({"status": report["status"], "blockers": report["blockers"], "report": str(args.output / "migration-report.html")}, ensure_ascii=False))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
