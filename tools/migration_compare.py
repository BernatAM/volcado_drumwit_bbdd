"""Comparación de resultados sin exportar valores personales al informe."""
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal


class ValidationError(Exception):
    pass


def normalize(value):
    if isinstance(value, dict):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return int(value.total_seconds())
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return int(value) if value.is_integer() else value
    if hasattr(value, "item"):
        return normalize(value.item())
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise TypeError(f"Tipo no comparable: {type(value).__name__}")


def canonical(record):
    return json.dumps(normalize(record), sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)


def compare_records(left, right):
    # Primer pase de OpenCode/qwen local, revisado: Counter conserva duplicados.
    left_counter = Counter(canonical(row) for row in left)
    right_counter = Counter(canonical(row) for row in right)
    matched = left_counter & right_counter
    left_only = left_counter - right_counter
    right_only = right_counter - left_counter
    def digest(counter):
        data = canonical(sorted(counter.items())).encode("utf-8")
        return hashlib.sha256(data).hexdigest()
    return {
        "left_rows": len(left), "right_rows": len(right),
        "matched_rows": sum(matched.values()),
        "left_only_rows": sum(left_only.values()),
        "right_only_rows": sum(right_only.values()),
        "equal": left_counter == right_counter,
        "left_sha256": digest(left_counter), "right_sha256": digest(right_counter),
    }


def compare_by_key(left, right, key):
    result = compare_records(left, right)
    def groups(rows):
        out = defaultdict(list)
        for row in rows:
            out[canonical(row.get(key))].append(row)
        return out
    left_groups, right_groups = groups(left), groups(right)
    result.update({
        "left_duplicate_keys": sum(len(rows) - 1 for rows in left_groups.values()),
        "right_duplicate_keys": sum(len(rows) - 1 for rows in right_groups.values()),
        "left_missing_keys": len(left_groups.keys() - right_groups.keys()),
        "right_missing_keys": len(right_groups.keys() - left_groups.keys()),
    })
    differences = Counter()
    for value in left_groups.keys() & right_groups.keys():
        if len(left_groups[value]) != 1 or len(right_groups[value]) != 1:
            continue
        a, b = left_groups[value][0], right_groups[value][0]
        for field in a.keys() | b.keys():
            # La ausencia de un campo es diferente de NULL en el payload estricto.
            if (field in a) != (field in b) or canonical(a.get(field)) != canonical(b.get(field)):
                differences[field] += 1
    result["changed_fields"] = dict(sorted(differences.items()))
    return result


def logical_reservations(rows):
    """Única equivalencia de forma permitida: opcionales ausentes frente a NULL."""
    return [dict(row, **{key: row.get(key) for key in (
        "fecha_ida", "fecha_vuelta", "id_reserva_compradora_regalo"
    )}) for row in rows]


def reconcile_history(preview_rows, simulated_phone_map, actual_clients, existing_reservations, queue, normalize_phone):
    """Relaciona el payload con claves reales; devuelve sólo conteos revisables."""
    phone_by_simulated_id = {value: phone for phone, value in simulated_phone_map.items()}
    actual_phone_ids = defaultdict(set)
    for client in actual_clients:
        phone = normalize_phone(client.get('telefono'))
        if phone:
            actual_phone_ids[phone].add(client['cliente_id'])
    existing_code_ids = defaultdict(set)
    for row in existing_reservations:
        existing_code_ids[str(row['id_reserva'])].add(row['cliente_id'])
    codes = {str(row['id_reserva']) for row in preview_rows}
    exact_links = conflicting_links = new_codes = new_phones = ambiguous_phones = 0
    for row in preview_rows:
        phone = phone_by_simulated_id[row['cliente_id']]
        actual_ids = actual_phone_ids.get(phone, set())
        code_ids = existing_code_ids.get(str(row['id_reserva']), set())
        new_phones += not actual_ids
        ambiguous_phones += len(actual_ids) > 1
        new_codes += not code_ids
        exact_links += len(actual_ids) == 1 and actual_ids <= code_ids
        conflicting_links += bool(code_ids) and not bool(actual_ids & code_ids)
    return {
        'preview_rows': len(preview_rows), 'existing_exact_client_code_links': exact_links,
        'rows_without_existing_client_phone': new_phones, 'rows_without_existing_code': new_codes,
        'existing_code_linked_to_different_client': conflicting_links,
        'rows_with_ambiguous_normalized_phone': ambiguous_phones,
        'existing_codes_linked_to_multiple_clients': sum(len(ids) > 1 for ids in existing_code_ids.values()),
        'existing_codes_absent_from_paid_preview': len(existing_code_ids.keys() - codes),
        'queue_references_absent_from_paid_preview': sum(bool(row.get('reserva_id')) and str(row['reserva_id']) not in codes for row in queue),
    }
