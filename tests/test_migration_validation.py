import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from tools.migration_compare import compare_by_key, compare_records, logical_reservations
from tools import validate_migration as audit
from test_drumwit8_etl import source_db  # registra el fixture SQLite en este módulo


def test_comparison_is_not_fooled_by_order_or_duplicate_row_counts():
    a, b = {"id": "A", "n": 1}, {"id": "B", "n": 2}
    assert compare_records([a, b, a], [b, a, a])["equal"]
    result = compare_records([a, b, a], [b, b, a])
    assert not result["equal"]
    assert result["matched_rows"] == 2
    assert result["left_only_rows"] == result["right_only_rows"] == 1


def test_null_empty_zero_are_distinct_and_key_order_is_irrelevant():
    assert compare_records([{"id": 1, "v": None}], [{"v": None, "id": 1}])["equal"]
    for value in ("", 0, False):
        assert not compare_records([{"v": None}], [{"v": value}])["equal"]


def test_fields_differ_without_exposing_personal_values():
    result = compare_by_key(
        [{"telefono": "34611111111", "nombre": "PERSONA UNO"}],
        [{"telefono": "34611111111", "nombre": "PERSONA DOS"}], "telefono",
    )
    assert result["changed_fields"] == {"nombre": 1}
    report = json.dumps(result)
    assert "PERSONA" not in report and "34611111111" not in report


def test_only_documented_optional_null_shape_is_normalized():
    original = [{"id_reserva": "B1", "regalo": 0}]
    new = [{"id_reserva": "B1", "regalo": 0, "fecha_ida": None, "fecha_vuelta": None, "id_reserva_compradora_regalo": None}]
    assert not compare_records(original, new)["equal"]
    assert compare_records(logical_reservations(original), logical_reservations(new))["equal"]
    assert not compare_records(logical_reservations(original), logical_reservations([dict(new[0], numero_viajeros=2)]))["equal"]


@pytest.mark.parametrize("query", ["UPDATE booking SET status='PAID'", "DELETE FROM gift", "SELECT 1; DELETE FROM gift", "SELECT 1 INTO OUTFILE '/tmp/data'", "SELECT 1 FOR UPDATE", "SELECT LOAD_FILE('/etc/passwd')"])
def test_read_only_wrapper_rejects_mutations_locks_and_file_access(query):
    source = audit.ReadOnlySource.__new__(audit.ReadOnlySource)
    source.conn = Mock()
    with pytest.raises(audit.ValidationError):
        source.query(query)
    source.conn.cursor.assert_not_called()


def test_full_comparison_never_silently_truncates():
    source = audit.ReadOnlySource.__new__(audit.ReadOnlySource)
    source.max_rows = 1
    source.metrics = []
    cursor = Mock()
    cursor.fetchall.return_value = [{"id": 1}, {"id": 2}]
    source.conn = Mock()
    source.conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
    source.conn.cursor.return_value.__exit__ = Mock(return_value=False)
    with pytest.raises(audit.ValidationError, match="max_rows"):
        source.query("SELECT id FROM booking", bounded=True)
    assert "LIMIT 2" in cursor.execute.call_args[0][0]


def test_loading_etl_never_executes_import_time_side_effects():
    code = '''
import os
from supabase import create_client
raise RuntimeError("must not run")
SUPABASE_KEY = os.environ["must not read"]
create_client("https://must-not-connect.invalid", "secret")
def normalize_phone(value): return str(value)
'''
    module, _ = audit.load_etl(code, "safe", 24)
    assert module.normalize_phone(123) == "123"


def test_read_only_setup_is_enforced_before_any_select(tmp_path, monkeypatch):
    env = tmp_path / ".env.test"
    env.write_text("MYSQL_HOST=offline.invalid\nMYSQL_PORT=3306\nMYSQL_USER=offline\nMYSQL_PASSWORD=offline\nMYSQL_DATABASE=offline\n")
    cursor = Mock()
    cursor.fetchall.return_value = []
    connection = Mock()
    connection.cursor.return_value.__enter__ = Mock(return_value=cursor)
    connection.cursor.return_value.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(audit.pymysql, "connect", Mock(return_value=connection))
    with pytest.raises(IndexError):
        audit.ReadOnlySource(env)
    statements = [call.args[0] for call in cursor.execute.call_args_list]
    assert statements[:3] == [
        "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        "SET SESSION TRANSACTION READ ONLY",
        "START TRANSACTION WITH CONSISTENT SNAPSHOT",
    ]
    assert statements[3].startswith("SELECT")
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_connection_failure_cannot_produce_success_report(tmp_path, monkeypatch):
    def fail(*args):
        raise audit.ValidationError("Sin acceso de red")
    monkeypatch.setattr(audit, "ReadOnlySource", fail)
    args = SimpleNamespace(prod_env=tmp_path/".env", test_env=tmp_path/".env.test", max_rows=20, legacy_ref=audit.LEGACY_REF)
    report, code = audit.run(args)
    assert code == 3 and report["status"] == "INCOMPLETE"
    assert len(report["blockers"]) == 2
    assert not report["comparisons"]
    audit.write_report(report, tmp_path)
    assert "INCOMPLETE" in (tmp_path/"migration-report.html").read_text()


def test_pipeline_preview_uses_actual_code_and_captures_writes(etl_modules, source_db):
    # Fixture del modelo nuevo con reserva/regalo que comparten ID.
    from test_drumwit8_etl import execute_mysql_fixture
    from datetime import datetime
    class Source:
        schema = "drumwit8"
        def query(self, sql, params=None, bounded=False):
            sql = sql.replace("%(as_of)s", "NOW()")
            sql = sql.replace("DATE_SUB(DATE(NOW()), INTERVAL 1 DAY)", "DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
            rows, _ = execute_mysql_fixture(source_db, sql)
            return rows
    source_db.create_function("NOW", 0, lambda: "2026-10-09 12:00:00")
    phones = {"4912345678": 1, "39061234567": 2, "34644444444": 3,
              "34622222222": 4, "34633333333": 5, "34666666666": 6, "34677777777": 7}
    output, raw, digest = audit.collect_reservations(Source(), datetime(2026, 10, 9, 12), 24, audit.LEGACY_REF, phones)
    assert len(digest) == 64
    assert raw["full"]["base_rows"] == 7
    assert {row["id_reserva"] for row in output["full"]["flights"]} == {"B42"}
    gift = next(row for row in output["full"]["base"] if row["id_reserva"] == "G42")
    assert gift["fecha_ida"] is None and gift["cliente_id"] == 3
    clients, _ = audit.collect_clients(Source(), datetime(2026, 10, 9, 12), 24, audit.LEGACY_REF)
    german = next(row for row in clients["full"] if row["telefono"] == "4912345678")
    assert german["nombre"] == "Marta Viajera"


def test_destination_audit_detects_phone_collisions_without_exporting_values():
    from tools.audit_supabase import summarize
    clients = [{'cliente_id': 1, 'telefono': '+34 611111111'},
               {'cliente_id': 2, 'telefono': '34611111111'}]
    reservations = [{'cliente_id': 1, 'id_reserva': 'A', 'regalo': 1},
                    {'cliente_id': 1, 'id_reserva': 'A', 'regalo': 1}]
    result = summarize(clients, reservations, [{'id': 1, 'reserva_id': 'X', 'flow_type': 'confirmados'}], lambda v: ''.join(c for c in v if c.isdigit()))
    assert result['clientes']['duplicate_normalized_phones'] == 1
    assert result['reservas']['duplicate_conflict_keys'] == 1
    assert result['cola']['references_absent_from_visible_reservations'] == 1
    assert '611111111' not in json.dumps(result)


def test_destination_pagination_refuses_incomplete_history():
    from tools.audit_supabase import ReadOnlyDestination
    destination = ReadOnlyDestination.__new__(ReadOnlyDestination)
    destination.max_rows = 20
    destination.get = Mock(side_effect=[([], 2), ([{'id': 1}], 2), ([], 2)])
    with pytest.raises(audit.ValidationError, match='incompleta'):
        destination.read_table('offline', 'id', 'id.asc')


def test_destination_pagination_refuses_changed_row_count():
    from tools.audit_supabase import ReadOnlyDestination
    destination = ReadOnlyDestination.__new__(ReadOnlyDestination)
    destination.max_rows = 20
    destination.get = Mock(side_effect=[([], 1), ([{'id': 1}], 2)])
    with pytest.raises(audit.ValidationError, match='conteo'):
        destination.read_table('offline', 'id', 'id.asc')


def test_real_history_reconciliation_detects_changed_customer_for_existing_code():
    from tools.migration_compare import reconcile_history
    preview = [{'cliente_id': 1, 'id_reserva': 'OLD'}, {'cliente_id': 2, 'id_reserva': 'NEW'}]
    clients = [{'cliente_id': 100, 'telefono': '111'}, {'cliente_id': 200, 'telefono': '222'}]
    history = [{'cliente_id': 200, 'id_reserva': 'OLD'}]
    result = reconcile_history(preview, {'111': 1, '222': 2}, clients, history, [{'reserva_id': 'ARCHIVED'}], lambda v: v)
    assert result['existing_code_linked_to_different_client'] == 1
    assert result['rows_without_existing_code'] == 1
    assert result['rows_without_existing_client_phone'] == 0
    assert result['queue_references_absent_from_paid_preview'] == 1
    assert 'OLD' not in json.dumps(result) and '111' not in json.dumps(result)
