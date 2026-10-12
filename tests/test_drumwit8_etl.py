"""Regresiones sin MySQL/Supabase: consultas reales sobre fixtures SQLite.

Sólo se adaptan las expresiones de fechas de MySQL; los JOIN, filtros de estado,
campos de contacto, UNION y recuentos son los que ejecuta el código de producción.
No sustituye una validación del esquema y dialecto en MySQL del cliente.
"""
import re
import sqlite3
from datetime import datetime
from unittest.mock import Mock

import pandas as pd
import pytest


@pytest.fixture
def source_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.create_function("CONCAT", -1, lambda *parts: None if None in parts else "".join(map(str, parts)))
    db.executescript("""
        CREATE TABLE booking (
            id INTEGER, code TEXT, created_at TEXT, updated_at TEXT,
            departure_date TEXT, trip_length_in_nights INTEGER,
            language TEXT, status TEXT, gift_id INTEGER
        );
        CREATE TABLE gift (
            id INTEGER, code TEXT, created_at TEXT, updated_at TEXT,
            language TEXT, status TEXT, adult_travelers_count INTEGER,
            child_travelers_count INTEGER, infant_travelers_count INTEGER
        );
        CREATE TABLE booking_contact_details (
            booking_id INTEGER, nickname TEXT, phone_country_code TEXT, phone_number TEXT,
            name TEXT, traveler_id INTEGER
        );
        CREATE TABLE gift_contact_details (
            gift_id INTEGER, nickname TEXT, phone_country_code TEXT, phone_number TEXT,
            name TEXT, last_name_1 TEXT, last_name_2 TEXT
        );
        CREATE TABLE traveler (
            id INTEGER, booking_id INTEGER, name TEXT, last_name_1 TEXT,
            last_name_2 TEXT, type TEXT
        );
        CREATE TABLE booking_flights (
            booking_id INTEGER, departure_time TEXT, return_time TEXT, destination_id INTEGER
        );
        CREATE TABLE destination (id INTEGER, name_es TEXT);

        INSERT INTO booking VALUES
            (42, 'B42', '2026-10-09 10:00:00', '2026-10-09 10:00:00', '2026-10-20', 3, 'fr', 'PAID', 77),
            (43, 'B43', '2026-10-09 09:00:00', '2026-10-09 09:00:00', NULL, 2, 'it', 'PAID', NULL),
            (44, 'B44', '2026-10-09 08:00:00', '2026-10-09 08:00:00', '2026-10-21', 1, 'es', 'PENDING', NULL),
            (45, 'B45', '2026-09-01 08:00:00', '2026-10-09 08:00:00', '2026-10-21', 1, 'es', 'PAID', NULL),
            (46, 'B46', '2026-09-01 08:00:00', '2026-09-01 08:00:00', '2026-10-21', 1, 'es', 'PAID', NULL);
        INSERT INTO gift VALUES
            (42, 'G42', '2026-10-09 11:00:00', '2026-10-09 11:00:00', 'es', 'PAID', 2, 1, 1),
            (44, 'G44', '2026-10-09 08:00:00', '2026-10-09 08:00:00', 'es', 'PENDING', 2, 0, 0),
            (45, 'G45', '2026-09-01 08:00:00', '2026-10-09 08:00:00', 'es', 'PAID', 1, NULL, NULL),
            (46, 'G46', '2026-09-01 08:00:00', '2026-09-01 08:00:00', 'es', 'PAID', 1, 0, 0);
        INSERT INTO booking_contact_details VALUES
            (42, NULL, '+49', '12345678', 'CONTACTO', 102),
            (43, NULL, '+39', '061234567', 'MARCO', NULL),
            (44, 'PENDIENTE', '+34', '611111111', 'PENDING', NULL),
            (45, 'MODIFICADO', '+34', '622222222', 'UPDATED', NULL),
            (46, 'ANTIGUO', '+34', '633333333', 'OLD', NULL);
        INSERT INTO gift_contact_details VALUES
            (42, NULL, '+34', '644444444', 'ANA', 'REGALO', NULL),
            (44, 'PENDIENTE', '+34', '655555555', 'PENDING', 'GIFT', NULL),
            (45, 'MODIFICADO', '+34', '666666666', 'UPDATED', 'GIFT', NULL),
            (46, 'ANTIGUO', '+34', '677777777', 'OLD', 'GIFT', NULL);
        INSERT INTO traveler VALUES
            (100, 42, 'NIÑO', 'NO ADULTO', NULL, 'CHILD'),
            (101, 42, 'BEBE', 'NO ADULTO', NULL, 'INFANT'),
            (102, 42, 'MARTA', 'VIAJERA', NULL, 'ADULT'),
            (103, 42, 'SEGUNDO', 'ADULTO', NULL, 'ADULT');
        INSERT INTO booking_flights VALUES (42, '08:15:00', '19:45:00', 7);
        INSERT INTO destination VALUES (7, 'Roma');
    """)
    yield db
    db.close()


def execute_mysql_fixture(db, query):
    query = re.sub(
        r"DATE_ADD\(b\.departure_date, INTERVAL b\.trip_length_in_nights DAY\)",
        "date(b.departure_date, '+' || b.trip_length_in_nights || ' days')", query,
    )
    query = query.replace("DATE_SUB(CURDATE(), INTERVAL 1 DAY)", "date('2026-10-09', '-1 day')")
    query = re.sub(r"NOW\(\) - INTERVAL (\d+) HOUR", r"datetime('2026-10-09 12:00:00', '-\1 hours')", query)
    cursor = db.execute(query)
    return [dict(row) for row in cursor], [col[0] for col in cursor.description]


def dataframe_query(db):
    def run(query):
        rows, columns = execute_mysql_fixture(db, query)
        return pd.DataFrame(rows, columns=columns)
    return run


def build_clients(mod, db, full=False):
    query = dataframe_query(db)
    named = mod.build_nombre_reserva_por_reserva(
        mod.extract_reservas(query, full=full),
        mod.extract_pasajero_reserva(query),
        mod.extract_pasajeros(query),
    )
    return mod.build_clientes_from_reservas(named)


def test_client_names_isolate_gift_from_booking_with_same_id(etl_modules, source_db):
    clients, _ = etl_modules
    payload = build_clients(clients, source_db).set_index("telefono")
    assert payload.loc["4912345678", "nombre"] == "Marta Viajera"
    assert payload.loc["34644444444", "nombre"] == "Ana Regalo"
    assert payload.loc["34644444444", "nickname"] == "Ana"
    assert payload.loc["4912345678", "idioma"] == "fr"
    assert payload.loc["39061234567", "nombre"] == "Marco"
    assert payload.loc["39061234567", "nickname"] == "Marco"


@pytest.mark.parametrize("table, id_col", [("booking_contact_details", "booking_id"), ("gift_contact_details", "gift_id")])
@pytest.mark.parametrize("number", [None, "", "   "])
def test_missing_phone_never_creates_country_code_only_client(etl_modules, source_db, table, id_col, number):
    clients, _ = etl_modules
    source_db.execute(f"UPDATE {table} SET phone_number=? WHERE {id_col}=42", (number,))
    payload = build_clients(clients, source_db)
    assert "49" not in set(payload.telefono)
    assert "34" not in set(payload.telefono)
    excluded = "4912345678" if id_col == "booking_id" else "34644444444"
    assert excluded not in set(payload.telefono)


def test_latest_contact_per_phone_preserves_status_contract(etl_modules, source_db):
    clients, _ = etl_modules
    source_db.execute("UPDATE gift_contact_details SET phone_country_code='+49', phone_number='12345678' WHERE gift_id=42")
    payload = build_clients(clients, source_db).set_index("telefono")
    row = payload.loc["4912345678"]
    assert row.nombre == "Ana Regalo"
    assert row.nickname == "Ana"
    assert row.status_drumwit == "cliente_sin_reserva"
    assert row.status_boarding_passes == "no_booking"
    assert bool(row.is_ai_mode) is True


def test_null_names_and_nickname_do_not_become_nan(etl_modules, source_db):
    clients, _ = etl_modules
    source_db.execute("UPDATE gift_contact_details SET name=NULL, last_name_1=NULL WHERE gift_id=42")
    payload = build_clients(clients, source_db).set_index("telefono")
    assert payload.loc["34644444444", "nombre"] == ""
    assert payload.loc["34644444444", "nickname"] == ""


@pytest.mark.parametrize("full", [False, True])
def test_client_extraction_window_applies_to_both_tables(etl_modules, source_db, full):
    clients, _ = etl_modules
    rows = clients.extract_reservas(dataframe_query(source_db), full=full)
    codes = set(rows.codigo)
    assert {"B42", "G42", "B44", "G44"} <= codes  # clientes incluye contactos aún no pagados
    assert ("B46" in codes) is full
    assert ("G46" in codes) is full
    if not full:
        assert "B45" not in codes and "G45" not in codes  # este ETL usa created_at, como el PDF


def test_no_adult_or_empty_travelers_uses_booking_contact(etl_modules, source_db):
    clients, _ = etl_modules
    source_db.execute("DELETE FROM traveler WHERE type='ADULT'")
    payload = build_clients(clients, source_db).set_index("telefono")
    assert payload.loc["4912345678", "nombre"] == "Contacto"
    source_db.execute("DELETE FROM traveler")
    payload = build_clients(clients, source_db).set_index("telefono")
    assert payload.loc["4912345678", "nombre"] == "Contacto"


@pytest.mark.parametrize("mode", ["incremental", "nocturno"])
def test_client_pipeline_empty_source_is_valid(etl_modules, source_db, monkeypatch, mode):
    clients, _ = etl_modules
    source_db.execute("DELETE FROM booking")
    source_db.execute("DELETE FROM gift")
    captured = []
    def write(frame):
        captured.append(frame)
        return 0, 0, [], []
    monkeypatch.setattr(clients, "upsert_by_phone_only_name", write)
    pipeline = clients.pipeline_incremental_horario if mode == "incremental" else clients.pipeline_upsert_nocturno
    pipeline(dataframe_query(source_db))
    assert len(captured) == 1 and captured[0].empty
    assert {"nombre", "telefono", "idioma", "nickname", "is_ai_mode"} <= set(captured[0].columns)


@pytest.mark.parametrize("mode", ["incremental", "nocturno"])
@pytest.mark.parametrize("shared_phone", [False, True])
def test_reservation_pipeline_same_numeric_id_keeps_flights_on_booking(etl_modules, source_db, monkeypatch, mode, shared_phone):
    _, reservations = etl_modules
    if shared_phone:
        source_db.execute("UPDATE gift_contact_details SET phone_country_code='+49', phone_number='12345678' WHERE gift_id=42")
    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def execute(self, query): self.rows, _ = execute_mysql_fixture(source_db, query)
        def fetchall(self): return self.rows
    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): return False
        def cursor(self): return Cursor()
    batches = []
    monkeypatch.setattr(reservations, "mysql_conn", Connection)
    monkeypatch.setattr(reservations, "load_cliente_map", lambda: {
        "4912345678": 1, "39061234567": 2, "34644444444": 3,
        "34622222222": 4, "34633333333": 5, "34666666666": 6, "34677777777": 7,
        "34611111111": 8, "34655555555": 9,
    })
    def upsert(rows):
        batches.append(rows)
        return len(rows)
    monkeypatch.setattr(reservations, "upsert_reservas", upsert)
    (reservations.pipeline_incremental if mode == "incremental" else reservations.pipeline_nocturno)()
    assert len(batches) == 2
    base = {row["id_reserva"]: row for row in batches[0]}
    assert "B44" not in base and "G44" not in base  # PENDING nunca genera reserva
    assert ("B46" in base) is (mode == "nocturno")
    assert ("G46" in base) is (mode == "nocturno")
    assert {"B45", "G45"} <= set(base)  # updated_at también selecciona cambios
    assert base["B42"]["cliente_id"] == 1
    assert base["B42"]["numero_viajeros"] == 4
    assert base["B42"]["fecha_ida"] == "2026-10-20"
    assert base["B42"]["fecha_vuelta"] == "2026-10-23"
    assert base["B42"]["id_reserva_compradora_regalo"] == 77
    assert base["B43"]["fecha_ida"] is None
    assert base["B43"]["id_reserva_compradora_regalo"] is None
    assert base["G42"]["cliente_id"] == (1 if shared_phone else 3)
    assert base["G42"]["regalo"] == 1
    assert base["G42"]["numero_viajeros"] == 4
    assert base["G42"]["fecha_ida"] is None and base["G42"]["fecha_vuelta"] is None
    assert base["G42"]["id_reserva_compradora_regalo"] is None
    assert base["G45"]["numero_viajeros"] == 1
    assert batches[1] == [{
        "cliente_id": 1, "id_reserva": "B42", "destino": "Roma", "regalo": 0,
        "id_reserva_compradora_regalo": 77, "hora_vuelo_ida": "08:15:00", "hora_vuelo_vuelta": "19:45:00",
        "fecha_ida": "2026-10-20", "fecha_reserva": "2026-10-09 10:00:00", "fecha_vuelta": "2026-10-23",
    }]


def test_client_updates_keep_existing_operational_flags(etl_modules, monkeypatch):
    clients, _ = etl_modules
    frame = pd.DataFrame([{
        "telefono": "4912345678", "nombre": "Marta Viajera", "nickname": "Marta", "idioma": "fr",
        "created_at": datetime(2026, 10, 9), "destino": None, "status_drumwit": "cliente_sin_reserva",
        "status_reserva": None, "is_user_required": False, "is_ai_mode": True, "status_boarding_passes": "no_booking",
    }])
    monkeypatch.setattr(clients, "fetch_existing_phones", lambda: {"4912345678"})
    write = Mock()
    monkeypatch.setattr(clients, "_http_upsert_bulk", write)
    result = clients.upsert_by_phone_only_name(frame)
    assert result[:2] == (0, 1)
    write.assert_called_once_with("clientes", [{
        "telefono": "4912345678", "nombre": "Marta Viajera", "nickname": "Marta", "idioma": "fr",
    }], on_conflict="telefono")


def test_reservation_upsert_preserves_existing_conflict_key(etl_modules, monkeypatch):
    _, reservations = etl_modules
    remote = Mock()
    monkeypatch.setattr(reservations, "supabase", remote)
    rows = [{"cliente_id": 1, "id_reserva": "B42"}, {"cliente_id": 1, "id_reserva": "G42"}]
    assert reservations.upsert_reservas(rows) == 2
    remote.table.assert_called_once_with("cliente_reservas")
    remote.table.return_value.upsert.assert_called_once_with(rows, on_conflict="cliente_id,id_reserva")


def test_unmatched_phone_does_not_make_or_enrich_reservations(etl_modules, source_db):
    _, reservations = etl_modules
    rows, _ = execute_mysql_fixture(source_db, reservations.SQL_RESERVA_BASE.format(booking_where="1=1", gift_where="1=1"))
    assert reservations.reservas_base_rows(rows, {}) == []
    flights, _ = execute_mysql_fixture(source_db, reservations.SQL_BOOKING_ENRICH.format(where_clause="1=1"))
    assert reservations.reservas_enrichment_rows(flights, {}, {}) == []
