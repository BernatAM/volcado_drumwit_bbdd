# volcado_reservas.py
import os
import re
import sys
import logging
from datetime import datetime, timedelta, date, time
from datetime import datetime as _dt
from datetime import timedelta as _td

from dotenv import load_dotenv
import pymysql  # pip install pymysql
from supabase import create_client, Client

load_dotenv(".env")

# ------------------------
# Config & logging
# ------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
)
log = logging.getLogger("etl.reservas")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Faltan SUPABASE_URL o SUPABASE_KEY en el entorno")
    sys.exit(2)

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DATABASE")
if not (MYSQL_USER and MYSQL_PASSWORD and MYSQL_DB):
    log.error("Faltan credenciales MySQL (MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB)")
    sys.exit(2)

ETL_MODE = os.getenv("ETL_MODE", "incremental").lower()
INCREMENTAL_HOURS = int(os.getenv("INCREMENTAL_HOURS", "24"))

# ------------------------
# Conexiones
# ------------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def mysql_conn():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

# ------------------------
# Utilidades
# ------------------------

# Prefijos por idioma
COUNTRY_PREFIX_BY_LANG = {
    "es": "34",
    "esp": "34",
    "it": "39",
    "fr": "33",
    "pt": "351",
    "en": "34"
}

def normalize_phone(phone_raw, lang: str | None = None) -> str | None:
    """
    Normaliza a dígitos (formato E.164 sin '+'):
    - elimina todo lo que no sea dígito
    - si empieza por '00' => lo quita
    - quita ceros domésticos iniciales
    - si el resultado tiene 11 dígitos -> se devuelve tal cual
    - si tiene menos de 11 y tenemos idioma -> se antepone prefijo del país
    - si >15 dígitos => descarta (None)
    """
    if phone_raw is None:
        return None

    s = str(phone_raw)
    # dejar solo dígitos
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None

    # quitar 00 inicial (formato internacional)
    if digits.startswith("00"):
        digits = digits[2:]

    # quitar ceros domésticos iniciales (0 de fijo/móvil local)
    digits = digits.lstrip("0")

    if not digits:
        return None

    # si ya tiene 11 dígitos (ej. 34 + 9 ES) lo damos por bueno
    if len(digits) == 11:
        return digits

    # si tiene menos de 11, añadimos prefijo según idioma
    if len(digits) < 11:
        prefix = None
        if lang:
            lang_key = str(lang).lower()
            prefix = COUNTRY_PREFIX_BY_LANG.get(lang_key)
        if prefix:
            digits = prefix + digits

    # si se ha ido de madre, descartamos
    if len(digits) > 15:
        return None

    return digits or None


def chunked(iterable, size=500):
    buf = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf

def combine_date_time(d: date, t: time) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second)

# ---- Saneo fechas MySQL (evitar '0000-00-00') ----
ZERO_DATE_STRS = {"0000-00-00", "0000-00-00 00:00:00", "0000-00-00T00:00:00"}

def _is_zero_like(v) -> bool:
    if v is None:
        return False
    if isinstance(v, (datetime, date)):
        return False
    s = str(v).strip()
    return s in ZERO_DATE_STRS

def clean_date_mysql(v) -> str | None:
    """
    Devuelve 'YYYY-MM-DD' o None si viene 0000-00-00/None.
    """
    if v is None or _is_zero_like(v):
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip().split(" ")[0]
    if s in ZERO_DATE_STRS or s == "" or s == "None":
        return None
    return s

def clean_datetime_mysql(v) -> str | None:
    """
    Devuelve 'YYYY-MM-DD HH:MM:SS' o None si inválido/cero.
    """
    if v is None or _is_zero_like(v):
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    s = str(v).strip()
    if s in ZERO_DATE_STRS or s == "" or s == "None":
        return None
    return s.replace("T", " ")

def clean_time_mysql(v) -> str | None:
    """
    Convierte TIME de MySQL a 'HH:MM:SS'.
    Acepta: datetime.time, datetime.timedelta, string 'HH:MM[:SS]'.
    """
    if v is None:
        return None
    if isinstance(v, time):
        return v.strftime("%H:%M:%S")
    if isinstance(v, _td):
        total = int(v.total_seconds()) % (24 * 3600)
        h = total // 3600
        m = (total % 3600) // 60
        s = total % 60
        return f"{h:02d}:{m:02d}:{s:02d}"
    s = str(v).strip()
    if not s:
        return None
    parts = s.split(":")
    if len(parts) >= 2:
        try:
            h = int(parts[0]); m = int(parts[1]); ssec = int(parts[2]) if len(parts) > 2 else 0
            return f"{h:02d}:{m:02d}:{ssec:02d}"
        except Exception:
            return None
    return None

# ---- Validación de fecha_vuelta >= fecha_ida (u omitirla) ----
def _safe_set_fecha_vuelta(row: dict, fecha_ida_str: str | None, fecha_vuelta_raw) -> None:
    """
    Añade 'fecha_vuelta' sólo si:
      - es válida (no '0000-00-00'), y
      - no es anterior a 'fecha_ida' (si fecha_ida existe)
    """
    fecha_vuelta_str = clean_date_mysql(fecha_vuelta_raw)
    if not fecha_vuelta_str:
        return
    if fecha_ida_str:
        try:
            ida = _dt.strptime(fecha_ida_str, "%Y-%m-%d").date()
            vuelta = _dt.strptime(fecha_vuelta_str, "%Y-%m-%d").date()
            if vuelta < ida:
                log.debug("Omitiendo fecha_vuelta %s < fecha_ida %s", fecha_vuelta_str, fecha_ida_str)
                return
        except Exception:
            return
    row["fecha_vuelta"] = fecha_vuelta_str


def clean_id_reserva_compradora(v):
    """Devuelve int > 0 o None (MySQL usa 0 como 'sin compradora')."""
    try:
        n = int(v) if v is not None else 0
        return n if n > 0 else None
    except Exception:
        return None

# ------------------------
# Carga clientes de Supabase (telefono->cliente_id)
# ------------------------
def load_cliente_map() -> dict:
    log.info("Descargando clientes (cliente_id, telefono) de Supabase...")
    telefono_to_cliente = {}
    page_size = 1000
    last_id = 0
    while True:
        resp = supabase.table("clientes") \
            .select("cliente_id,telefono") \
            .gte("cliente_id", last_id + 1) \
            .order("cliente_id", desc=False) \
            .limit(page_size) \
            .execute()
        rows = resp.data or []
        if not rows:
            break
        for r in rows:
            # Aquí NO pasamos idioma: los teléfonos ya están normalizados con prefijo
            tel = normalize_phone(r.get("telefono") or "")
            if tel:
                telefono_to_cliente[tel] = r["cliente_id"]
            last_id = r["cliente_id"]
        if len(rows) < page_size:
            break
    log.info("Clientes descargados: %d", len(telefono_to_cliente))
    return telefono_to_cliente

# ------------------------
# Queries MySQL (con telefono_eff y nombre_eff)
# ------------------------
SQL_RESERVA_BASE = """
SELECT
    r.id AS reserva_id,
    COALESCE(NULLIF(r.localizador,''), CAST(r.id AS CHAR)) AS id_reserva,
    r.fecha_reserva,
    r.fecha_salida,
    r.fecha_llegada,
    -- Teléfono efectivo: principal o comprador_regalo si el principal está vacío
    COALESCE(NULLIF(r.telefono, ''), NULLIF(r.telefono_comprador_regalo, '')) AS telefono_eff,
    -- Nombre efectivo (por si luego lo necesitas)
    COALESCE(NULLIF(r.nombre_regalo, ''), NULLIF(r.nombre_comprador_regalo, ''), NULLIF(r.nickname, '')) AS nombre_eff,
    r.idioma,
    r.regalo,
    r.id_reserva_compradora_regalo,
    r.num_viajeros as numero_viajeros
FROM reserva r
WHERE 
r.estado > 0 AND {where_clause}
"""

SQL_BOOKING_ENRICH = """
SELECT
    r.id AS reserva_id,
    COALESCE(NULLIF(r.localizador,''), CAST(r.id AS CHAR)) AS id_reserva,
    r.fecha_reserva,   -- blindaje NOT NULL en enriquecimiento
    r.fecha_salida,
    r.fecha_llegada,
    COALESCE(NULLIF(r.telefono, ''), NULLIF(r.telefono_comprador_regalo, '')) AS telefono_eff,
    COALESCE(NULLIF(r.nombre_regalo, ''), NULLIF(r.nombre_comprador_regalo, ''), NULLIF(r.nickname, '')) AS nombre_eff,
    r.idioma,
    r.regalo,
    r.id_reserva_compradora_regalo,
    bf.departure_time AS hora_ida,
    bf.return_time    AS hora_vuelta,
    d.nombre_es       AS destino_nombre
FROM reserva r
JOIN booking_flights bf ON bf.booking_id = r.id
LEFT JOIN destino d ON d.id = bf.destination_id
WHERE
r.estado > 0 AND {where_clause}
"""

def build_where(mode: str, hours: int = INCREMENTAL_HOURS) -> str:
    # Filtros fijos (para evitar fechas mal formadas)
    conds = [
        "(r.fecha_salida IS NULL OR r.fecha_salida <> '0000-00-00')",
        "(r.fecha_llegada IS NULL OR r.fecha_llegada <> '0000-00-00')",
        "(r.fecha_llegada IS NULL OR r.fecha_llegada >= '1970-01-02')",
    ]

    # Filtro incremental (solo si toca)
    if mode == "incremental":
        conds.insert(
            0,
            f"(r.ultima_modificacion >= NOW() - INTERVAL {hours} HOUR "
            f"OR r.fecha_reserva >= NOW() - INTERVAL {hours} HOUR)"
        )

    # En modo "full" no añadimos nada más: carga todo el histórico
    return " AND ".join(conds)

# ------------------------
# Transformación a registros Supabase
# ------------------------
def reservas_base_rows(mysql_rows, telefono_map):
    """
    - fecha_ida (NOT NULL): si inválida -> descartar fila
    - fecha_vuelta/fecha_reserva: opcionales; si inválidas -> omitir campo
    - regalo (0/1) y compradora (nullable)
    """
    out = []
    skipped_no_cliente = 0
    skipped_bad_fecha_ida = 0
    for r in mysql_rows:
        # Usa idioma de la reserva para normalizar el teléfono
        tel_norm = normalize_phone(r["telefono_eff"], r.get("idioma"))
        cliente_id = telefono_map.get(tel_norm)
        if not cliente_id:
            skipped_no_cliente += 1
            continue

        fecha_ida = clean_date_mysql(r.get("fecha_salida"))
        if not fecha_ida:
            skipped_bad_fecha_ida += 1
            continue

        row = {
            "cliente_id": cliente_id,
            "id_reserva": r["id_reserva"],
            "destino": "Pendiente",
            "fecha_ida": fecha_ida,
            "regalo": r.get("regalo"),
        }

        _safe_set_fecha_vuelta(row, fecha_ida, r.get("fecha_llegada"))

        fecha_reserva = clean_datetime_mysql(r.get("fecha_reserva"))
        if fecha_reserva:
            row["fecha_reserva"] = fecha_reserva

        compradora = clean_id_reserva_compradora(r.get("id_reserva_compradora_regalo"))
        if compradora is not None:
            row["id_reserva_compradora_regalo"] = compradora

        out.append(row)

    if skipped_no_cliente:
        log.warning("Reservas omitidas por no encontrar cliente por teléfono: %d", skipped_no_cliente)
    if skipped_bad_fecha_ida:
        log.warning("Reservas omitidas por fecha_ida inválida (0000-00-00): %d", skipped_bad_fecha_ida)
    return out

def reservas_enrichment_rows(mysql_rows, telefono_map, reservas_base_index):
    """
    Devuelve filas para ACTUALIZAR destino y horas de vuelo.
    Si por cualquier motivo la fila base no existiera y el upsert hiciera INSERT,
    aquí enviamos también las columnas NOT NULL (fecha_reserva, fecha_ida) para no romper.
    Además, incluimos regalo y compradora para mantener consistencia.
    """
    out = []
    missing_base = 0
    fallback_fr = 0

    for r in mysql_rows:
        reserva_id = r["reserva_id"]
        base_pair = reservas_base_index.get(reserva_id)
        if not base_pair:
            missing_base += 1
            continue

        cliente_id, id_reserva = base_pair
        row = {
            "cliente_id": cliente_id,
            "id_reserva": id_reserva,
            "destino": (r.get("destino_nombre") or "Pendiente"),
            "regalo": r.get("regalo"),
        }

        # Horas (normalizadas a HH:MM:SS)
        hora_ida = clean_time_mysql(r.get("hora_ida"))
        if hora_ida:
            row["hora_vuelo_ida"] = hora_ida
        hora_vuelta = clean_time_mysql(r.get("hora_vuelta"))
        if hora_vuelta:
            row["hora_vuelo_vuelta"] = hora_vuelta

        # Blindaje NOT NULL si el upsert entra por INSERT
        fecha_ida = clean_date_mysql(r.get("fecha_salida"))
        if fecha_ida:
            row["fecha_ida"] = fecha_ida
        else:
            log.debug("Saltando enriquecimiento por fecha_ida inválida en reserva_id=%s", reserva_id)
            continue

        fr = clean_datetime_mysql(r.get("fecha_reserva"))
        if fr:
            row["fecha_reserva"] = fr
        else:
            row["fecha_reserva"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            fallback_fr += 1

        _safe_set_fecha_vuelta(row, fecha_ida, r.get("fecha_llegada"))

        compradora = clean_id_reserva_compradora(r.get("id_reserva_compradora_regalo"))
        if compradora is not None:
            row["id_reserva_compradora_regalo"] = compradora

        out.append(row)

    if missing_base:
        log.warning("Vuelos con reserva base no localizada (saltados): %d", missing_base)
    if fallback_fr:
        log.info("reservas_enrichment_rows: fecha_reserva con fallback NOW() en %d filas", fallback_fr)

    return out

# ------------------------
# Upserts a Supabase (cliente_reservas)
# ------------------------
def upsert_reservas(rows):
    if not rows:
        return 0
    total = 0
    for batch in chunked(rows, 1000):
        _ = supabase.table("cliente_reservas") \
            .upsert(batch, on_conflict="cliente_id,id_reserva") \
            .execute()
        total += len(batch)
    return total

# ------------------------
# Pipelines
# ------------------------
def pipeline_incremental():
    where = build_where("incremental")
    with mysql_conn() as conn, conn.cursor() as cur:
        tel_map = load_cliente_map()

        # Base
        cur.execute(SQL_RESERVA_BASE.format(where_clause=where))
        base_rows_mysql = cur.fetchall() or []
        log.info("Reservas base MySQL recuperadas (incremental): %d", len(base_rows_mysql))

        reservas_base_idx = {}
        base_rows_sb = []
        for r in base_rows_mysql:
            # normalizamos teléfono usando idioma de la reserva
            tel_norm = normalize_phone(r["telefono_eff"], r.get("idioma"))
            cliente_id = tel_map.get(tel_norm)
            if not cliente_id:
                continue

            row = {
                "cliente_id": cliente_id,
                "id_reserva": r["id_reserva"],
                "destino": "Pendiente",
                "regalo": r.get("regalo"),
                "numero_viajeros": r.get("numero_viajeros"),
            }

            fecha_ida = clean_date_mysql(r.get("fecha_salida"))
            row["fecha_ida"] = fecha_ida

            _safe_set_fecha_vuelta(row, fecha_ida, r.get("fecha_llegada"))

            fecha_reserva = clean_datetime_mysql(r.get("fecha_reserva"))
            if fecha_reserva:
                row["fecha_reserva"] = fecha_reserva

            compradora = clean_id_reserva_compradora(r.get("id_reserva_compradora_regalo"))
            if compradora is not None:
                row["id_reserva_compradora_regalo"] = compradora

            base_rows_sb.append(row)
            reservas_base_idx[r["reserva_id"]] = (cliente_id, r["id_reserva"])

        inserted = upsert_reservas(base_rows_sb)
        log.info("Upsert reservas base (incremental): %d", inserted)
        log.info("Reservas base indexadas para enriquecimiento: %d", len(reservas_base_idx))

        # Enriquecimiento
        cur.execute(SQL_BOOKING_ENRICH.format(where_clause=where))
        enrich_rows_mysql = cur.fetchall() or []
        log.info("Registros booking_flights (incremental): %d", len(enrich_rows_mysql))

        enrich_rows_sb = reservas_enrichment_rows(enrich_rows_mysql, tel_map, reservas_base_idx)
        updated = upsert_reservas(enrich_rows_sb)
        log.info("Upsert reservas con vuelos (incremental): %d", updated)

def pipeline_nocturno():
    where = build_where("nocturno")
    with mysql_conn() as conn, conn.cursor() as cur:
        tel_map = load_cliente_map()

        # Base
        cur.execute(SQL_RESERVA_BASE.format(where_clause=where))
        base_rows_mysql = cur.fetchall() or []
        log.info("Reservas base MySQL recuperadas (nocturno): %d", len(base_rows_mysql))

        reservas_base_idx = {}
        base_rows_sb = []
        for r in base_rows_mysql:
            tel_norm = normalize_phone(r["telefono_eff"], r.get("idioma"))
            cliente_id = tel_map.get(tel_norm)
            if not cliente_id:
                continue

            row = {
                "cliente_id": cliente_id,
                "id_reserva": r["id_reserva"],
                "destino": "Pendiente",
                "regalo": r.get("regalo"),
                "numero_viajeros": r.get("numero_viajeros"),
            }

            fecha_ida = clean_date_mysql(r.get("fecha_salida"))
            row["fecha_ida"] = fecha_ida

            _safe_set_fecha_vuelta(row, fecha_ida, r.get("fecha_llegada"))

            fecha_reserva = clean_datetime_mysql(r.get("fecha_reserva"))
            if fecha_reserva:
                row["fecha_reserva"] = fecha_reserva

            compradora = clean_id_reserva_compradora(r.get("id_reserva_compradora_regalo"))
            if compradora is not None:
                row["id_reserva_compradora_regalo"] = compradora

            base_rows_sb.append(row)
            reservas_base_idx[r["reserva_id"]] = (cliente_id, r["id_reserva"])

        inserted = upsert_reservas(base_rows_sb)
        log.info("Upsert reservas base (nocturno): %d", inserted)
        log.info("Reservas base indexadas para enriquecimiento: %d", len(reservas_base_idx))

        # Enriquecimiento
        cur.execute(SQL_BOOKING_ENRICH.format(where_clause=where))
        enrich_rows_mysql = cur.fetchall() or []
        log.info("Registros booking_flights (nocturno): %d", len(enrich_rows_mysql))

        enrich_rows_sb = reservas_enrichment_rows(enrich_rows_mysql, tel_map, reservas_base_idx)
        updated = upsert_reservas(enrich_rows_sb)
        log.info("Upsert reservas con vuelos (nocturno): %d", updated)

# ------------------------
# Main
# ------------------------
if __name__ == "__main__":
    log.info("Iniciando ETL reservas | ETL_MODE=%s", ETL_MODE)
    ETL_MODE = 'incremental'
    try:
        if ETL_MODE == "incremental":
            pipeline_incremental()
        elif ETL_MODE == "nocturno":
            pipeline_nocturno()
        else:
            log.error("ETL_MODE desconocido: %s (usa 'incremental' o 'nocturno')", ETL_MODE)
            sys.exit(2)
        log.info("ETL reservas finalizado con éxito.")
    except Exception as e:
        log.exception("Fallo en ETL reservas: %s", e)
        sys.exit(1)
