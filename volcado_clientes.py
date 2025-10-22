# etl_clientes_desde_reservas.py
from __future__ import annotations

import os
import sys
import re
import time
import json
import logging
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone
from dotenv import load_dotenv

import pandas as pd
import requests
from supabase import create_client, Client

# ------------------------------------------------------------
# Config & Logging
# ------------------------------------------------------------
log = logging.getLogger("etl.clientes")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s :: %(message)s",
)

load_dotenv(".env")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    log.error("Faltan SUPABASE_URL o SUPABASE_KEY en el entorno.")
    sys.exit(1)

sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Tu función real de lectura
from bbdd_conection import run_query

# ============================================================
# Utils
# ============================================================
def normalize_phone(raw: Any) -> Optional[str]:
    """
    Normaliza a dígitos (formato E.164 sin '+'):
    - elimina todo lo que no sea dígito
    - si empieza por '00' => lo quita
    - quita ceros domésticos iniciales
    - si quedan 9 dígitos (móvil ES típico), antepone '34'
    - si >15 dígitos => descarta
    """
    if raw is None:
        return None
    s = str(raw)
    digits = re.sub(r"\D+", "", s)
    if not digits:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
    digits = digits.lstrip("0")
    if len(digits) == 9 and not digits.startswith("34"):
        digits = "34" + digits
    if len(digits) > 15:
        return None
    return digits or None

def _to_camel_case_name(x: Any) -> str:
    """CamelCase por palabra (soporta guiones)."""
    if x is None:
        return ""
    s = str(x).strip().lower()
    if not s:
        return ""
    parts = []
    for token in s.split():
        subparts = [sp.capitalize() for sp in token.split("-")]
        parts.append("-".join(subparts))
    return " ".join(parts)

def _normalize_lang(x: Any, default: str = "es") -> Optional[str]:
    """
    Normaliza idioma a minúsculas sencillas (ej. 'es', 'en', 'pt').
    Si viene vacío/nulo, aplica default.
    """
    if x is None:
        return default
    s = str(x).strip().lower()
    return s if s else default

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def df_dropna_phones(df: pd.DataFrame, phone_col: str) -> pd.DataFrame:
    df = df.copy()
    df[phone_col] = df[phone_col].apply(normalize_phone)
    df = df[df[phone_col].notna()]
    df = df[df[phone_col].astype(str).str.fullmatch(r"\d{3,15}")]
    df = df.drop_duplicates(subset=[phone_col], keep="first")
    return df

def _is_adult(v) -> bool:
    if pd.isna(v):
        return False
    s = str(v).strip().upper()
    if s in {"0", "ADT", "ADL", "ADU", "ADULT", "ADULT0"}:
        return True
    try:
        return int(s) == 0
    except:
        return False

def sanitize_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    - created_at -> ISO 8601 UTC
    - nombre/nickname en CamelCase y nickname nunca vacío
    - NaN -> None
    """
    out = df.copy()
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], utc=True, errors="coerce")
        out["created_at"] = out["created_at"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if "nombre" in out.columns:
        out["nombre"] = out["nombre"].apply(_to_camel_case_name)

    if "nickname" in out.columns:
        out["nickname"] = out["nickname"].astype(str)
        mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
        out.loc[mask_nn, "nickname"] = out.loc[mask_nn, "nombre"]
        out["nickname"] = out["nickname"].apply(_to_camel_case_name)

    if "idioma" in out.columns:
        out["idioma"] = out["idioma"].apply(lambda v: _normalize_lang(v, "es"))

    out = out.where(pd.notna(out), None)
    return out

# ============================================================
# Extracciones (solo reservas y pasajeros)
# ============================================================
def extract_reservas(run_query, full: bool = True) -> pd.DataFrame:
    """
    Necesario: reserva_id, fecha_reserva, nickname, telefono, idioma, datos de regalo.
    """
    base = """
    SELECT 
        r.id AS reserva_id,
        r.fecha_reserva,
        r.nickname,
        r.telefono,
        -- Campos para regalo:
        r.telefono_comprador_regalo,
        r.nombre_comprador_regalo,
        r.apellido1_comprador_regalo,
        NULL AS nombre_comprador_reserva,
        r.idioma AS idioma
    FROM reserva r
    """
    where = " WHERE r.fecha_reserva > DATE_SUB(CURDATE(), INTERVAL 1 DAY)" if not full else ""
    return run_query(base + where)

def extract_pasajero_reserva(run_query) -> pd.DataFrame:
    return run_query("""
    SELECT 
        id_reserva AS reserva_id,
        id_pasajero AS pasajero_id
    FROM pasajeros__reserva
    """)

def extract_pasajeros(run_query) -> pd.DataFrame:
    return run_query("""
    SELECT
        id AS pasajero_id,
        nombre,
        apellido1 AS apellido,
        tipo_pasajero AS tipo
    FROM pasajero
    """)

# ============================================================
# Cálculo nombre por RESERVA (Norma 1→2→3 + caso REGALO)
# ============================================================
def build_nombre_reserva_por_reserva(
    df_reservas: pd.DataFrame,
    df_pasajero_reserva: pd.DataFrame,
    df_pasajeros: pd.DataFrame
) -> pd.DataFrame:
    r = df_reservas.copy()

    # Asegura columnas
    for c in (
        "nickname", "nombre_comprador_reserva",
        "telefono", "telefono_comprador_regalo",
        "nombre_comprador_regalo", "apellido1_comprador_regalo",
        "fecha_reserva", "idioma"
    ):
        if c not in r.columns:
            r[c] = None

    # Primer pasajero adulto por reserva
    pr = df_pasajero_reserva.copy()
    p  = df_pasajeros.copy()
    if "tipo" in p.columns:
        p["__is_adult__"] = p["tipo"].apply(_is_adult)
    else:
        p["__is_adult__"] = True

    p_adult  = p[p["__is_adult__"] == True].copy()
    pr_adult = pr.merge(p_adult, on="pasajero_id", how="inner").sort_values(["reserva_id", "pasajero_id"])

    for c in ("nombre", "apellido"):
        if c not in pr_adult.columns:
            pr_adult[c] = ""
    pr_adult["nombre_apellido"] = (
        pr_adult["nombre"].fillna("").astype(str).str.strip() + " " +
        pr_adult["apellido"].fillna("").astype(str).str.strip()
    ).str.strip()

    first_adult = pr_adult.drop_duplicates(subset=["reserva_id"], keep="first")[["reserva_id", "nombre_apellido"]]
    r = r.merge(first_adult, on="reserva_id", how="left")

    # Teléfono efectivo
    def pick_phone(row):
        t = normalize_phone(row.get("telefono"))
        if t:
            return t
        return normalize_phone(row.get("telefono_comprador_regalo"))

    # Nombre efectivo
    def pick_name(row):
        telefono_normal = normalize_phone(row.get("telefono"))
        is_gift = telefono_normal is None and normalize_phone(row.get("telefono_comprador_regalo")) is not None

        if is_gift:
            nom = (str(row.get("nombre_comprador_regalo") or "").strip() + " " +
                   str(row.get("apellido1_comprador_regalo") or "").strip()).strip()
            return _to_camel_case_name(nom) if nom else None

        nick = str(row.get("nickname") or "").strip()
        if nick:
            return _to_camel_case_name(nick)
        pa = str(row.get("nombre_apellido") or "").strip()
        if pa:
            return _to_camel_case_name(pa)
        comp = str(row.get("nombre_comprador_reserva") or "").strip()
        return _to_camel_case_name(comp) if comp else None

    out = r[[
        "reserva_id","fecha_reserva","telefono","telefono_comprador_regalo",
        "nickname","nombre_apellido","nombre_comprador_reserva",
        "nombre_comprador_regalo","apellido1_comprador_regalo","idioma"
    ]].copy()

    out["telefono_efectivo"]    = out.apply(pick_phone, axis=1)
    out["nombre_reserva_norma"] = out.apply(pick_name,  axis=1)
    out["idioma"]               = out["idioma"].apply(lambda v: _normalize_lang(v, "es"))

    return out[["reserva_id","fecha_reserva","telefono_efectivo","nombre_reserva_norma","idioma"]]

# ============================================================
# Construcción de "clientes" desde RESERVAS
# ============================================================
def build_clientes_from_reservas(df_nombre_reserva: pd.DataFrame) -> pd.DataFrame:
    """
    - Normaliza teléfono efectivo
    - Nombre/nickname = nombre_reserva_norma (CamelCase); si vacío -> "Cliente <telefono>"
    - Agrega por teléfono tomando la última reserva (fecha_reserva máx)
    - Hereda idioma de la reserva más reciente de ese teléfono (default 'es')
    """
    df = df_nombre_reserva.copy()

    # Teléfono efectivo normalizado
    df["telefono"] = df["telefono_efectivo"].apply(normalize_phone)
    df = df[df["telefono"].notna()]

    # Fecha parseada
    df["fecha_reserva_parsed"] = pd.to_datetime(df["fecha_reserva"], errors="coerce", utc=True)

    # Orden por teléfono y fecha desc (luego reserva_id desc como desempate)
    df = df.sort_values(by=["telefono", "fecha_reserva_parsed", "reserva_id"], ascending=[True, False, False])

    # Última por teléfono
    df = df.drop_duplicates(subset=["telefono"], keep="first")

    # Nombre/nickname (CamelCase) y fallback
    df["nombre_calc"] = df["nombre_reserva_norma"].fillna("").astype(str)
    df["nombre_calc"] = df["nombre_calc"].where(df["nombre_calc"].str.strip() != "", "Cliente " + df["telefono"].astype(str))
    df["nombre_calc"] = df["nombre_calc"].apply(_to_camel_case_name)

    # Idioma heredado de la última reserva del teléfono
    df["idioma_calc"] = df["idioma"].apply(lambda v: _normalize_lang(v, "es"))

    out = pd.DataFrame({
        "nombre": df["nombre_calc"],
        "telefono": df["telefono"],
        "nickname": df["nombre_calc"],
        "created_at": now_utc(),
        "destino": None,
        "status_drumwit": "cliente_sin_reserva",   # INSERT: default explícito
        "status_reserva": None,
        "is_user_required": False,                 # INSERT: default explícito
        "is_ai_mode": True,                        # INSERT: default explícito
        "idioma": df["idioma_calc"],
        "status_boarding_passes": "no_booking",
    })

    out = df_dropna_phones(out, "telefono")
    # Doble cinturón
    out["nombre"]   = out["nombre"].apply(_to_camel_case_name)
    out["nickname"] = out["nickname"].apply(_to_camel_case_name)
    out["idioma"]   = out["idioma"].apply(lambda v: _normalize_lang(v, "es"))

    return out[
        ["nombre","telefono","created_at","destino","status_drumwit","status_reserva",
         "is_user_required","is_ai_mode","idioma","status_boarding_passes","nickname"]
    ]

# ============================================================
# Lectura teléfonos existentes destino
# ============================================================
def fetch_existing_phones() -> set[str]:
    phones: set[str] = set()
    page_size = 1000
    last_id = None
    while True:
        q = sb.table("clientes").select("cliente_id,telefono").order("cliente_id", desc=False).limit(page_size)
        if last_id is not None:
            q = q.gt("cliente_id", last_id)
        resp = q.execute()
        rows = resp.data or []
        if not rows:
            break
        for r in rows:
            tel = normalize_phone(r.get("telefono"))
            if tel:
                phones.add(tel)
            last_id = r["cliente_id"]
        if len(rows) < page_size:
            break
    log.info("Teléfonos existentes descargados: %d", len(phones))
    return phones

# ============================================================
# Upsert por teléfono (insert nuevos; update SOLO nombre/nickname)
# ============================================================
def _http_upsert_bulk(table_name: str, rows: List[Dict[str, Any]], on_conflict: str) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal"
    }
    params = {"on_conflict": on_conflict}
    resp = requests.post(url, params=params, headers=headers, json=rows, timeout=120)
    if not (200 <= resp.status_code < 300):
        raise RuntimeError(f"Upsert HTTP falló: {resp.status_code} {resp.text}")

def upsert_by_phone_only_name(df_clients: pd.DataFrame, batch_size: int = 2000) -> Tuple[int, int, list[str], list[str]]:
    """
    BULK:
      - Inserta NUEVOS teléfonos en lotes (payload completo).
      - Para EXISTENTES, hace un UPSERT masivo con SOLO ['telefono','nombre','nickname'].
    Requiere UNIQUE en clientes.telefono.


    En inserts enviamos explícitamente: status_drumwit='cliente_sin_reserva',
    is_user_required=False, is_ai_mode=True, idioma=heredado (default 'es').
    En updates NO tocamos esos campos.
    """
    df = sanitize_df_for_json(df_clients.copy())

    existing = fetch_existing_phones()
    to_insert = df[~df["telefono"].isin(existing)].copy()
    to_update = df[df["telefono"].isin(existing)].copy()

    inserted_phones = to_insert["telefono"].dropna().astype(str).tolist()
    updated_phones  = to_update["telefono"].dropna().astype(str).tolist()

    insert_cols = [
        "nombre","telefono","created_at","destino","status_drumwit","status_reserva",
        "is_user_required","is_ai_mode","idioma","status_boarding_passes","nickname"
    ]
    update_cols = ["telefono", "nombre", "nickname", "idioma"]  # <- SOLO estos para no tocar flags/idioma/status

    total_inserted = 0
    total_updated  = 0

    # ---- BULK INSERT (nuevos) ----
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert.iloc[i:i+batch_size]
        payload = batch[insert_cols].to_dict(orient="records")
        if not payload:
            continue
        try:
            # insert normal; si hay carrera, fallback a upsert HTTP
            sb.table("clientes").insert(payload).execute()
        except Exception:
            _http_upsert_bulk("clientes", payload, on_conflict="telefono")
        total_inserted += len(payload)

    # ---- BULK UPSERT (existentes) SOLO nombre/nickname ----
    to_update = to_update[(to_update["telefono"].notna()) & (to_update["nombre"].astype(str).str.strip() != "")]
    for i in range(0, len(to_update), batch_size):
        batch = to_update.iloc[i:i+batch_size]
        if batch.empty:
            continue
        payload = batch[update_cols].to_dict(orient="records")
        _http_upsert_bulk("clientes", payload, on_conflict="telefono")
        total_updated += len(payload)

    log.info("BULK upsert -> Insertados: %d | Actualizados: %d", total_inserted, total_updated)
    return total_inserted, total_updated, inserted_phones, updated_phones


# ============================================================
# Pipeline principal
# ============================================================
def pipeline_incremental_horario(run_query_func) -> None:
    # 1) Reservas último día + todos los pax
    df_r  = extract_reservas(run_query_func, full=False)
    df_pr = extract_pasajero_reserva(run_query_func)
    df_p  = extract_pasajeros(run_query_func)

    # 2) Nombre por reserva (norma + regalo)
    df_nombre = build_nombre_reserva_por_reserva(df_r, df_pr, df_p)

    # 3) Clientes desde reservas (agregado por teléfono)
    df_clients = build_clientes_from_reservas(df_nombre)

    # 4) Upsert (insert nuevos / update solo nombre & nickname)
    inserted, updated, inserted_phones, updated_phones = upsert_by_phone_only_name(df_clients)

def pipeline_upsert_nocturno(run_query_func) -> None:
    # 1) Todas las reservas + todos los pax
    df_r  = extract_reservas(run_query_func, full=True)
    df_pr = extract_pasajero_reserva(run_query_func)
    df_p  = extract_pasajeros(run_query_func)

    # 2) Nombre por reserva (norma + regalo)
    df_nombre = build_nombre_reserva_por_reserva(df_r, df_pr, df_p)

    # 3) Clientes desde reservas (última por teléfono)
    df_clients = build_clientes_from_reservas(df_nombre)

    # 4) Upsert (insert nuevos / update solo nombre & nickname)
    inserted, updated, inserted_phones, updated_phones = upsert_by_phone_only_name(df_clients)

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    mode = os.getenv("ETL_MODE", "nocturno").lower()  # "incremental" o "nocturno"
    ETL_MODE = "incremental"
    if mode == "incremental":
        pipeline_incremental_horario(run_query)
    elif mode == "nocturno":
        pipeline_upsert_nocturno(run_query)
    else:
        log.error("ETL_MODE desconocido: %s (usa 'incremental' o 'nocturno')")
        sys.exit(2)
