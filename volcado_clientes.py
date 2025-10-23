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
    - nombre/nickname formateados
    - nombre hard-limit 100 chars (por restricción varchar(100))
    - NaN -> None
    """
    out = df.copy()
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], utc=True, errors="coerce")
        out["created_at"] = out["created_at"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    if "nombre" in out.columns:
        # Compacta espacios y recorta a 100 por si acaso
        out["nombre"] = out["nombre"].apply(lambda s: re.sub(r"\s+", " ", str(s or "").strip()))
        out["nombre"] = out["nombre"].astype(str).str.slice(0, 100)

    if "nickname" in out.columns:
        out["nickname"] = out["nickname"].astype(str)
        mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
        # Fallback al nombre (que ya está recortado a 100)
        out.loc[mask_nn, "nickname"] = out["nombre"]
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
    Necesario: reserva_id, fecha_reserva, nickname, telefono, idioma, datos de regalo (incluye apellido2_comprador_regalo).
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
        COALESCE(r.apellido2_comprador_regalo, NULL) AS apellido2_comprador_regalo,
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
        apellido1 AS apellido1,
        COALESCE(apellido2, '') AS apellido2,
        tipo_pasajero AS tipo
    FROM pasajero
    """)


# ============================================================
# Cálculo nombre por RESERVA (Norma 1→2→3 + caso REGALO)
#  - nickname_puro: para guardar en Supabase (CamelCase, sin apellidos)
#  - nickname_display: para mostrar entre paréntesis en 'nombre' (conserva case si viene de reserva.nickname)
# ============================================================
def build_nombre_reserva_por_reserva(
    df_reservas: pd.DataFrame,
    df_pasajero_reserva: pd.DataFrame,
    df_pasajeros: pd.DataFrame
) -> pd.DataFrame:
    r = df_reservas.copy()

    need_cols = [
        "nickname", "telefono", "telefono_comprador_regalo",
        "nombre_comprador_regalo", "apellido1_comprador_regalo", "apellido2_comprador_regalo",
        "fecha_reserva", "idioma"
    ]
    for c in need_cols:
        if c not in r.columns:
            r[c] = None

    # Primer pasajero adulto
    pr = df_pasajero_reserva.copy()
    p  = df_pasajeros.copy()
    if "tipo" in p.columns:
        p["__is_adult__"] = p["tipo"].apply(_is_adult)
    else:
        p["__is_adult__"] = True

    p_adult  = p[p["__is_adult__"] == True].copy()
    pr_adult = pr.merge(p_adult, on="pasajero_id", how="inner").sort_values(["reserva_id", "pasajero_id"])

    for c in ("nombre", "apellido1", "apellido2"):
        if c not in pr_adult.columns:
            pr_adult[c] = ""
    first_adult = pr_adult.drop_duplicates(subset=["reserva_id"], keep="first")[
        ["reserva_id", "nombre", "apellido1", "apellido2"]
    ].rename(columns={
        "nombre": "pax_nombre",
        "apellido1": "pax_apellido1",
        "apellido2": "pax_apellido2"
    })

    r = r.merge(first_adult, on="reserva_id", how="left")

    def pick_phone(row):
        t = normalize_phone(row.get("telefono"))
        return t if t else normalize_phone(row.get("telefono_comprador_regalo"))

    # Devuelve: nombre_base, ap1, ap2, nickname_puro (CamelCase), nickname_display (case original si venía de reserva)
    def pick_parts(row):
        telefono_normal = normalize_phone(row.get("telefono"))
        is_gift = telefono_normal is None and normalize_phone(row.get("telefono_comprador_regalo")) is not None

        if is_gift:
            base_nombre  = _to_camel_case_name(row.get("nombre_comprador_regalo"))
            base_ap1     = _to_camel_case_name(row.get("apellido1_comprador_regalo"))
            base_ap2     = _to_camel_case_name(row.get("apellido2_comprador_regalo"))

            nick_reserva_raw = (row.get("nickname") or "").strip()
            if nick_reserva_raw:
                nickname_display = nick_reserva_raw  # conserva case original
                nickname_puro    = _to_camel_case_name(nick_reserva_raw)  # guardamos en CamelCase
            else:
                nickname_display = base_nombre
                nickname_puro    = base_nombre

            return base_nombre, base_ap1, base_ap2, nickname_puro, nickname_display

        # No regalo: base = primer pax adulto
        base_nombre = _to_camel_case_name(row.get("pax_nombre"))
        base_ap1    = _to_camel_case_name(row.get("pax_apellido1"))
        base_ap2    = _to_camel_case_name(row.get("pax_apellido2"))

        nick_reserva_raw = (row.get("nickname") or "").strip()
        if nick_reserva_raw:
            nickname_display = nick_reserva_raw   # conserva case original
            nickname_puro    = _to_camel_case_name(nick_reserva_raw)
        else:
            nickname_display = base_nombre
            nickname_puro    = base_nombre

        return base_nombre, base_ap1, base_ap2, nickname_puro, nickname_display

    out = r[[
        "reserva_id","fecha_reserva","telefono","telefono_comprador_regalo",
        "nickname","pax_nombre","pax_apellido1","pax_apellido2",
        "nombre_comprador_regalo","apellido1_comprador_regalo","apellido2_comprador_regalo",
        "idioma"
    ]].copy()

    out["telefono_efectivo"] = out.apply(pick_phone, axis=1)

    parts = out.apply(pick_parts, axis=1, result_type="expand")
    parts.columns = ["nombre_base", "apellido1_base", "apellido2_base", "nickname_puro", "nickname_display"]
    out = pd.concat([out, parts], axis=1)

    # Normaliza
    out["nombre_base"]    = out["nombre_base"].fillna("").astype(str).apply(_to_camel_case_name)
    out["apellido1_base"] = out["apellido1_base"].fillna("").astype(str).apply(_to_camel_case_name)
    out["apellido2_base"] = out["apellido2_base"].fillna("").astype(str).apply(_to_camel_case_name)
    out["nickname_puro"]  = out["nickname_puro"].fillna("").astype(str).str.strip()
    out["nickname_display"] = out["nickname_display"].fillna("").astype(str).str.strip()
    out["idioma"]         = out["idioma"].apply(lambda v: _normalize_lang(v, "es"))

    return out[[
        "reserva_id","fecha_reserva","telefono_efectivo","idioma",
        "nombre_base","apellido1_base","apellido2_base","nickname_puro","nickname_display"
    ]]


# ============================================================
# Construcción de "clientes" desde RESERVAS
#  - nombre = 'Nombre (nickname_display) Apellido1 Apellido2' (acotado a 100)
#  - nickname = CamelCase (sin apellidos)
# ============================================================
def _build_display_name_bounded(nombre: str, nickname_display: str, ap1: str, ap2: str, max_len: int = 100) -> str:
    """
    Construye: 'Nombre (nickname_display) Apellido1 Apellido2' y lo ajusta a max_len.
    Prioridad de conservación: Nombre > Apellido1 > Apellido2 > Nickname.
    Estrategia de recorte por pasos:
      1) Quitar Apellido2 si excede.
      2) Recortar nickname_display dentro de paréntesis (o quitarlo).
      3) Recortar Apellido1.
      4) Recortar Nombre.
    """
    def clean(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "").strip())

    n   = _to_camel_case_name(nombre)
    a1  = _to_camel_case_name(ap1)
    a2  = _to_camel_case_name(ap2)
    nkd = (nickname_display or "").strip()  # conserva case original si venía de reserva

    parts = []
    if n:
        parts.append(n)
    use_parens = bool(nkd) and nkd.lower() != n.lower()
    if use_parens:
        parts.append(f"({nkd})")
    if a1:
        parts.append(a1)
    if a2:
        parts.append(a2)

    def join(parts_list: list[str]) -> str:
        return clean(" ".join(p for p in parts_list if p))

    full = join(parts)
    if len(full) <= max_len:
        return full

    # 1) quitar Apellido2
    if a2 and a2 in parts:
        parts_1 = [p for p in parts if p != a2]
        full_1 = join(parts_1)
        if len(full_1) <= max_len:
            return full_1
        parts = parts_1

    # 2) recortar nickname_display
    if use_parens:
        idx = next((i for i, p in enumerate(parts) if p.startswith("(") and p.endswith(")")), None)
        if idx is not None:
            raw_nk = nkd
            for cut in range(len(raw_nk), -1, -1):
                parts[idx] = f"({raw_nk[:cut]})" if cut > 0 else ""
                test = join(parts)
                if len(test) <= max_len:
                    return test
            parts[idx] = ""
            parts = [p for p in parts if p]
            full_2 = join(parts)
            if len(full_2) <= max_len:
                return full_2

    # 3) recortar Apellido1
    if a1 and a1 in parts:
        idx = parts.index(a1)
        for cut in range(len(a1), -1, -1):
            parts[idx] = a1[:cut]
            test = join(parts)
            if len(test) <= max_len:
                return test
        parts[idx] = ""

    # 4) recortar Nombre
    if n and n in parts:
        idx = parts.index(n)
        for cut in range(len(n), -1, -1):
            parts[idx] = n[:cut]
            test = join(parts)
            if len(test) <= max_len:
                return test

    # Fallback mínimo
    return clean((n or a1 or a2 or nkd)[:max_len])


def build_clientes_from_reservas(df_nombre_reserva: pd.DataFrame) -> pd.DataFrame:
    required = {"reserva_id","fecha_reserva","telefono_efectivo","idioma",
                "nombre_base","apellido1_base","apellido2_base","nickname_puro","nickname_display"}
    missing = required - set(df_nombre_reserva.columns)
    if missing:
        raise ValueError(f"build_clientes_from_reservas: faltan columnas: {sorted(missing)}")

    df = df_nombre_reserva.copy()

    # Teléfono y fecha
    df["telefono"] = df["telefono_efectivo"].apply(normalize_phone)
    df = df[df["telefono"].notna()]
    df["fecha_reserva_parsed"] = pd.to_datetime(df["fecha_reserva"], errors="coerce", utc=True)

    # Última por teléfono
    df = df.sort_values(by=["telefono","fecha_reserva_parsed","reserva_id"], ascending=[True,False,False])
    df = df.drop_duplicates(subset=["telefono"], keep="first").copy()

    # nickname_final que guardamos en Supabase (CamelCase; si vacío → nombre_base)
    df["nickname_final"] = df["nickname_puro"].fillna("").astype(str).str.strip()
    mask_empty_nk = df["nickname_final"] == ""
    df.loc[mask_empty_nk, "nickname_final"] = df.loc[mask_empty_nk, "nombre_base"].fillna("").astype(str)
    df["nickname_final"] = df["nickname_final"].apply(_to_camel_case_name)

    # nombre para Supabase con paréntesis usando nickname_display (case original si viene de reserva)
    df["nombre_final"] = [
        _build_display_name_bounded(nb or "", nkd or "", a1 or "", a2 or "", max_len=100)
        for nb, nkd, a1, a2 in zip(df["nombre_base"], df["nickname_display"], df["apellido1_base"], df["apellido2_base"])
    ]

    df["idioma_calc"] = df["idioma"].apply(lambda v: _normalize_lang(v, "es"))

    out = pd.DataFrame({
        "nombre": df["nombre_final"].apply(lambda s: re.sub(r"\s+", " ", str(s or "").strip())),
        "telefono": df["telefono"],
        "nickname": df["nickname_final"],  # guardamos nickname en CamelCase
        "created_at": now_utc(),
        "destino": None,
        "status_drumwit": "cliente_sin_reserva",
        "status_reserva": None,
        "is_user_required": False,
        "is_ai_mode": True,
        "idioma": df["idioma_calc"].apply(lambda v: _normalize_lang(v, "es")),
        "status_boarding_passes": "no_booking",
    })

    out = df_dropna_phones(out, "telefono")
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
# Upsert por teléfono (insert nuevos; update SOLO nombre/nickname[/idioma])
#  - Inserta en BULK los nuevos
#  - Para existentes, BULK upsert con columnas acotadas
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
      - Para EXISTENTES, hace un UPSERT masivo con SOLO ['telefono','nombre','nickname','idioma'].
    Requiere UNIQUE en clientes.telefono.

    En inserts enviamos explícitamente: status_drumwit='cliente_sin_reserva',
    is_user_required=False, is_ai_mode=True, idioma heredado (default 'es').
    En updates NO tocamos flags ni created_at ni otros campos.
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
    update_cols = ["telefono", "nombre", "nickname", "idioma"]  # actualizamos idioma también (de la última reserva)

    total_inserted = 0
    total_updated  = 0

    # ---- BULK INSERT (nuevos) ----
    for i in range(0, len(to_insert), batch_size):
        batch = to_insert.iloc[i:i+batch_size]
        payload = batch[insert_cols].to_dict(orient="records")
        if not payload:
            continue
        try:
            sb.table("clientes").insert(payload).execute()
        except Exception:
            _http_upsert_bulk("clientes", payload, on_conflict="telefono")
        total_inserted += len(payload)

    # ---- BULK UPSERT (existentes) SOLO nombre/nickname/idioma ----
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

    # 4) Upsert (insert nuevos / update solo nombre & nickname & idioma)
    inserted, updated, inserted_phones, updated_phones = upsert_by_phone_only_name(df_clients)
    log.info("Incremental -> Insertados: %d | Actualizados: %d", inserted, updated)


def pipeline_upsert_nocturno(run_query_func) -> None:
    # 1) Todas las reservas + todos los pax
    df_r  = extract_reservas(run_query_func, full=True)
    df_pr = extract_pasajero_reserva(run_query_func)
    df_p  = extract_pasajeros(run_query_func)

    # 2) Nombre por reserva (norma + regalo)
    df_nombre = build_nombre_reserva_por_reserva(df_r, df_pr, df_p)

    # 3) Clientes desde reservas (última por teléfono)
    df_clients = build_clientes_from_reservas(df_nombre)

    # 4) Upsert (insert nuevos / update solo nombre & nickname & idioma)
    inserted, updated, inserted_phones, updated_phones = upsert_by_phone_only_name(df_clients)
    log.info("Nocturno -> Insertados: %d | Actualizados: %d", inserted, updated)


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    mode = os.getenv("ETL_MODE", "nocturno").lower()  # "incremental" o "nocturno"
    mode == "nocturno"
    if mode == "incremental":
        pipeline_incremental_horario(run_query)
    elif mode == "nocturno":
        pipeline_upsert_nocturno(run_query)
    else:
        log.error("ETL_MODE desconocido: %s (usa 'incremental' o 'nocturno')")
        sys.exit(2)
