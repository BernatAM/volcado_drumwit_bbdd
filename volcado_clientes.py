# etl_clientes_con_nickname.py
from __future__ import annotations

import os
import sys
import re
import math
import time
import json
import logging
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone
from dotenv import load_dotenv

import pandas as pd
import requests
from supabase import create_client, Client

# ------------------------------------------------------------------
# Config & Logging
# ------------------------------------------------------------------
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

# Usa tu función real de lectura
from bbdd_conection import run_query

# ============================================================
# OpcionesInicio seed (inline)
# ============================================================
VAR_PATTERN = re.compile(r"{{(.*?)}}")

def extract_variables_ordered(texto: str) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for match in VAR_PATTERN.findall(texto or ""):
        if match not in seen:
            seen.add(match)
            ordered.append(match)
    return ordered

def generar_json_variables(variables: List[str], cliente: Dict[str, Any]) -> Dict[str, Any]:
    from collections import OrderedDict
    json_vars = OrderedDict()
    for var in variables:
        var_lower = var.lower()
        if var_lower in ("destino", "ciudad"):
            json_vars[var] = None
            continue
        if any(ch.isdigit() for ch in var):
            json_vars[var] = None
            continue
        if isinstance(cliente, dict) and var_lower in cliente:
            json_vars[var] = cliente.get(var_lower)
        elif isinstance(cliente, dict) and var in cliente:
            json_vars[var] = cliente.get(var)
        else:
            json_vars[var] = None
    return json_vars

def preparar_inserts_opcionesinicio_client(
    plantillas: List[Dict[str, Any]],
    cliente: Dict[str, Any],
    client_id: int,
    lang_cliente: str
) -> List[Dict[str, Any]]:
    inserts: List[Dict[str, Any]] = []
    for plantilla in plantillas:
        texto = plantilla.get("texto")
        flow_id = plantilla.get("flow_id")
        if not texto or not flow_id:
            continue
        variables = extract_variables_ordered(texto)
        json_variables = generar_json_variables(variables, cliente)
        inserts.append({
            "flow_id": flow_id,
            "cliente_id": client_id,
            "json_variables": json.dumps(json_variables, ensure_ascii=False),
            "send_datetime": None,
            "is_send": False,
            "is_canceled": False,
            "idioma": lang_cliente,
            "lang": lang_cliente
        })
    return inserts

def _to_json_safe(v: Any):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, dict):
        return {str(k): _to_json_safe(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [_to_json_safe(x) for x in v]
    try:
        return str(v)
    except Exception:
        return None

def _json_safe_payload(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{str(k): _to_json_safe(v) for k, v in r.items()} for r in rows]

def _insert_with_retry(table, payload: List[Dict[str, Any]], max_retries: int = 3, backoff_base: float = 0.7) -> bool:
    attempt = 0
    last_err = None
    while attempt <= max_retries:
        try:
            table.insert(payload).execute()
            return True
        except Exception as e:
            msg = str(e)
            if any(code in msg for code in (" 500", " 502", " 503", " 504", "Bad Gateway", "Gateway Timeout")):
                last_err = e
                time.sleep(backoff_base * (2 ** attempt))
                attempt += 1
                continue
            raise
    log.warning("insert_with_retry agotado tras %d reintentos: %s", max_retries, last_err)
    return False

def cargar_opcionesinicio_client_bulk(client_ids: List[int], chunk_size: int = 2000) -> None:
    if not client_ids:
        log.info("Sin client_ids para seed de opcionesinicio_client (bulk).")
        return

    have_ids: set[int] = set()
    page = 300
    for i in range(0, len(client_ids), page):
        chunk = client_ids[i:i+page]
        r = (sb.table("opcionesinicio_client")
             .select("cliente_id")
             .in_("cliente_id", chunk)
             .execute())
        for row in (r.data or []):
            cid = row.get("cliente_id")
            if cid is not None:
                have_ids.add(int(cid))

    missing_ids = [int(cid) for cid in client_ids if int(cid) not in have_ids]
    if not missing_ids:
        log.info("Todos los clientes del lote ya tenían opcionesinicio_client.")
        return

    clientes: List[Dict[str, Any]] = []
    for i in range(0, len(missing_ids), page):
        chunk = missing_ids[i:i+page]
        r = (sb.table("clientes")
             .select("*")
             .in_("cliente_id", chunk)
             .execute())
        clientes.extend(r.data or [])
    by_id: Dict[int, Dict[str, Any]] = {int(c["cliente_id"]): c for c in clientes if c and "cliente_id" in c}

    lang_groups: Dict[str, List[int]] = {}
    for cid in missing_ids:
        cl = by_id.get(int(cid), {})
        lang = (cl.get("idioma") or cl.get("lang") or "es")
        lang_groups.setdefault(lang, []).append(int(cid))

    total_inserts = 0
    for lang, ids in lang_groups.items():
        plantillas = (sb.table("opcionesinicio").select("*").eq("lang", lang).execute()).data or []
        if not plantillas:
            log.warning("No hay plantillas en opcionesinicio para lang='%s'. Se salta %d clientes.", lang, len(ids))
            continue

        bulk_inserts: List[Dict[str, Any]] = []
        for cid in ids:
            cl = by_id.get(int(cid), {})
            bulk_inserts.extend(preparar_inserts_opcionesinicio_client(
                plantillas=plantillas, cliente=cl, client_id=int(cid), lang_cliente=lang
            ))

        if not bulk_inserts:
            continue

        bulk_inserts = _json_safe_payload(bulk_inserts)

        def insert_batch_safely(rows: List[Dict[str, Any]], size: int) -> int:
            if not rows:
                return 0
            inserted_here = 0
            for j in range(0, len(rows), size):
                batch = rows[j:j+size]
                ok = _insert_with_retry(sb.table("opcionesinicio_client"), batch)
                if ok:
                    inserted_here += len(batch)
                else:
                    if size > 1:
                        half = math.ceil(size / 2)
                        inserted_here += insert_batch_safely(batch, half)
                    else:
                        log.warning("No se pudo insertar fila individual (se omite). Ejemplo: %s", batch[0])
            return inserted_here

        inserted = insert_batch_safely(bulk_inserts, chunk_size)
        total_inserts += inserted
        log.info("lang='%s': insertadas %d filas en opcionesinicio_client.", lang, inserted)

    log.info("✅ Bulk opcionesinicio_client completado: %d filas insertadas para %d clientes.", total_inserts, len(missing_ids))

def _fetch_client_ids_by_phones(phones: list[str]) -> list[int]:
    if not phones:
        return []
    ids: list[int] = []
    page = 300
    for i in range(0, len(phones), page):
        chunk = phones[i:i+page]
        r = (sb.table("clientes")
              .select("cliente_id, telefono")
              .in_("telefono", chunk)
              .execute())
        for row in (r.data or []):
            if "cliente_id" in row:
                ids.append(int(row["cliente_id"]))
    return ids

def _seed_opcionesinicio_for_phones(phones: list[str]) -> None:
    phones = [str(p) for p in phones if p]
    if not phones:
        return
    client_ids = _fetch_client_ids_by_phones(phones)
    cargar_opcionesinicio_client_bulk(client_ids)

# ============================================================
# Utils
# ============================================================
def normalize_phone(raw: Any) -> Optional[str]:
    """
    E.164 sin '+':
    - deja dígitos
    - '00' -> quita
    - quita ceros domésticos iniciales
    - 9 dígitos (ES) -> antepone 34
    - >15 dígitos -> None
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
    out = df.copy()
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], utc=True, errors="coerce")
        out["created_at"] = out["created_at"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    if "nickname" in out.columns:
        out["nickname"] = out["nickname"].astype(str)
        mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
        out.loc[mask_nn, "nickname"] = out.loc[mask_nn, "nombre"]
        out["nickname"] = out["nickname"].apply(_to_camel_case_name)
    out = out.where(pd.notna(out), None)
    return out

# ============================================================
# Extracciones
# ============================================================
def extract_clientes_origen(run_query, full: bool = True) -> pd.DataFrame:
    query = """SELECT 
                nombre,
                COALESCE(apellido1, '') AS apellido1,
                telefono,
                idioma, 
                id AS cliente_id,
                fecha_activacion
            FROM cliente
    """
    if not full:
        query += " WHERE fecha_activacion > DATE_SUB(CURDATE(), INTERVAL 1 DAY)"
    df = run_query(query)
    return df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)

def extract_clientes_reserva(run_query, full: bool = True, cliente_ids: Optional[List[int]] = None) -> pd.DataFrame:
    base = """SELECT 
                id_reserva as reserva_id,
                id_cliente as cliente_id
              FROM reservas__cliente"""
    if cliente_ids:
        ids = ",".join(str(int(i)) for i in sorted(set(cliente_ids)))
        query = base + f" WHERE id_cliente IN ({ids})"
    else:
        query = base
    return run_query(query)

def extract_reservas(run_query, full: bool = True, reserva_ids: Optional[List[int]] = None) -> pd.DataFrame:
    base = """
    SELECT 
        id as reserva_id,
        localizador,
        fecha_reserva,
        fecha_salida,
        fecha_llegada,
        nickname,
        telefono,
        NULL AS nombre_comprador_reserva
    FROM reserva
    """
    where_clauses = []
    if not full:
        where_clauses.append("fecha_reserva > DATE_SUB(CURDATE(), INTERVAL 1 DAY)")
    if reserva_ids:
        ids = ",".join(str(int(i)) for i in sorted(set(reserva_ids)))
        where_clauses.append(f"id IN ({ids})")
    query = base + (" WHERE " + " AND ".join(where_clauses) if where_clauses else "")
    return run_query(query)

def extract_pasajero_reserva(run_query, reserva_ids: Optional[List[int]] = None) -> pd.DataFrame:
    base = """
    SELECT 
        id_reserva as reserva_id,
        id_pasajero as pasajero_id
    FROM pasajeros__reserva
    """
    if reserva_ids:
        ids = ",".join(str(int(i)) for i in sorted(set(reserva_ids)))
        query = base + f" WHERE id_reserva IN ({ids})"
    else:
        query = base
    return run_query(query)

def extract_pasajeros(run_query, pasajero_ids: Optional[List[int]] = None) -> pd.DataFrame:
    base = """
    SELECT
        id as pasajero_id,
        nombre,
        apellido1 as apellido,
        tipo_pasajero as tipo
    FROM pasajero
    """
    if pasajero_ids:
        ids = ",".join(str(int(i)) for i in sorted(set(pasajero_ids)))
        query = base + f" WHERE id IN ({ids})"
    else:
        query = base
    return run_query(query)

# ============================================================
# Cálculos y construcción de datasets
# ============================================================
def build_nombre_reserva_por_reserva(
    df_reservas: pd.DataFrame,
    df_pasajero_reserva: pd.DataFrame,
    df_pasajeros: pd.DataFrame
) -> pd.DataFrame:
    r = df_reservas.copy()
    for c in ("nickname", "nombre_comprador_reserva"):
        if c not in r.columns:
            r[c] = None
    if "fecha_reserva" not in r.columns:
        r["fecha_reserva"] = None

    pr = df_pasajero_reserva.copy()
    p = df_pasajeros.copy()

    if "tipo" in p.columns:
        p["__is_adult__"] = p["tipo"].apply(_is_adult)
    else:
        p["__is_adult__"] = True

    p_adult = p[p["__is_adult__"] == True].copy()
    pr_adult = pr.merge(p_adult, on="pasajero_id", how="inner")
    pr_adult = pr_adult.sort_values(["reserva_id", "pasajero_id"], ascending=[True, True])

    for c in ("nombre", "apellido"):
        if c not in pr_adult.columns:
            pr_adult[c] = ""
    pr_adult["nombre_apellido"] = (
        pr_adult["nombre"].fillna("").astype(str).str.strip() + " " +
        pr_adult["apellido"].fillna("").astype(str).str.strip()
    ).str.strip()

    first_adult = pr_adult.drop_duplicates(subset=["reserva_id"], keep="first")[["reserva_id", "nombre_apellido"]]
    r = r.merge(first_adult, on="reserva_id", how="left")

    def pick_nombre_reserva(row):
        nick = str(row.get("nickname") or "").strip()
        if nick:
            return nick
        pa = str(row.get("nombre_apellido") or "").strip()
        if pa:
            return pa
        comp = str(row.get("nombre_comprador_reserva") or "").strip()
        if comp:
            return comp
        return None

    out = r[["reserva_id", "fecha_reserva", "nickname", "nombre_comprador_reserva", "nombre_apellido"]].copy()
    out["nombre_reserva_norma"] = out.apply(pick_nombre_reserva, axis=1)
    return out[["reserva_id", "fecha_reserva", "nombre_reserva_norma"]]

def build_nickname_por_cliente(
    df_clientes_reserva: pd.DataFrame,
    df_nombre_reserva_por_reserva: pd.DataFrame,
    df_clientes_src: pd.DataFrame
) -> pd.DataFrame:
    c = df_clientes_src.copy()
    if "apellido1" not in c.columns and "apellido" in c.columns:
        c["apellido1"] = c["apellido"]

    c["__nombre_cliente__"] = (
        c.get("nombre", "").fillna("").astype(str).str.strip() + " " +
        c.get("apellido1", "").fillna("").astype(str).str.strip()
    ).str.strip()
    c = c[["cliente_id", "__nombre_cliente__"]].drop_duplicates("cliente_id")

    cr = df_clientes_reserva.copy()
    nr = df_nombre_reserva_por_reserva.copy()

    x = cr.merge(nr, on="reserva_id", how="left")

    if "fecha_reserva" in x.columns:
        x["fecha_reserva_parsed"] = pd.to_datetime(x["fecha_reserva"], errors="coerce", utc=True)
    else:
        x["fecha_reserva_parsed"] = pd.NaT

    x = x.sort_values(
        by=["cliente_id", "fecha_reserva_parsed", "reserva_id"],
        ascending=[True, False, False]
    ).drop_duplicates(subset=["cliente_id"], keep="first")

    x = x.merge(c, on="cliente_id", how="left")
    x["nickname_resuelto"] = x["nombre_reserva_norma"].astype(str)
    x["nickname_resuelto"] = x["nickname_resuelto"].where(
        x["nickname_resuelto"].fillna("").str.strip() != "",
        x["__nombre_cliente__"].fillna("").astype(str)
    )
    x["nickname_resuelto"] = x["nickname_resuelto"].where(
        x["nickname_resuelto"].fillna("").str.strip() != "",
        "Cliente " + x["cliente_id"].astype(str)
    )
    return x[["cliente_id", "nickname_resuelto"]]

def build_clientes_desde_reservas_sin_cliente(
    df_reservas: pd.DataFrame,
    df_clientes_reserva: pd.DataFrame,
    df_pasajero_reserva: pd.DataFrame,
    df_pasajeros: pd.DataFrame
) -> pd.DataFrame:
    reservas_con_cliente = set(df_clientes_reserva["reserva_id"].dropna().astype(int).tolist())
    r = df_reservas.copy()
    r["reserva_id"] = r["reserva_id"].astype(int)
    r_no_cli = r[~r["reserva_id"].isin(reservas_con_cliente)].copy()
    if r_no_cli.empty:
        return pd.DataFrame(columns=[
            "nombre","telefono","created_at","destino","status_drumwit","status_reserva",
            "is_user_required","is_ai_mode","idioma","status_boarding_passes","nickname"
        ])

    pr = df_pasajero_reserva.copy()
    p = df_pasajeros.copy()
    if "tipo" in p.columns:
        p["__is_adult__"] = p["tipo"].apply(_is_adult)
    else:
        p["__is_adult__"] = True
    p_adult = p[p["__is_adult__"] == True].copy()
    pr_adult = pr.merge(p_adult, on="pasajero_id", how="inner").sort_values(
        ["reserva_id", "pasajero_id"], ascending=[True, True]
    )

    for c in ("nombre", "apellido"):
        if c not in pr_adult.columns:
            pr_adult[c] = ""
    pr_adult["nombre_apellido"] = (
        pr_adult["nombre"].fillna("").astype(str).str.strip() + " " +
        pr_adult["apellido"].fillna("").astype(str).str.strip()
    ).str.strip()
    first_adult = pr_adult.drop_duplicates(subset=["reserva_id"], keep="first")[["reserva_id", "nombre_apellido"]]

    x = r_no_cli.merge(first_adult, on="reserva_id", how="left")

    out = pd.DataFrame({
        "nombre": x["nombre_apellido"].fillna("").astype(str).apply(_to_camel_case_name),
        "telefono": x["telefono"].astype(str),
        "created_at": now_utc(),
        "destino": None,
        "status_drumwit": None,
        "status_reserva": None,
        "is_user_required": None,
        "is_ai_mode": None,
        "idioma": None,
        "status_boarding_passes": None,
        "nickname": x["nombre_apellido"].fillna("").astype(str).apply(_to_camel_case_name),
    })

    out = df_dropna_phones(out, "telefono")

    out["nickname"] = out["nickname"].astype(str)
    mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
    out.loc[mask_nn, "nickname"] = out.loc[mask_nn, "nombre"]
    out["nickname"] = out["nickname"].apply(_to_camel_case_name)
    return out

def map_clientes(
    df_cli_src: pd.DataFrame,
    df_nick_por_cliente: pd.DataFrame
) -> pd.DataFrame:
    df = df_cli_src.copy()

    if "apellido1" not in df.columns and "apellido" in df.columns:
        df["apellido1"] = df["apellido"]

    if {"nombre", "apellido1"}.issubset(df.columns):
        df["nombre_full"] = (
            df["nombre"].fillna("").astype(str).str.strip() + " " +
            df["apellido1"].fillna("").astype(str).str.strip()
        ).str.strip()
        df["nombre_final"] = df["nombre_full"].where(df["nombre_full"] != "", df["nombre"].fillna("").astype(str))
    elif "nombre" in df.columns:
        df["nombre_final"] = df["nombre"].fillna("").astype(str).str.strip()
    else:
        df["nombre_final"] = ""

    tel_col = "telefono" if "telefono" in df.columns else "phone"
    if tel_col not in df.columns:
        raise ValueError("El origen de clientes no trae columna de teléfono ('telefono' o 'phone').")
    df["telefono_norm"] = df[tel_col].apply(normalize_phone)

    if "cliente_id" not in df.columns:
        raise ValueError("El origen de clientes debe incluir 'cliente_id' para calcular nickname por cliente.")
    df = df.merge(df_nick_por_cliente, on="cliente_id", how="left")
    df["nombre_final"] = df["nombre_final"].apply(_to_camel_case_name)

    out = pd.DataFrame({
        "nombre": df["nombre_final"],
        "telefono": df["telefono_norm"],
        "created_at": now_utc(),
        "destino": None,
        "status_drumwit": None,
        "status_reserva": None,
        "is_user_required": None,
        "is_ai_mode": None,
        "idioma": df["idioma"] if "idioma" in df.columns else None,
        "status_boarding_passes": None,
        "nickname": df["nickname_resuelto"]
    })

    out = df_dropna_phones(out, "telefono")

    if "idioma" not in out.columns:
        out["idioma"] = None

    out["nickname"] = out["nickname"].astype(str)
    mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
    out.loc[mask_nn, "nickname"] = out.loc[mask_nn, "nombre"]
    out["nickname"] = out["nickname"].apply(_to_camel_case_name)

    cols = [
        "nombre", "telefono", "created_at", "destino", "status_drumwit", "status_reserva",
        "is_user_required", "is_ai_mode", "idioma", "status_boarding_passes", "nickname"
    ]
    return out[cols]

# ============================================================
# Lectura teléfonos existentes destino (para estimaciones/filtrado)
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
# UPSERT masivo (INSERT + UPDATE) por teléfono
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

def upsert_by_phone(clients_df: pd.DataFrame, batch_size: int = 1000) -> Tuple[int, int, list[str], list[str]]:
    df = sanitize_df_for_json(clients_df.copy())

    existing = fetch_existing_phones()
    maybe_insert_mask = ~df["telefono"].isin(existing)
    maybe_update_mask = ~maybe_insert_mask

    inserted_phones = df.loc[maybe_insert_mask, "telefono"].dropna().astype(str).tolist()
    updated_phones  = df.loc[maybe_update_mask, "telefono"].dropna().astype(str).tolist()
    inserted_aprox = len(inserted_phones)
    updated_aprox  = len(updated_phones)

    total = len(df)
    if total == 0:
        log.info("No hay filas para upsert.")
        return 0, 0, [], []

    cols = [
        "nombre", "telefono", "created_at", "destino", "status_drumwit", "status_reserva",
        "is_user_required", "is_ai_mode", "idioma", "status_boarding_passes", "nickname"
    ]
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas obligatorias en df para upsert: {missing}")

    has_upsert = hasattr(sb.table("clientes"), "upsert")

    done = 0
    for i in range(0, total, batch_size):
        batch = df.iloc[i:i+batch_size]
        payload = batch[cols].to_dict(orient="records")
        if has_upsert:
            sb.table("clientes").upsert(payload, on_conflict="telefono").execute()
        else:
            _http_upsert_bulk("clientes", payload, on_conflict="telefono")
        done += len(payload)

    log.info("Upsert masivo completado: %d filas procesadas en lotes de %d.", done, batch_size)
    return inserted_aprox, updated_aprox, inserted_phones, updated_phones

# ============================================================
# Helpers compuestos
# ============================================================
def _build_df_nickname_por_cliente(run_query_func, full: bool = True) -> pd.DataFrame:
    df_cli_src = extract_clientes_origen(run_query_func, full=full)
    df_cr = extract_clientes_reserva(run_query_func, full=full, cliente_ids=df_cli_src["cliente_id"].tolist())
    reserva_ids = df_cr["reserva_id"].dropna().astype(int).unique().tolist()
    df_r = extract_reservas(run_query_func, full=full, reserva_ids=reserva_ids)
    df_pr = extract_pasajero_reserva(run_query_func, reserva_ids=reserva_ids)
    pasajero_ids = df_pr["pasajero_id"].dropna().astype(int).unique().tolist()
    df_p = extract_pasajeros(run_query_func, pasajero_ids=pasajero_ids)
    df_nombre_por_reserva = build_nombre_reserva_por_reserva(df_r, df_pr, df_p)
    df_nick_por_cliente = build_nickname_por_cliente(df_cr, df_nombre_por_reserva, df_cli_src)
    return df_nick_por_cliente

# ============================================================
# Pipelines
# ============================================================
def pipeline_incremental_horario(run_query_func) -> None:
    df_cli = extract_clientes_origen(run_query_func, full=False)
    df_nick_cli = _build_df_nickname_por_cliente(run_query_func, full=False)
    df_map = map_clientes(df_cli, df_nick_cli)

    df_map["nickname"] = df_map["nickname"].astype(str)
    mask_nn = df_map["nickname"].isna() | (df_map["nickname"].str.strip() == "") | (df_map["nickname"] == "nan")
    df_map.loc[mask_nn, "nickname"] = df_map.loc[mask_nn, "nombre"]

    # Reservas sin cliente (incremental)
    df_cr = extract_clientes_reserva(run_query_func, full=False, cliente_ids=df_cli["cliente_id"].tolist())
    df_r_all = extract_reservas(run_query_func, full=False)
    df_pr = extract_pasajero_reserva(run_query_func, reserva_ids=None)
    pasajero_ids = df_pr["pasajero_id"].dropna().astype(int).unique().tolist()
    df_p = extract_pasajeros(run_query_func, pasajero_ids=pasajero_ids)

    df_pax_extra = build_clientes_desde_reservas_sin_cliente(df_r_all, df_cr, df_pr, df_p)

    if not df_pax_extra.empty:
        phones_in_df_map = set(df_map["telefono"].astype(str).tolist())
        existing = fetch_existing_phones()
        df_pax_extra = df_pax_extra[
            ~df_pax_extra["telefono"].astype(str).isin(phones_in_df_map)
            & ~df_pax_extra["telefono"].isin(existing)
        ]

    df_upsert = pd.concat([df_map, df_pax_extra], ignore_index=True) if not df_pax_extra.empty else df_map

    inserted, updated, inserted_phones, updated_phones = upsert_by_phone(df_upsert)

    try:
        phones = list({*inserted_phones, *updated_phones})
        _seed_opcionesinicio_for_phones(phones)
    except Exception as e:
        log.warning("Seed opcionesinicio_client (incremental) falló: %s", e)

def pipeline_upsert_nocturno(run_query_func) -> None:
    df_cli = extract_clientes_origen(run_query_func, full=True)
    df_nick_cli = _build_df_nickname_por_cliente(run_query_func, full=True)
    df_map = map_clientes(df_cli, df_nick_cli)

    df_map["nickname"] = df_map["nickname"].astype(str)
    mask_nn = df_map["nickname"].isna() | (df_map["nickname"].str.strip() == "") | (df_map["nickname"] == "nan")
    df_map.loc[mask_nn, "nickname"] = df_map.loc[mask_nn, "nombre"]

    # Reservas sin cliente (full)
    df_cr = extract_clientes_reserva(run_query_func, full=True, cliente_ids=df_cli["cliente_id"].tolist())
    df_r_all = extract_reservas(run_query_func, full=True)
    df_pr = extract_pasajero_reserva(run_query_func, reserva_ids=None)
    pasajero_ids = df_pr["pasajero_id"].dropna().astype(int).unique().tolist()
    df_p = extract_pasajeros(run_query_func, pasajero_ids=pasajero_ids)

    df_pax_extra = build_clientes_desde_reservas_sin_cliente(df_r_all, df_cr, df_pr, df_p)

    if not df_pax_extra.empty:
        phones_in_df_map = set(df_map["telefono"].astype(str).tolist())
        existing = fetch_existing_phones()
        df_pax_extra = df_pax_extra[
            ~df_pax_extra["telefono"].astype(str).isin(phones_in_df_map)
            & ~df_pax_extra["telefono"].isin(existing)
        ]

    df_upsert = pd.concat([df_map, df_pax_extra], ignore_index=True) if not df_pax_extra.empty else df_map

    inserted, updated, inserted_phones, updated_phones = upsert_by_phone(df_upsert)

    try:
        phones = list({*inserted_phones, *updated_phones})
        _seed_opcionesinicio_for_phones(phones)
    except Exception as e:
        log.warning("Seed opcionesinicio_client (nocturno) falló: %s", e)

# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    mode = os.getenv("ETL_MODE", "nocturno").lower()  # por defecto, "nocturno"
    if mode == "incremental":
        pipeline_incremental_horario(run_query)
    elif mode == "nocturno":
        pipeline_upsert_nocturno(run_query)
    else:
        log.error("ETL_MODE desconocido: %s (usa 'incremental' o 'nocturno')", mode)
        sys.exit(2)
