# etl_clientes_con_nickname.py
from __future__ import annotations

import os
import sys
import logging
from typing import Any, Dict, Optional, Tuple, List
from datetime import datetime, timezone
from dotenv import load_dotenv

import pandas as pd
from supabase import create_client, Client

# Usa tu función real de lectura
from bbdd_conection import run_query

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


# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------
def normalize_phone(raw: Any) -> Optional[str]:
    """
    Reglas:
    - Quitar espacios, paréntesis, guiones, puntos, NBSP.
    - Si empieza por '+', quitar el '+'. (ej: +34612345678 -> 34612345678)
    - Si NO empieza por '+' ni por '34', anteponer '34'. (ej: 612345678 -> 34612345678)
    - No introducir espacios.
    """
    if raw is None:
        return None
    s = str(raw).strip()

    # quitar separadores
    for ch in ("(", ")", " ", "-", ".", "\u00A0"):
        s = s.replace(ch, "")

    if not s:
        return None

    # si empieza por '+', quitarlo
    if s.startswith("+"):
        s = s[1:]

    # si no empieza por '34', anteponer '34'
    if not s.startswith("34"):
        s = "34" + s

    return s or None


def _to_camel_case_name(x: Any) -> str:
    """
    Convierte a 'CamelCase' por palabra: primera letra mayúscula, resto minúsculas.
    Maneja espacios múltiples y guiones simples.
    """
    if x is None:
        return ""
    s = str(x).strip().lower()
    if not s:
        return ""
    # titulo básico por palabras
    parts = []
    for token in s.split():
        # soporta guiones: 'maria-jose' -> 'Maria-Jose'
        subparts = [sp.capitalize() for sp in token.split("-")]
        parts.append("-".join(subparts))
    return " ".join(parts)



def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def df_dropna_phones(df: pd.DataFrame, phone_col: str) -> pd.DataFrame:
    df = df.copy()
    df[phone_col] = df[phone_col].apply(normalize_phone)
    df = df[df[phone_col].notna()]
    df = df[df[phone_col].astype(str).str.len() > 0]
    df = df.drop_duplicates(subset=[phone_col], keep="first")
    return df

def _is_adult(v) -> bool:
    if pd.isna(v):
        return False
    s = str(v).strip().upper()
    # casos típicos: 0, ADT/ADL/ADU, ADULT...
    if s in {"0", "ADT", "ADL", "ADU", "ADULT", "ADULT0"}:
        return True
    try:
        return int(s) == 0
    except:
        return False

def sanitize_df_for_json(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Datetimes -> ISO 8601 (UTC)
    - NaN -> None
    - nickname nunca vacío (si vacío/NaN/'nan' => nombre)
    """
    out = df.copy()

    # Datetimes -> ISO
    if "created_at" in out.columns:
        out["created_at"] = pd.to_datetime(out["created_at"], utc=True, errors="coerce")
        out["created_at"] = out["created_at"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # nickname nunca vacío
    if "nickname" in out.columns:
        out["nickname"] = out["nickname"].astype(str)
        mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
        out.loc[mask_nn, "nickname"] = out.loc[mask_nn, "nombre"]
        out["nickname"] = out["nickname"].apply(_to_camel_case_name)

    # NaN -> None
    out = out.where(pd.notna(out), None)
    return out


# ------------------------------------------------------------
# Extracciones (con filtros opcionales)
# ------------------------------------------------------------
def extract_clientes_origen(run_query, full: bool = True) -> pd.DataFrame:
    """
    Debe devolver como mínimo: cliente_id, nombre, (apellido o apellido1), telefono, (opcional idioma), fecha_activacion
    """
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
    """
    Debe devolver: cliente_id, reserva_id
    """
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
    """
    Debe devolver: reserva_id, fecha_reserva, nickname, nombre_comprador_reserva (si existe; aquí puede ser NULL)
    """
    base = """
    SELECT 
        id as reserva_id,
        localizador,
        fecha_reserva,
        fecha_salida,
        fecha_llegada,
        nickname,
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
    """
    Debe devolver: reserva_id, pasajero_id
    """
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
    """
    Debe devolver: pasajero_id, nombre, apellido, tipo (0=adulto)
    """
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


# ------------------------------------------------------------
# Cálculo del "nombre de la reserva" por la Norma 1→2→3
# ------------------------------------------------------------
def build_nombre_reserva_por_reserva(
    df_reservas: pd.DataFrame,
    df_pasajero_reserva: pd.DataFrame,
    df_pasajeros: pd.DataFrame
) -> pd.DataFrame:
    """
    Retorna DF: reserva_id, fecha_reserva, nombre_reserva_norma
    Norma:
      1) reservas.nickname si no vacío
      2) primer pasajero adulto (tipo=0) con menor pasajero_id (nombre + apellido)
      3) reservas.nombre_comprador_reserva (si existiera; en nuestro caso suele ser NULL)
    """
    r = df_reservas.copy()
    for c in ("nickname", "nombre_comprador_reserva"):
        if c not in r.columns:
            r[c] = None
    if "fecha_reserva" not in r.columns:
        r["fecha_reserva"] = None

    # 2) Primer pasajero adulto por reserva (min pasajero_id)
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

    # 1,2,3) coalesce según norma
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


# ------------------------------------------------------------
# Agregación: último nickname por cliente (con fallback = nombre del cliente)
# ------------------------------------------------------------
def build_nickname_por_cliente(
    df_clientes_reserva: pd.DataFrame,
    df_nombre_reserva_por_reserva: pd.DataFrame,
    df_clientes_src: pd.DataFrame
) -> pd.DataFrame:
    """
    Une cliente_id <-> reserva_id con el nombre_reserva_norma y devuelve
    el último (por fecha_reserva desc) como nickname del cliente.
    Fallbacks:
      - Si nombre_reserva_norma está vacío, usar nombre del cliente.
      - Si tampoco hay nombre de cliente, "Cliente <cliente_id>".
    Retorna DF: cliente_id, nickname_resuelto
    """
    # nombre del cliente (full)
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
    )
    x = x.drop_duplicates(subset=["cliente_id"], keep="first")

    # fallback 3: nombre del cliente
    x = x.merge(c, on="cliente_id", how="left")
    x["nickname_resuelto"] = x["nombre_reserva_norma"].astype(str)
    x["nickname_resuelto"] = x["nickname_resuelto"].where(
        x["nickname_resuelto"].fillna("").str.strip() != "",
        x["__nombre_cliente__"].fillna("").astype(str)
    )
    # Último recurso
    x["nickname_resuelto"] = x["nickname_resuelto"].where(
        x["nickname_resuelto"].fillna("").str.strip() != "",
        "Cliente " + x["cliente_id"].astype(str)
    )

    return x[["cliente_id", "nickname_resuelto"]]


# ------------------------------------------------------------
# Lectura teléfonos existentes destino
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# Mapeo origen -> destino (incluye nickname y garantiza no-vacío)
# ------------------------------------------------------------
def map_clientes(
    df_cli_src: pd.DataFrame,
    df_nick_por_cliente: pd.DataFrame
) -> pd.DataFrame:
    """
    Construye DataFrame destino con columnas:
      nombre, telefono, created_at, destino(NULL), status_drumwit(NULL), status_reserva(NULL),
      is_user_required(NULL), is_ai_mode(NULL), idioma(NULL->default 'es'), status_boarding_passes(NULL),
      nickname (nunca vacío)
    """
    df = df_cli_src.copy()

    # nombre final
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

    # telefono normalizado
    tel_col = "telefono" if "telefono" in df.columns else "phone"
    if tel_col not in df.columns:
        raise ValueError("El origen de clientes no trae columna de teléfono ('telefono' o 'phone').")
    df["telefono_norm"] = df[tel_col].apply(normalize_phone)

    # une nickname por cliente_id
    if "cliente_id" not in df.columns:
        raise ValueError("El origen de clientes debe incluir 'cliente_id' para calcular nickname por cliente.")
    df = df.merge(df_nick_por_cliente, on="cliente_id", how="left")
    df["nombre_final"] = df["nombre_final"].apply(_to_camel_case_name)

    # preparar columnas finales
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

    # filtra teléfonos válidos y dedup por teléfono (intra-batch)
    out = df_dropna_phones(out, "telefono")

    # si idioma no viene, deja None para que aplique DEFAULT 'es'
    if "idioma" not in out.columns:
        out["idioma"] = None

    # Cinturón final: si nickname vacío, usar nombre
    out["nickname"] = out["nickname"].astype(str)
    mask_nn = out["nickname"].isna() | (out["nickname"].str.strip() == "") | (out["nickname"] == "nan")
    out.loc[mask_nn, "nickname"] = out.loc[mask_nn, "nombre"]
    out["nickname"] = out["nickname"].apply(_to_camel_case_name)

    # orden final
    cols = [
        "nombre", "telefono", "created_at", "destino", "status_drumwit", "status_reserva",
        "is_user_required", "is_ai_mode", "idioma", "status_boarding_passes", "nickname"
    ]
    return out[cols]


# ------------------------------------------------------------
# Inserción (incremental horario: solo nuevos)
# ------------------------------------------------------------
def insert_only_new(clients_df: pd.DataFrame, existing_phones: set[str], batch_size: int = 500) -> Tuple[int, int]:
    to_insert = clients_df[~clients_df["telefono"].isin(existing_phones)].copy()
    if to_insert.empty:
        log.info("No hay clientes nuevos que insertar.")
        return 0, len(clients_df)

    # Sanitiza para JSON (fechas, NaN, nickname)
    to_insert = sanitize_df_for_json(to_insert)

    inserted = 0
    omitted = len(clients_df) - len(to_insert)
    for i in range(0, len(to_insert), batch_size):
        payload = to_insert.iloc[i:i+batch_size].to_dict(orient="records")
        sb.table("clientes").insert(payload).execute()
        inserted += len(payload)
    log.info("Insertados: %d | Omitidos (ya existían): %d", inserted, omitted)
    return inserted, omitted


# ------------------------------------------------------------
# Upsert nocturno (por teléfono) con actualización de nickname
# ------------------------------------------------------------
def upsert_by_phone(clients_df: pd.DataFrame, batch_size: int = 300) -> Tuple[int, int]:
    existing = fetch_existing_phones()
    to_insert = clients_df[~clients_df["telefono"].isin(existing)].copy()
    to_update = clients_df[clients_df["telefono"].isin(existing)].copy()

    # Sanitiza ambos
    to_insert = sanitize_df_for_json(to_insert)
    to_update = sanitize_df_for_json(to_update)

    inserted = 0
    updated = 0

    # INSERT nuevos
    for i in range(0, len(to_insert), batch_size):
        payload = to_insert.iloc[i:i+batch_size].to_dict(orient="records")
        sb.table("clientes").insert(payload).execute()
        inserted += len(payload)

    # UPDATE existentes (nombre, idioma, nickname)
    update_cols = ["nombre", "idioma", "nickname"]
    if not to_update.empty:
        for _, row in to_update.iterrows():
            payload = {c: row[c] for c in update_cols if c in row and row[c] is not None and str(row[c]).strip() != ""}
            if not payload:
                continue
            sb.table("clientes").update(payload).eq("telefono", row["telefono"]).execute()
            updated += 1

    log.info("Upsert nocturno -> Insertados: %d | Actualizados: %d", inserted, updated)
    return inserted, updated


# ------------------------------------------------------------
# Helpers de construcción de nickname por cliente
# ------------------------------------------------------------
def _build_df_nickname_por_cliente(run_query_func, full: bool = True) -> pd.DataFrame:
    # 1) clientes (para fallback del nombre del cliente)
    df_cli_src = extract_clientes_origen(run_query_func, full=full)

    # 2) relaciones cliente-reserva
    df_cr = extract_clientes_reserva(run_query_func, full=full, cliente_ids=df_cli_src["cliente_id"].tolist())

    # 3) reservas (filtradas por las relaciones)
    reserva_ids = df_cr["reserva_id"].dropna().astype(int).unique().tolist()
    df_r = extract_reservas(run_query_func, full=full, reserva_ids=reserva_ids)

    # 4) pasajeros de esas reservas y sus datos
    df_pr = extract_pasajero_reserva(run_query_func, reserva_ids=reserva_ids)
    pasajero_ids = df_pr["pasajero_id"].dropna().astype(int).unique().tolist()
    df_p = extract_pasajeros(run_query_func, pasajero_ids=pasajero_ids)

    # 5) nombre de la reserva por norma 1→2→3
    df_nombre_por_reserva = build_nombre_reserva_por_reserva(df_r, df_pr, df_p)

    # 6) último nickname por cliente (con fallback al nombre del cliente)
    df_nick_por_cliente = build_nickname_por_cliente(df_cr, df_nombre_por_reserva, df_cli_src)
    return df_nick_por_cliente


# ------------------------------------------------------------
# Pipelines
# ------------------------------------------------------------
def pipeline_incremental_horario(run_query_func) -> None:
    # Origen principal de clientes (último día)
    df_cli = extract_clientes_origen(run_query_func, full=False)
    # Nickname por cliente (últimas reservas)
    df_nick_cli = _build_df_nickname_por_cliente(run_query_func, full=False)
    # Map a destino (incluye nickname nunca vacío)
    df_map = map_clientes(df_cli, df_nick_cli)

    # Cinturón final (por si acaso)
    df_map["nickname"] = df_map["nickname"].astype(str)
    mask_nn = df_map["nickname"].isna() | (df_map["nickname"].str.strip() == "") | (df_map["nickname"] == "nan")
    df_map.loc[mask_nn, "nickname"] = df_map.loc[mask_nn, "nombre"]

    # Inserta solo nuevos por teléfono
    existing = fetch_existing_phones()
    insert_only_new(df_map, existing)

def pipeline_upsert_nocturno(run_query_func) -> None:
    df_cli = extract_clientes_origen(run_query_func, full=True)
    df_nick_cli = _build_df_nickname_por_cliente(run_query_func, full=True)
    df_map = map_clientes(df_cli, df_nick_cli)

    # Cinturón final (por si acaso)
    df_map["nickname"] = df_map["nickname"].astype(str)
    mask_nn = df_map["nickname"].isna() | (df_map["nickname"].str.strip() == "") | (df_map["nickname"] == "nan")
    df_map.loc[mask_nn, "nickname"] = df_map.loc[mask_nn, "nombre"]

    upsert_by_phone(df_map)


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
if __name__ == "__main__":
    mode = os.getenv("ETL_MODE", "incremental").lower()
    if mode == "incremental":
        pipeline_incremental_horario(run_query)
    elif mode == "nocturno":
        pipeline_upsert_nocturno(run_query)
    else:
        log.error("ETL_MODE desconocido: %s (usa 'incremental' o 'nocturno')")
        sys.exit(2)
