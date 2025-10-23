# autoschedule_confirmados.py
from __future__ import annotations

import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List, Set

from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError  # para capturar duplicados 23505

# ----------------- Configuración -----------------

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

MAD = ZoneInfo("Europe/Madrid")
# Corte de producción: solo crear envíos con send_datetime Madrid >= este momento
PRODUCTION_CUTOFF_MAD = datetime(2025, 10, 23, 8, 0, 0, tzinfo=MAD)

# Candidatos de flow_id
CONFIRMAR_NORMAL = ["confirmar_mail_confirmacion", "confirmar_mail_confirmacion_1"]
CONFIRMAR_REPETIDOR = ["confirmar_mail_confirmacion_repetidor", "confirmar_mail_confirmacion_repetidor_1"]

CANJEO_CANDS = ["canjeo_bono_1", "canjeo_bono"]

# Regalo: si es repetidor por teléfono usamos el específico, si no una de las dos normales
REGALO_NORMAL = ["tarjeta_regalo", "tarjeta_regalo_1"]
REGALO_REPETIDOR = "tarjeta_regalo_repetidor"

# ----------------- Modelos -----------------

@dataclass
class OPCItem:
    flow_id: str
    flow_type: Optional[str]    # 'confirmados' | 'canjeo' | 'regalo_confirmado' | 'como_ha_ido' | 'vuestra_aventura' | None
    reserva_id: Optional[str]   # TEXT (cliente_reservas.id_reserva); None si flow sin reserva
    cliente_id: int
    json_variables: str
    send_datetime: str          # naive 'YYYY-MM-DD HH:MM:SS' (hora local Madrid sin tz)
    is_send: bool
    is_canceled: bool
    idioma: Optional[str]
    lang: Optional[str]

# ----------------- Utilidades -----------------

def parse_to_madrid(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=MAD)
        return value.astimezone(MAD)

    if isinstance(value, str):
        v = value.strip()
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=MAD)
        return dt.astimezone(MAD)

    raise ValueError(f"No puedo parsear fecha_reserva: {value!r}")

def compute_send_datetime_madrid(created_mad: datetime) -> datetime:
    start = created_mad.replace(hour=7, minute=30, second=0, microsecond=0)
    end   = created_mad.replace(hour=20, minute=30, second=0, microsecond=0)

    if start.time() <= created_mad.time() <= end.time():
        return created_mad + timedelta(minutes=30)

    minutes = created_mad.minute
    if created_mad.time() < start.time():
        base = created_mad.replace(hour=9, minute=0, second=0, microsecond=0)
    else:
        next_day = (created_mad + timedelta(days=1)).date()
        base = datetime(next_day.year, next_day.month, next_day.day, 9, 0, 0, tzinfo=MAD)

    return base.replace(minute=minutes)

def iso_naive(dt_aware: datetime) -> str:
    return dt_aware.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")

def djb2_hash(s: str) -> int:
    h = 5381
    for ch in s:
        h = ((h << 5) + h) + ord(ch)
        h &= 0xFFFFFFFF
    return h

# ----------------- Acceso a Supabase -----------------

def fetch_reservas_since(since_utc: datetime) -> List[Dict[str, Any]]:
    """
    Lee cliente_reservas con fecha_reserva >= since_utc (timestamptz).
    Campos usados:
      id_reserva (TEXT), cliente_id (INT), fecha_reserva (ts/tstz),
      regalo (int/bool), id_reserva_compradora_regalo (TEXT|null)
    """
    since_iso = since_utc.astimezone(timezone.utc).isoformat()
    r = (
        supabase.table("cliente_reservas")
        .select("id_reserva, cliente_id, fecha_reserva, regalo, id_reserva_compradora_regalo")
        .gte("fecha_reserva", since_iso)
        .order("fecha_reserva", desc=False)
        .execute()
    )
    return r.data or []

def fetch_cliente(cliente_id: int) -> Dict[str, Any]:
    r = supabase.table("clientes").select("*").eq("cliente_id", cliente_id).single().execute()
    return r.data or {}

def cliente_tiene_reservas_previas_por_cliente(cliente_id: int, created_mad: datetime, excluir_id_reserva: str) -> bool:
    antes_iso = created_mad.astimezone(timezone.utc).isoformat()
    r = (
        supabase.table("cliente_reservas")
        .select("id_reserva")
        .eq("cliente_id", cliente_id)
        .lt("fecha_reserva", antes_iso)
        .neq("id_reserva", excluir_id_reserva)
        .limit(1)
        .execute()
    )
    return bool(r.data)

def cliente_tiene_reservas_previas_por_telefono(telefono: str, created_mad: datetime, excluir_id_reserva: str) -> bool:
    if not telefono:
        return False
    cl = (supabase.table("clientes").select("cliente_id").eq("telefono", telefono).execute())
    ids = [int(x["cliente_id"]) for x in (cl.data or []) if "cliente_id" in x]
    if not ids:
        return False
    antes_iso = created_mad.astimezone(timezone.utc).isoformat()
    r = (
        supabase.table("cliente_reservas")
        .select("id_reserva, cliente_id")
        .in_("cliente_id", ids)
        .lt("fecha_reserva", antes_iso)
        .neq("id_reserva", excluir_id_reserva)
        .limit(1)
        .execute()
    )
    return bool(r.data)

# ----------------- Existentes en opcionesinicio_client -----------------

def fetch_all_existing_reserva_ids(page_size: int = 5000) -> Set[str]:
    """
    Descarga TODAS las reserva_id actualmente presentes en opcionesinicio_client (paginado).
    Se filtran None/'' en Python.
    """
    existing: Set[str] = set()
    start = 0
    while True:
        # usamos 'id' para paginar con rango estable
        resp = (
            supabase.table("opcionesinicio_client")
            .select("id,reserva_id")
            .order("id", desc=False)
            .range(start, start + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        for row in rows:
            rid = row.get("reserva_id")
            if rid:
                existing.add(str(rid))
        if len(rows) < page_size:
            break
        start += page_size
    return existing

# ----------------- INSERT-ONLY -----------------

def insert_items_individually(items: List[OPCItem]) -> int:
    """
    Inserta items de uno en uno (INSERT puro).
    - Ignora conflictos UNIQUE (23505) sin romper el batch.
    - Devuelve cuántos se insertaron realmente.
    """
    inserted = 0
    for it in items:
        payload = asdict(it)
        try:
            supabase.table("opcionesinicio_client").insert(payload, returning="minimal").execute()
            inserted += 1
        except APIError as e:
            # Si es duplicado UNIQUE (23505), lo ignoramos; cualquier otro error se relanza.
            code = (getattr(e, "code", None) or getattr(e, "args", [{}])[0].get("code"))
            if code == "23505":
                continue
            raise
    return inserted

# ----------------- Builders por tipo -----------------

def build_item_confirmados(row: Dict[str, Any]) -> Optional[OPCItem]:
    id_reserva = str(row["id_reserva"])
    cliente_id = int(row["cliente_id"])
    created_mad = parse_to_madrid(row["fecha_reserva"])
    send_mad = compute_send_datetime_madrid(created_mad)

    if send_mad < PRODUCTION_CUTOFF_MAD:
        return None

    cliente = fetch_cliente(cliente_id)
    nombre = (cliente.get("nombre") or "").strip()
    idioma = (cliente.get("idioma") or cliente.get("lang") or "es")

    es_repetidor = cliente_tiene_reservas_previas_por_cliente(cliente_id, created_mad, excluir_id_reserva=id_reserva)
    candidatos = CONFIRMAR_REPETIDOR if es_repetidor else CONFIRMAR_NORMAL

    flow_id = candidatos[djb2_hash(id_reserva) % len(candidatos)]
    json_vars = json.dumps({"nombre": nombre}, ensure_ascii=False)

    return OPCItem(
        flow_id=flow_id,
        flow_type="confirmados",
        reserva_id=id_reserva,
        cliente_id=cliente_id,
        json_variables=json_vars,
        send_datetime=iso_naive(send_mad),
        is_send=False,
        is_canceled=False,
        idioma=idioma,
        lang=idioma,
    )

def build_item_canjeo(row: Dict[str, Any]) -> Optional[OPCItem]:
    id_reserva = str(row["id_reserva"])
    cliente_id = int(row["cliente_id"])
    created_mad = parse_to_madrid(row["fecha_reserva"])
    send_mad = compute_send_datetime_madrid(created_mad)

    if send_mad < PRODUCTION_CUTOFF_MAD:
        return None

    cliente = fetch_cliente(cliente_id)
    nombre = (cliente.get("nombre") or "").strip()
    idioma = (cliente.get("idioma") or cliente.get("lang") or "es")

    flow_id = CANJEO_CANDS[djb2_hash(id_reserva) % len(CANJEO_CANDS)]
    json_vars = json.dumps({"nombre": nombre}, ensure_ascii=False)

    return OPCItem(
        flow_id=flow_id,
        flow_type="canjeo",
        reserva_id=id_reserva,
        cliente_id=cliente_id,
        json_variables=json_vars,
        send_datetime=iso_naive(send_mad),
        is_send=False,
        is_canceled=False,
        idioma=idioma,
        lang=idioma,
    )

def build_item_regalo(row: Dict[str, Any]) -> Optional[OPCItem]:
    id_reserva = str(row["id_reserva"])
    cliente_id = int(row["cliente_id"])
    created_mad = parse_to_madrid(row["fecha_reserva"])
    send_mad = compute_send_datetime_madrid(created_mad)

    if send_mad < PRODUCTION_CUTOFF_MAD:
        return None

    cliente = fetch_cliente(cliente_id)
    nombre = (cliente.get("nombre") or "").strip()
    idioma = (cliente.get("idioma") or cliente.get("lang") or "es")
    telefono = (cliente.get("telefono") or cliente.get("phone") or "").strip()

    es_repetidor = cliente_tiene_reservas_previas_por_telefono(telefono, created_mad, excluir_id_reserva=id_reserva)

    if es_repetidor and REGALO_REPETIDOR:
        flow_id = REGALO_REPETIDOR
    else:
        flow_id = REGALO_NORMAL[djb2_hash(id_reserva) % len(REGALO_NORMAL)]

    json_vars = json.dumps({"nombre": nombre}, ensure_ascii=False)

    return OPCItem(
        flow_id=flow_id,
        flow_type="regalo_confirmado",
        reserva_id=id_reserva,
        cliente_id=cliente_id,
        json_variables=json_vars,
        send_datetime=iso_naive(send_mad),
        is_send=False,
        is_canceled=False,
        idioma=idioma,
        lang=idioma,
    )

# ----------------- Selector de tipo -----------------

def build_item_for_reserva(row: Dict[str, Any]) -> Optional[OPCItem]:
    """
    - regalo_confirmado  -> si regalo == 1
    - canjeo             -> si id_reserva_compradora_regalo no es nulo
    - confirmados        -> resto
    """
    regalo_flag = row.get("regalo")
    try:
        is_regalo = bool(int(regalo_flag)) if isinstance(regalo_flag, (str, int)) else bool(regalo_flag)
    except Exception:
        is_regalo = bool(regalo_flag)

    id_reserva_compradora_regalo = row.get("id_reserva_compradora_regalo")

    if is_regalo:
        return build_item_regalo(row)
    if id_reserva_compradora_regalo not in (None, "", "null"):
        return build_item_canjeo(row)
    return build_item_confirmados(row)

# ----------------- Runner incremental -----------------

def run_incremental_confirmados(window_hours: int = 24) -> int:
    """
    Regla:
      - Si el item es 'confirmados' y su reserva_id YA existe en cualquier fila de opcionesinicio_client -> NO insertar.
      - Para otros flow_type, se insertan sin mirar esa regla.
    """
    now_utc = datetime.now(timezone.utc)
    since_utc = now_utc - timedelta(hours=max(1, window_hours))

    # 1) Cargar existentes (todas las reserva_id de opcionesinicio_client)
    existing_reserva_ids = fetch_all_existing_reserva_ids()

    # 2) Construir candidatos desde cliente_reservas en la ventana
    reservas = fetch_reservas_since(since_utc)

    items: List[OPCItem] = []
    for r in reservas:
        it = build_item_for_reserva(r)
        if not it:
            continue
        # Regla de filtrado: solo afecta a confirmados
        if it.flow_type == "confirmados" and it.reserva_id in existing_reserva_ids:
            continue
        items.append(it)

    # 3) Insertar SOLO nuevos (uno a uno, ignorando duplicados por UNIQUE si los hubiera)
    inserted = insert_items_individually(items)

    print(f"[incremental] ventana={window_hours}h, candidatos={len(items)}, insertados={inserted}")
    return inserted

# ----------------- Main -----------------

if __name__ == "__main__":
    run_incremental_confirmados(window_hours=24)
