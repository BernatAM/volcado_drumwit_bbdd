# autoschedule_confirmados.py
from __future__ import annotations

import os
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from supabase import create_client, Client

# ----------------- Configuración -----------------

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

MAD = ZoneInfo("Europe/Madrid")
# Corte de producción: solo crear envíos con send_datetime Madrid >= este momento
PRODUCTION_CUTOFF_MAD = datetime(2025, 10, 22, 10, 0, 0, tzinfo=MAD)

# Candidatos de flow_id
CONFIRMAR_NORMAL = ["confirmar_mail_confirmacion", "confirmar_mail_confirmacion_1"]
CONFIRMAR_REPETIDOR = ["confirmar_mail_confirmacion_repetidor", "confirmar_mail_confirmacion_repetidor_1"]

# ----------------- Modelos -----------------

@dataclass
class OPCItem:
    flow_id: str
    flow_type: Optional[str]    # 'confirmados' | 'como_ha_ido' | 'vuestra_aventura' | None
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
    """
    Convierte 'value' a datetime aware en Europe/Madrid.
    Reglas:
      - Si es str y NO tiene tz (naive) -> interpretar como hora de Madrid.
      - Si es str con tz (Z, +01:00, etc.) -> convertir a Madrid.
      - Si es datetime naive -> asumir que está en hora de Madrid.
      - Si es datetime aware -> convertir a Madrid.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=MAD)
        return value.astimezone(MAD)

    if isinstance(value, str):
        v = value.strip()
        # fromisoformat admite 'YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]' o con 'Z' si la sustituimos
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=MAD)
        return dt.astimezone(MAD)

    raise ValueError(f"No puedo parsear fecha_reserva: {value!r}")

def compute_send_datetime_madrid(created_mad: datetime) -> datetime:
    """
    Reglas:
      - Si hora de creación en [07:30, 20:30] -> send = creación + 30 min
      - Si fuera -> siguiente 08:MM (mismo día si <07:30; día siguiente si >20:30)
    """
    start = created_mad.replace(hour=7, minute=30, second=0, microsecond=0)
    end   = created_mad.replace(hour=20, minute=30, second=0, microsecond=0)

    if start.time() <= created_mad.time() <= end.time():
        return created_mad + timedelta(minutes=30)

    minutes = created_mad.minute
    if created_mad.time() < start.time():
        base = created_mad.replace(hour=8, minute=0, second=0, microsecond=0)
    else:
        next_day = (created_mad + timedelta(days=1)).date()
        base = datetime(next_day.year, next_day.month, next_day.day, 8, 0, 0, tzinfo=MAD)

    return base.replace(minute=minutes)

def iso_naive(dt_aware: datetime) -> str:
    """Devuelve 'YYYY-MM-DD HH:MM:SS' sin tz."""
    return dt_aware.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")

def djb2_hash(s: str) -> int:
    """Hash determinista simple para elegir variantes de flow_id a partir de id_reserva TEXT."""
    h = 5381
    for ch in s:
        h = ((h << 5) + h) + ord(ch)  # h*33 + ch
        h &= 0xFFFFFFFF
    return h

# ----------------- Acceso a Supabase -----------------

def fetch_reservas_since(since_utc: datetime) -> List[Dict[str, Any]]:
    """
    Lee cliente_reservas con fecha_reserva >= since_utc (timestamptz).
    Campos usados: id_reserva (TEXT), cliente_id (INT), fecha_reserva (timestamptz o timestamp sin tz).
    """
    # Si la columna es timestamptz, comparar en UTC es correcto.
    # Si por algún motivo fuera timestamp sin tz (local Madrid),
    # este filtro podría excluir/incluir bordes; puedes subir la ventana si te preocupa.
    since_iso = since_utc.astimezone(timezone.utc).isoformat()
    r = (
        supabase.table("cliente_reservas")
        .select("id_reserva, cliente_id, fecha_reserva")
        .gte("fecha_reserva", since_iso)
        .order("fecha_reserva", desc=False)
        .execute()
    )
    return r.data or []

def fetch_cliente(cliente_id: int) -> Dict[str, Any]:
    r = supabase.table("clientes").select("*").eq("cliente_id", cliente_id).single().execute()
    return r.data or {}

def cliente_tiene_reservas_previas(cliente_id: int, created_mad: datetime, excluir_id_reserva: str) -> bool:
    """
    ¿El cliente tiene alguna reserva anterior a 'created_mad' (hora Madrid) distinta de 'excluir_id_reserva'?
    Convertimos a UTC para comparar si la columna es timestamptz.
    """
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
    rows = r.data or []
    return len(rows) > 0

def upsert_opc_items(items: List[OPCItem]) -> int:
    """
    Upsert idempotente separando:
      A) con reserva → on_conflict(flow_type,reserva_id)
      B) sin reserva → on_conflict(flow_id,cliente_id,lang,send_datetime)
    """
    if not items:
        return 0

    with_reserva = [
        asdict(x) for x in items
        if x.reserva_id is not None and x.flow_type is not None
    ]
    sin_reserva = [
        asdict(x) for x in items
        if x.reserva_id is None
    ]

    total = 0

    if with_reserva:
        supabase.table("opcionesinicio_client").upsert(
            with_reserva,
            on_conflict="flow_type,reserva_id",
            returning="minimal",
        ).execute()
        total += len(with_reserva)

    if sin_reserva:
        supabase.table("opcionesinicio_client").upsert(
            sin_reserva,
            on_conflict="flow_id,cliente_id,lang,send_datetime",
            returning="minimal",
        ).execute()
        total += len(sin_reserva)

    return total

# ----------------- Construcción de "confirmados" -----------------

def build_confirmados_item_for_reserva(row: Dict[str, Any]) -> Optional[OPCItem]:
    """
    Construye 1 fila de 'confirmados' para una reserva:
      - Interpreta fecha_reserva como hora de Madrid si llega naive
      - Calcula send_datetime con regla Madrid
      - Respeta corte de producción
      - flow_id repetidor/normal en base a histórico del cliente
    """
    id_reserva = str(row["id_reserva"])
    cliente_id = int(row["cliente_id"])

    # AHORA: parseamos directamente a Madrid (naive => Madrid; aware => convertido a Madrid)
    created_mad = parse_to_madrid(row["fecha_reserva"])
    send_mad = compute_send_datetime_madrid(created_mad)

    # Respetar el corte de producción
    if send_mad < PRODUCTION_CUTOFF_MAD:
        return None

    cliente = fetch_cliente(cliente_id)
    nombre = (cliente.get("nombre") or "").strip()
    idioma = (cliente.get("idioma") or cliente.get("lang") or "es")

    # ¿repetidor? (comparamos en UTC para mayor compatibilidad con timestamptz)
    es_repetidor = cliente_tiene_reservas_previas(cliente_id, created_mad, excluir_id_reserva=id_reserva)
    candidatos = CONFIRMAR_REPETIDOR if es_repetidor else CONFIRMAR_NORMAL

    # Elección determinista en base a id_reserva (texto)
    h = djb2_hash(id_reserva)
    flow_id = candidatos[h % len(candidatos)]

    json_vars = json.dumps({"nombre": nombre}, ensure_ascii=False)

    return OPCItem(
        flow_id=flow_id,
        flow_type="confirmados",
        reserva_id=id_reserva,             # TEXT
        cliente_id=cliente_id,
        json_variables=json_vars,
        send_datetime=iso_naive(send_mad), # naive, hora Madrid
        is_send=False,
        is_canceled=False,
        idioma=idioma,
        lang=idioma,
    )

# ----------------- Runner incremental -----------------

def run_incremental_confirmados(window_hours: int = 24) -> int:
    """
    Ejecuta una ventana deslizante (p.ej. 24h) sobre cliente_reservas.fecha_reserva.
    Idempotente gracias a las UNIQUE constraints.
    """
    now_utc = datetime.now(timezone.utc)
    since_utc = now_utc - timedelta(hours=max(1, window_hours))

    reservas = fetch_reservas_since(since_utc)

    items: List[OPCItem] = []
    for r in reservas:
        it = build_confirmados_item_for_reserva(r)
        if it:
            items.append(it)

    inserted = upsert_opc_items(items)
    print(f"[incremental] ventana={window_hours}h, candidatos={len(items)}, upsertados={inserted}")
    return inserted

# ----------------- Main -----------------

if __name__ == "__main__":
    # Llama a este script cada 30 minutos (cron/Task Scheduler/systemd). Ajusta la ventana si quieres.
    run_incremental_confirmados(window_hours=24)
