# autoschedule.py
from __future__ import annotations
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass
import json
from datetime import datetime, timezone

# Importa tus utilidades/servicios existentes
# from app.config import Settings
# from app.services.db_supabase import insert_opcionesinicio_client_batch
# from app.services.db_supabase import get_cliente_by_phone

# --- Modelos ligeros para tipado (puedes usar tus Pydantic existentes) ---

@dataclass
class AutoScheduleRequest:
    telefono: str
    idioma: Optional[str] = None
    destino: Optional[str] = None
    fecha_reserva: Optional[datetime] = None
    fecha_vuelo: Optional[datetime] = None     # (ida)
    fecha_vuelta: Optional[datetime] = None
    repetidor: bool = False

@dataclass
class AutoScheduleItem:
    flow_id: str
    cliente_id: int
    json_variables: str
    send_datetime: str
    idioma: Optional[str]

@dataclass
class AutoScheduleResponse:
    cliente_id: int
    upserted: int
    items: List[Dict[str, Any]]

# --- Helpers (idénticos a los que ya usas) ---

def _to_naive_utc(dt: datetime) -> datetime:
    """Devuelve datetime naive en UTC (yyyy-mm-dd HH:MM:SS)"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # asumimos que viene en hora local y la tratamos como UTC "naive" para consistencia
        # si prefieres Europe/Madrid -> UTC, haz la conversión aquí
        return dt.replace(tzinfo=None)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)

def _deterministic_choice(candidates: List[str], key: str) -> str:
    """Selección determinista estable entre candidatos a partir de una key."""
    if not candidates:
        raise ValueError("No hay candidatos disponibles para la selección del flow.")
    # hash estable
    h = 0
    for ch in key:
        h = (h * 33 + ord(ch)) & 0xFFFFFFFF
    return candidates[h % len(candidates)]

# --- FUNCIÓN PURA REUTILIZABLE ---

def create_autoscheduled_messages_for_cliente(
    payload: AutoScheduleRequest,
    *,
    get_cliente_by_phone: Callable[[str], Optional[Dict[str, Any]]],
    insert_opcionesinicio_client_batch: Callable[[List[Dict[str, Any]]], List[Dict[str, Any]]],
    salt: str = "drumwit-default-salt",
) -> AutoScheduleResponse:
    """
    Genera y upserta las 'opciones de inicio' programadas para un cliente
    a partir de fechas/destino. Lanza ValueError en errores de negocio.
    """

    # 1) Cliente por teléfono
    cliente = get_cliente_by_phone('34657705038')#payload.telefono)
    if not cliente:
        raise ValueError(f"Cliente no encontrado para telefono={payload.telefono}")

    cliente_id = int(cliente["cliente_id"])
    nombre = (cliente.get("nombre") or "").strip()

    # 2) Candidatos por tipo (igual que antes)
    if payload.repetidor:
        confirmar_candidates = ["confirmar_mail_confirmacion_repetidor", "confirmar_mail_confirmacion_repetidor_1"]
    else:
        confirmar_candidates = ["confirmar_mail_confirmacion", "confirmar_mail_confirmacion_1"]

    como_ha_ido_candidates = ["como_ha_ido_vuelo_hotel", "como_ha_ido_vuelo_hotel_2", "como_va_por_ciudad"]
    vuestra_aventura_candidates = ["vuestra_aventura", "vuestra_aventura_2"]

    # 3) Construcción condicional de filas
    rows: List[Dict[str, Any]] = []

    # --- bloque: confirmado (fecha_reserva) ---
    if payload.fecha_reserva:
        dt_confirmado = _to_naive_utc(payload.fecha_reserva)
        k1 = f"{salt}|{cliente_id}|confirmado|{dt_confirmado.isoformat(sep=' ', timespec='seconds')}"
        flow_confirmado = _deterministic_choice(confirmar_candidates, key=k1)

        json_confirmado = json.dumps({"nombre": nombre}, ensure_ascii=False)

        rows.append({
            "flow_id": flow_confirmado,
            "cliente_id": cliente_id,
            "json_variables": json_confirmado,
            "send_datetime": dt_confirmado.isoformat(sep=" ", timespec="seconds"),
            "idioma": payload.idioma or None,
        })

    # --- bloque: como_ha_ido (fecha_vuelo/ida) ---
    if payload.fecha_vuelo:
        if not payload.destino:
            raise ValueError("El campo 'destino' es obligatorio cuando se informa 'fecha_vuelo'.")
        dt_como_ha_ido = _to_naive_utc(payload.fecha_vuelo)
        k2 = f"{salt}|{cliente_id}|como_ha_ido|{dt_como_ha_ido.isoformat(sep=' ', timespec='seconds')}"
        flow_como_ha_ido = _deterministic_choice(como_ha_ido_candidates, key=k2)

        json_como_ha_ido = json.dumps({"nombre": nombre, "ciudad": payload.destino}, ensure_ascii=False)

        rows.append({
            "flow_id": flow_como_ha_ido,
            "cliente_id": cliente_id,
            "json_variables": json_como_ha_ido,
            "send_datetime": dt_como_ha_ido.isoformat(sep=" ", timespec="seconds"),
            "idioma": payload.idioma or None,
        })

    # --- bloque: vuestra_aventura (fecha_vuelta) ---
    if payload.fecha_vuelta:
        dt_vuestra = _to_naive_utc(payload.fecha_vuelta)
        k3 = f"{salt}|{cliente_id}|vuestra_aventura|{dt_vuestra.isoformat(sep=' ', timespec='seconds')}"
        flow_vuestra = _deterministic_choice(vuestra_aventura_candidates, key=k3)

        json_vuestra = json.dumps({"nombre": nombre}, ensure_ascii=False)

        rows.append({
            "flow_id": flow_vuestra,
            "cliente_id": cliente_id,
            "json_variables": json_vuestra,
            "send_datetime": dt_vuestra.isoformat(sep=" ", timespec="seconds"),
            "idioma": payload.idioma or None,
        })

    # 4) Validación final
    if not rows:
        raise ValueError("No se ha proporcionado ninguna fecha válida (fecha_reserva, fecha_vuelo o fecha_vuelta).")

    # 5) Upsert por batch
    upserted = insert_opcionesinicio_client_batch(rows)

    return AutoScheduleResponse(
        cliente_id=cliente_id,
        upserted=len(upserted),
        items=upserted,
    )
