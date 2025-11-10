# autoschedule_viajes.py
from __future__ import annotations

import os
import json
import argparse
from dataclasses import dataclass, asdict
from datetime import datetime, date, time, timedelta, timezone
from typing import Optional, Dict, Any, List, Union, Tuple, Set

from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from supabase import create_client, Client

# ----------------- Config -----------------

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Faltan SUPABASE_URL / SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
MAD = ZoneInfo("Europe/Madrid")

# flow_ids fijos según reglas
COMO_HA_IDO_NORMAL = "como_ha_ido_vuelo_hotel"
COMO_HA_IDO_REPETIDOR = "como_va_por_ciudad"
COMO_HA_IDO_NEXTDAY = "como_ha_ido_vuelo_hotel_2"

VUESTRA_AVENTURA_NORMAL = "vuestra_aventura"
VUESTRA_AVENTURA_REPETIDOR = "vuetra_aventura_2"  # (tal cual en tu BBDD)

# ----------------- Modelos -----------------

@dataclass
class OPCItem:
    flow_id: str
    flow_type: str                 # 'como_ha_ido' | 'vuestra_aventura'
    reserva_id: str                # id_reserva TEXT
    cliente_id: int
    json_variables: str
    send_datetime: str             # naive 'YYYY-MM-DD HH:MM:SS' (en hora Madrid, sin tz)
    is_send: bool
    is_canceled: bool
    idioma: Optional[str]
    lang: Optional[str]

# ----------------- Utils -----------------

def djb2_hash(s: str) -> int:
    h = 5381
    for ch in s:
        h = ((h << 5) + h) + ord(ch)
        h &= 0xFFFFFFFF
    return h

def iso_naive(dt_aware_mad: datetime) -> str:
    return dt_aware_mad.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")

def today_madrid() -> date:
    return datetime.now(MAD).date()

def to_date(value: Any) -> Optional[date]:
    """Acepta date | datetime | 'YYYY-MM-DD' | None -> date | None."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    return None

def to_time(value: Any) -> Optional[time]:
    """Acepta time | datetime | 'HH:MM' | 'HH:MM:SS[.ffffff]' | None -> time | None."""
    if value is None:
        return None
    if isinstance(value, time):
        return value.replace(microsecond=0)
    if isinstance(value, datetime):
        return value.time().replace(microsecond=0)
    if isinstance(value, str):
        s = value.strip()
        if "." in s:
            s = s.split(".", 1)[0]
        parts = s.split(":")
        try:
            h = int(parts[0]); m = int(parts[1]); ssec = int(parts[2]) if len(parts) > 2 else 0
            return time(h, m, ssec)
        except Exception:
            return None
    return None

def combine_madrid(d_val: Any, t_val: Any) -> Optional[datetime]:
    """Combina fecha+hora interpretando como hora de Madrid. Si falta hora o fecha, devuelve None."""
    d = to_date(d_val)
    t = to_time(t_val)
    if not d or not t:
        return None
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=MAD)

def ensure_date(d: Union[str, date]) -> date:
    """Acepta 'YYYY-MM-DD' o date y devuelve date."""
    if isinstance(d, date):
        return d
    return datetime.strptime(d, "%Y-%m-%d").date()

def _chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def _parse_day_naive(dt_str: str) -> str:
    # 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'
    return dt_str.split(" ", 1)[0]

def pick_nickname(cli: Dict[str, Any]) -> str:
    """
    Devuelve el mejor nickname disponible, con fallback al nombre.
    Prioridad: nickname > nick > alias > apodo > nombre.
    """
    for key in ("nickname", "nick", "alias", "apodo"):
        val = (cli.get(key) or "").strip()
        if val:
            return val
    return (cli.get("nombre") or "").strip()

# ----------------- DB helpers -----------------

def fetch_clientes(client_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not client_ids:
        return {}
    by_id: Dict[int, Dict[str, Any]] = {}
    chunk = 500
    for i in range(0, len(client_ids), chunk):
        ids = client_ids[i:i+chunk]
        r = (supabase.table("clientes")
             .select("*")
             .in_("cliente_id", ids)
             .execute())
        for c in (r.data or []):
            by_id[int(c["cliente_id"])] = c
    return by_id

def fetch_reservas_ida_para_fechas(dias: List[date]) -> List[Dict[str, Any]]:
    """Trae reservas con fecha_ida en las fechas dadas."""
    if not dias:
        return []
    dias_str = [d.isoformat() for d in dias]
    r = (supabase.table("cliente_reservas")
         .select("id_reserva, cliente_id, destino, fecha_ida, hora_vuelo_ida, fecha_reserva, numero_viajeros")
         .in_("fecha_ida", dias_str)
         .execute())
    return r.data or []

def fetch_reservas_vuelta_para_fecha(d: date) -> List[Dict[str, Any]]:
    """Trae reservas cuya fecha_vuelta == d."""
    r = (supabase.table("cliente_reservas")
         .select("id_reserva, cliente_id, destino, fecha_vuelta, fecha_reserva")
         .eq("fecha_vuelta", d.isoformat())
         .execute())
    return r.data or []

def fetch_existing_como_ha_ido_for(reserva_ids: List[str], chunk: int = 100) -> set[str]:
    """
    Devuelve el set de reserva_id que YA tienen flow_type='como_ha_ido'.
    Lee en lotes pequeños (IN de 100).
    """
    if not reserva_ids:
        return set()

    uniq = [str(r) for r in {r for r in reserva_ids if r}]
    existing: set[str] = set()

    for i in range(0, len(uniq), chunk):
        rids = uniq[i:i+chunk]
        resp = (
            supabase.table("opcionesinicio_client")
            .select("reserva_id")
            .eq("flow_type", "como_ha_ido")
            .in_("reserva_id", rids)
            .execute()
        )
        for row in (resp.data or []):
            rid = row.get("reserva_id")
            if rid:
                existing.add(str(rid))

    return existing

def _fetch_existing_reservas_por_tipo_y_dia(flow_type: str, day_str: str, page_size: int = 1000) -> Set[str]:
    """
    Lee reserva_id existentes para un flow_type y día concreto (rango send_datetime [day, next_day)).
    Paginado por 'id' para no sobrecargar.
    """
    start_of_day = f"{day_str} 00:00:00"
    next_day = (datetime.strptime(day_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    start = 0
    existentes: Set[str] = set()

    while True:
        resp = (
            supabase.table("opcionesinicio_client")
            .select("id,reserva_id")
            .eq("flow_type", flow_type)
            .gte("send_datetime", start_of_day)
            .lt("send_datetime", f"{next_day} 00:00:00")
            .order("id", desc=False)
            .range(start, start + page_size - 1)
            .execute()
        )
        rows = resp.data or []
        if not rows:
            break
        for r in rows:
            rid = r.get("reserva_id")
            if rid:
                existentes.add(str(rid))
        if len(rows) < page_size:
            break
        start += page_size

    return existentes

def upsert_reservas(items: List[OPCItem]) -> int:
    """
    Inserta SOLO si no existe ya (flow_type, reserva_id), sin ON CONFLICT.
    Agrupa por (flow_type, día de send_datetime) y filtra contra existentes en ese rango.
    """
    if not items:
        return 0

    grupos: Dict[Tuple[str, str], List[OPCItem]] = {}
    for it in items:
        if not it.reserva_id:
            continue
        day = _parse_day_naive(it.send_datetime)
        grupos.setdefault((it.flow_type, day), []).append(it)

    if not grupos:
        return 0

    to_insert: List[dict] = []
    for (ft, day), group_items in grupos.items():
        existentes = _fetch_existing_reservas_por_tipo_y_dia(ft, day)
        for it in group_items:
            if it.reserva_id not in existentes:
                to_insert.append(asdict(it))

    if not to_insert:
        return 0

    inserted = 0
    for chunk in _chunked(to_insert, 300):
        supabase.table("opcionesinicio_client").insert(chunk, returning="minimal").execute()
        inserted += len(chunk)
    return inserted

# ----------------- Reglas de envío -----------------

def send_dt_como_ha_ido_por_hora_ida(ida_dt_mad: datetime) -> datetime:
    """
    Reglas CÓMO HA IDO:
      - Vuelo <= 10:00 -> ese mismo día a las 15:00
      - 10:00 < vuelo <= 15:00 -> ese mismo día a las 19:00
      - Vuelo > 15:00 -> al día siguiente a las 09:30
    """
    h = ida_dt_mad.time()
    if h <= time(10, 0):
        return ida_dt_mad.replace(hour=15, minute=0, second=0, microsecond=0)
    if h <= time(15, 0):
        return ida_dt_mad.replace(hour=19, minute=0, second=0, microsecond=0)
    nxt = ida_dt_mad.date() + timedelta(days=1)
    return datetime(nxt.year, nxt.month, nxt.day, 9, 30, 0, tzinfo=MAD)

def send_dt_vuestra_aventura_para_dia(target_day: date) -> datetime:
    """Para VUESTRA AVENTURA: siempre target_day a las 10:00 (hora ya usada en tu script)."""
    return datetime(target_day.year, target_day.month, target_day.day, 10, 0, 0, tzinfo=MAD)

# ----------------- Repetidores -----------------

def _to_utc_iso(dt_mad: datetime) -> str:
    return dt_mad.astimezone(timezone.utc).isoformat()

def cliente_tiene_reservas_previas_por_cliente(cliente_id: int, ref_dt_mad: datetime, excluir_id_reserva: str) -> bool:
    """
    ¿El cliente tiene reservas anteriores a ref_dt_mad?
    Usa cliente_reservas.fecha_reserva < ref_dt y excluye la actual.
    """
    antes_iso = _to_utc_iso(ref_dt_mad)
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

def cliente_tiene_reservas_previas_por_telefono(telefono: str, ref_dt_mad: datetime, excluir_id_reserva: str) -> bool:
    """
    Variante por teléfono (útil si hay duplicidad de cliente_id).
    """
    if not telefono:
        return False
    cl = (supabase.table("clientes").select("cliente_id").eq("telefono", telefono).execute())
    ids = [int(x["cliente_id"]) for x in (cl.data or []) if "cliente_id" in x]
    if not ids:
        return False
    antes_iso = _to_utc_iso(ref_dt_mad)
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

# ----------------- BUILDERS -----------------

def build_item_como_ha_ido(row: Dict[str, Any], cliente: Dict[str, Any], send_dt_mad: datetime, flow_id: str) -> OPCItem:
    nickname = pick_nickname(cliente)
    idioma = (cliente.get("idioma") or cliente.get("lang") or "es")
    if idioma == 'pt':
        idioma = 'pt_pt'
    id_reserva = str(row["id_reserva"])
    destino = row.get("destino")
    json_vars = json.dumps({"nombre": nickname, "ciudad": destino}, ensure_ascii=False)
    return OPCItem(
        flow_id=flow_id,
        flow_type="como_ha_ido",
        reserva_id=id_reserva,
        cliente_id=int(row["cliente_id"]),
        json_variables=json_vars,
        send_datetime=iso_naive(send_dt_mad),
        is_send=False,
        is_canceled=False,
        idioma=idioma,
        lang=idioma,
    )

def build_item_vuestra_aventura(row: Dict[str, Any], cliente: Dict[str, Any], send_dt_mad: datetime, flow_id: str) -> OPCItem:
    nickname = pick_nickname(cliente)
    idioma = (cliente.get("idioma") or cliente.get("lang") or "es")
    if idioma == 'pt':
        idioma = 'pt_pt'
    id_reserva = str(row["id_reserva"])
    json_vars = json.dumps({"nombre": nickname}, ensure_ascii=False)
    return OPCItem(
        flow_id=flow_id,
        flow_type="vuestra_aventura",
        reserva_id=id_reserva,
        cliente_id=int(row["cliente_id"]),
        json_variables=json_vars,
        send_datetime=iso_naive(send_dt_mad),
        is_send=False,
        is_canceled=False,
        idioma=idioma,
        lang=idioma,
    )

# ----------------- PLANIFICADORES -----------------

def plan_como_ha_ido_para_dia(target_day: Union[str, date]) -> int:
    """
    Genera TODOS los 'como_ha_ido' cuyo envío cae EXACTAMENTE en 'target_day' (Madrid).
      - Considera idas en 'target_day' (15:00 o 19:00 del mismo día según hora).
      - Considera idas en 'target_day - 1' con hora > 15:00 (envío: target_day a las 09:30).
    Selección de plantilla:
      - Si el envío es mismo día: normal vs repetidor.
      - Si el envío es al día siguiente por hora tardía: usar como_ha_ido_vuelo_hotel_2.
    """
    target_day = ensure_date(target_day)
    ayer = target_day - timedelta(days=1)

    rows = fetch_reservas_ida_para_fechas([ayer, target_day])
    if not rows:
        print(f"[como_ha_ido] No hay idas en {ayer} / {target_day}.")
        return 0

    cliente_ids = list({int(r["cliente_id"]) for r in rows})
    cache_clientes = fetch_clientes(cliente_ids)

    items: List[OPCItem] = []
    for r in rows:
        ida_dt = combine_madrid(r.get("fecha_ida"), r.get("hora_vuelo_ida"))
        if ida_dt is None:
            continue
        send_dt = send_dt_como_ha_ido_por_hora_ida(ida_dt)
        if send_dt.date() != target_day:
            # Este envío no cae en el día objetivo
            continue

        cl = cache_clientes.get(int(r["cliente_id"]), {})
        rid = str(r["id_reserva"])

        # ¿El envío es al día siguiente por vuelo tarde?
        envio_next_day = (send_dt.date() != ida_dt.date())
        if envio_next_day:
            flow_id = COMO_HA_IDO_NEXTDAY
        else:
            # Mismo día → decidir por repetidor (prioridad por cliente; si quieres por teléfono, descomenta abajo)
            es_repetidor = cliente_tiene_reservas_previas_por_cliente(int(r["cliente_id"]), ida_dt, excluir_id_reserva=rid)
            # # Alternativa por teléfono:
            # telefono = (cl.get("telefono") or cl.get("phone") or "").strip()
            # es_repetidor = cliente_tiene_reservas_previas_por_telefono(telefono, ida_dt, excluir_id_reserva=rid)
            flow_id = COMO_HA_IDO_REPETIDOR if es_repetidor else COMO_HA_IDO_NORMAL

        if r.get("numero_viajeros") == 1 and cl.get("idioma") == 'es':
            flow_id = flow_id + "_singular"

        items.append(build_item_como_ha_ido(r, cl, send_dt, flow_id))

    if not items:
        print(f"[como_ha_ido] No hay envíos para {target_day}.")
        return 0

    inserted = upsert_reservas(items)
    print(f"[como_ha_ido] day={target_day} candidatos={len(items)} insertados={inserted}")
    return inserted

def plan_vuestra_aventura_para_dia(target_day: Union[str, date], require_como_ha_ido: bool = True) -> int:
    """
    Genera TODOS los 'vuestra_aventura' cuyo envío es 'target_day' a las 10:00 (Madrid):
      - Considera vueltas en 'target_day - 1'.
      - Envía en 'target_day' 10:00.
      - Si 'require_como_ha_ido' es True, SOLO crea si ya existe 'como_ha_ido' para esa id_reserva.
    Selección de plantilla:
      - Repetidor → vuetra_aventura_2
      - Normal → vuestra_aventura
    """
    target_day = ensure_date(target_day)
    ayer = target_day - timedelta(days=1)

    # 1) Traer vueltas del día anterior
    rows = fetch_reservas_vuelta_para_fecha(ayer)
    if not rows:
        print(f"[vuestra_aventura] No hay vueltas en {ayer}.")
        return 0

    # 2) Cache de clientes
    cliente_ids = list({int(r["cliente_id"]) for r in rows})
    cache_clientes = fetch_clientes(cliente_ids)

    # 3) Hora de envío fija 10:00 del día objetivo
    send_dt = send_dt_vuestra_aventura_para_dia(target_day)

    # 4) Precargar en bloque las reservas que YA tienen 'como_ha_ido'
    existing_como: set[str] = set()
    if require_como_ha_ido:
        all_rids = [str(r["id_reserva"]) for r in rows]
        existing_como = fetch_existing_como_ha_ido_for(all_rids)

    # 5) Construir items filtrando por existencia (si se requiere)
    items: List[OPCItem] = []
    for r in rows:
        rid = str(r["id_reserva"])
        if require_como_ha_ido and rid not in existing_como:
            continue
        cl = cache_clientes.get(int(r["cliente_id"]), {})

        # Repetidor por cliente respecto al momento de envío (o usa ayer/fecha_reserva si prefieres)
        es_repetidor = cliente_tiene_reservas_previas_por_cliente(int(r["cliente_id"]), send_dt, excluir_id_reserva=rid)
        # # Alternativa por teléfono:
        # telefono = (cl.get("telefono") or cl.get("phone") or "").strip()
        # es_repetidor = cliente_tiene_reservas_previas_por_telefono(telefono, send_dt, excluir_id_reserva=rid)

        flow_id = VUESTRA_AVENTURA_REPETIDOR if es_repetidor else VUESTRA_AVENTURA_NORMAL
        if r.get("numero_viajeros") == 1 and cl.get("idioma") == 'es':
            flow_id = flow_id + "_singular"
        items.append(build_item_vuestra_aventura(r, cl, send_dt, flow_id))

    if not items:
        print(f"[vuestra_aventura] No hay envíos para {target_day} (tras filtro como_ha_ido={require_como_ha_ido}).")
        return 0

    inserted = upsert_reservas(items)
    print(f"[vuestra_aventura] day={target_day} candidatos={len(items)} insertados={inserted}")
    return inserted

# ----------------- CLI -----------------

def main():
    p = argparse.ArgumentParser(description="Planifica 'como_ha_ido' y 'vuestra_aventura' para un día objetivo (Madrid).")
    p.add_argument("--day", help="Día objetivo YYYY-MM-DD (Madrid). Si se omite, usa hoy Madrid.")
    p.add_argument("--skip-aventura", action="store_true", help="No planificar 'vuestra_aventura'.")
    p.add_argument("--skip-como", action="store_true", help="No planificar 'como_ha_ido'.")
    p.add_argument("--no-require-como", action="store_true", help="Para 'vuestra_aventura', no exigir que exista 'como_ha_ido'.")
    args = p.parse_args()

    target_day = ensure_date(args.day) if args.day else today_madrid()

    if not args.skip_como:
        plan_como_ha_ido_para_dia(target_day)

    if not args.skip_aventura:
        plan_vuestra_aventura_para_dia(target_day, require_como_ha_ido=not args.no_require_como)

if __name__ == "__main__":
    # Ejemplos:
    # main()
    target_day = "2025-11-10"
    plan_como_ha_ido_para_dia(target_day)
    plan_vuestra_aventura_para_dia(target_day, require_como_ha_ido=True)
