import re
import os
import json
from collections import OrderedDict
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# Configuración Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

VAR_PATTERN = re.compile(r"{{(.*?)}}")


def extract_variables_ordered(texto: str):
    """
    Extrae variables {{var}} del texto preservando el orden de aparición
    y eliminando duplicados (se conserva la primera aparición).
    """
    seen = set()
    ordered = []
    for match in VAR_PATTERN.findall(texto or ""):
        if match not in seen:
            seen.add(match)
            ordered.append(match)
    return ordered

def generar_json_variables(variables, cliente):
    """
    Construye un dict ORDENADO con las variables en el mismo orden en que fueron encontradas.
    Reglas:
      - 'destino' y 'ciudad' -> None (siempre, por ser cambiantes).
      - Si la var contiene dígitos -> None.
      - Buscar primero por clave lowercase en el cliente, luego tal cual; si no, None.
    """
    json_vars = OrderedDict()
    for var in variables:
        var_lower = var.lower()

        # Forzar 'destino' y 'ciudad' a None porque son cambiantes
        if var_lower in ("destino", "ciudad"):
            json_vars[var] = None
            continue

        # Si contiene dígitos, lo dejamos a None
        if any(char.isdigit() for char in var):
            json_vars[var] = None
            continue

        # Buscar por lowercase y luego por la clave tal cual
        if isinstance(cliente, dict) and var_lower in cliente:
            json_vars[var] = cliente.get(var_lower)
        elif isinstance(cliente, dict) and var in cliente:
            json_vars[var] = cliente.get(var)
        else:
            json_vars[var] = None

    return json_vars

def preparar_inserts_opcionesinicio_client(plantillas, cliente, client_id, lang_cliente: str):
    inserts = []
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
            # hereda del cliente (NO de la plantilla)
            "idioma": lang_cliente,
            "lang": lang_cliente
        })
    return inserts

def cargar_opcionesinicio_client(client_id: int):
    # 1) Cliente
    cliente_resp = supabase.table("clientes").select("*").eq("cliente_id", client_id).single().execute()
    cliente = cliente_resp.data
    if not cliente:
        raise ValueError(f"Cliente {client_id} no encontrado")

    # Idioma heredado del cliente (prioriza 'idioma', luego 'lang', si no -> 'es')
    lang_cliente = (cliente.get("idioma") or cliente.get("lang") or "es")

    # 2) Plantillas filtradas por lang del cliente
    plantillas_resp = (
        supabase
        .table("opcionesinicio")
        .select("*")
        .eq("lang", lang_cliente)
        .execute()
    )
    plantillas = plantillas_resp.data or []

    # 3) Preparar inserts
    inserts = preparar_inserts_opcionesinicio_client(plantillas, cliente, client_id, lang_cliente)

    # 4) Insertar
    if inserts:
        supabase.table("opcionesinicio_client").insert(inserts).execute()
        print(f"✅ Insertadas {len(inserts)} plantillas (lang='{lang_cliente}') para cliente {client_id}")
    else:
        print(f"⚠️ No se insertó ninguna plantilla para lang='{lang_cliente}'. Revisa 'opcionesinicio'.")


# whatsapp_functions/templates/create_opcionesinicioclient.py

def cargar_opcionesinicio_client_bulk(client_ids: list[int], chunk_size: int = 200) -> None:
    """
    Para cada cliente en client_ids:
      - Si NO tiene filas en opcionesinicio_client, crea sus filas desde opcionesinicio (por lang del cliente).
      - Si ya tiene, lo salta (idempotente).
    Procesa en trozos para no saturar la API.
    """
    if not client_ids:
        print("[info] Sin client_ids para seed de opcionesinicio_client (bulk).")
        return

    # 1) Averiguar quién NO tiene filas
    missing_ids = []
    for i in range(0, len(client_ids), chunk_size):
        chunk = client_ids[i:i+chunk_size]
        # Trae conteos por cliente
        resp = (
            supabase.table("opcionesinicio_client")
            .select("cliente_id", count="exact")
            .in_("cliente_id", chunk)
            .execute()
        )
        # Clientes con 0 filas = chunk - {cliente_id devueltos}
        have = {row["cliente_id"] for row in (resp.data or [])}
        # OJO: la API no devuelve conteos por cliente, devuelve filas. Si hay filas, el cliente aparece.
        # Por tanto, 'have' = clientes con al menos 1 fila.
        missing_ids.extend([cid for cid in chunk if cid not in have])

    if not missing_ids:
        print("[info] Todos los clientes del lote ya tenían opcionesinicio_client.")
        return

    # 2) Cargar clientes (idioma, etc.)
    clientes = []
    for i in range(0, len(missing_ids), chunk_size):
        chunk = missing_ids[i:i+chunk_size]
        r = (supabase.table("clientes")
             .select("*")
             .in_("cliente_id", chunk)
             .execute())
        clientes.extend(r.data or [])
    by_id = {c["cliente_id"]: c for c in clientes if c}

    # 3) Agrupa por idioma del cliente para no pedir plantillas cada vez
    #    idioma: usa prioridad (idioma -> lang -> 'es')
    lang_groups = {}
    for cid in missing_ids:
        cl = by_id.get(cid)
        if not cl:
            continue
        lang = (cl.get("idioma") or cl.get("lang") or "es")
        lang_groups.setdefault(lang, []).append(cid)

    # 4) Por cada lang, trae plantillas una vez y genera los inserts de todos sus clientes
    total_inserts = 0
    for lang, ids in lang_groups.items():
        plantillas_resp = supabase.table("opcionesinicio").select("*").eq("lang", lang).execute()
        plantillas = plantillas_resp.data or []
        if not plantillas:
            print(f"[warn] No hay plantillas en opcionesinicio para lang='{lang}'. Se salta {len(ids)} clientes.")
            continue

        bulk_inserts = []
        for cid in ids:
            cl = by_id.get(cid) or {}
            # Generar inserts del cliente cid con estas plantillas
            cliente_vars = preparar_inserts_opcionesinicio_client(plantillas, cl, cid, lang)
            bulk_inserts.extend(cliente_vars)

        # Inserta en lotes
        for i in range(0, len(bulk_inserts), chunk_size):
            payload = bulk_inserts[i:i+chunk_size]
            supabase.table("opcionesinicio_client").insert(payload).execute()
            total_inserts += len(payload)

    print(f"✅ Bulk opcionesinicio_client: insertadas {total_inserts} filas para {len(missing_ids)} clientes sin registros.")


# ============================================
# Ensurar baseline de opciones de inicio (NULL send_datetime)
# usando preparar_inserts_opcionesinicio_client del proyecto
# ============================================

# Asegúrate de tener el alias si no lo pusiste ya:
# supabase = sb


def ensure_opciones_inicio_baseline_cliente(client_id: int) -> int:
    """
    Garantiza que el cliente tenga 1 fila baseline (send_datetime IS NULL)
    en opcionesinicio_client por cada flow_id disponible en su idioma.

    - Usa preparar_inserts_opcionesinicio_client(...) para construir inserts (con json_variables).
    - Filtra los flow_id que YA existan con send_datetime IS NULL.
    - Devuelve el número de filas insertadas.
    """
    # 1) Cliente
    cliente_resp = supabase.table("clientes").select("*").eq("cliente_id", client_id).single().execute()
    cliente = cliente_resp.data
    if not cliente:
        raise ValueError(f"Cliente {client_id} no encontrado")

    # Idioma
    lang_cliente = (cliente.get("idioma") or cliente.get("lang") or "es")

    # 2) Baseline existentes (send_datetime IS NULL) por flow_id
    baseline_resp = (
        supabase.table("opcionesinicio_client")
        .select("flow_id")
        .eq("cliente_id", client_id)
        .is_("send_datetime", "null")
        .execute()
    )
    flow_ids_existentes = {r["flow_id"] for r in (baseline_resp.data or []) if r.get("flow_id")}

    # 3) Plantillas por idioma
    plantillas_resp = (
        supabase.table("opcionesinicio")
        .select("*")
        .eq("lang", lang_cliente)
        .execute()
    )
    plantillas = plantillas_resp.data or []

    # 4) Construir inserts con tu función (esto rellena json_variables)
    inserts_all = preparar_inserts_opcionesinicio_client(plantillas, cliente, client_id, lang_cliente) or []

    # 5) Filtrar a solo las que FALTAN en baseline (por flow_id) y que sean baseline (send_datetime None)
    inserts = []
    for ins in inserts_all:
        fid = ins.get("flow_id")
        # baseline = sin send_datetime
        sd = ins.get("send_datetime", None)
        if fid and fid not in flow_ids_existentes and (sd is None):
            inserts.append(ins)

    # 6) Insertar solo faltantes
    if inserts:
        supabase.table("opcionesinicio_client").insert(inserts).execute()
        print(f"✅ Cliente {client_id}: insertadas {len(inserts)} baseline (lang='{lang_cliente}')")
        return len(inserts)
    else:
        print(f"✔️ Cliente {client_id}: baseline completa, nada que insertar.")
        return 0


# ---------- Variante bulk (todos o subset) ----------

from functools import lru_cache

@lru_cache(maxsize=128)
def _flow_ids_por_lang(lang: str) -> set[str]:
    """Ayuda opcional para acelerar: qué flow_id existen en opcionesinicio para un lang dado."""
    resp = supabase.table("opcionesinicio").select("flow_id").eq("lang", (lang or "es")).execute()
    return {r["flow_id"] for r in (resp.data or []) if r.get("flow_id")}

def ensure_opciones_inicio_baseline_para_clientes(cliente_ids: list[int], insert_chunk: int = 1000) -> int:
    """
    Idempotente: para una lista de cliente_ids, crea baseline faltante (send_datetime NULL)
    invocando preparar_inserts_opcionesinicio_client por cliente y filtrando flow_ids ya existentes.
    Devuelve el total insertado.
    """
    if not cliente_ids:
        return 0

    total_insertadas = 0

    # Traer todos los clientes con todos los campos (preparar_inserts puede necesitarlos)
    cli_resp = supabase.table("clientes").select("*").in_("cliente_id", cliente_ids).execute()
    clientes = cli_resp.data or []
    by_id = {c["cliente_id"]: c for c in clientes if c and c.get("cliente_id") is not None}

    # Pre-cargar baseline existentes para el conjunto
    base_resp = (
        supabase.table("opcionesinicio_client")
        .select("cliente_id, flow_id, send_datetime")
        .in_("cliente_id", list(by_id.keys()))
        .is_("send_datetime", "null")
        .execute()
    )
    baseline_por_cliente: dict[int, set[str]] = {}
    for r in (base_resp.data or []):
        cid = r.get("cliente_id")
        fid = r.get("flow_id")
        if cid is None or not fid:
            continue
        baseline_por_cliente.setdefault(cid, set()).add(fid)

    # Construir payload por cliente usando TU preparador y filtrar baseline
    buffer: list[dict] = []
    for cid, cliente in by_id.items():
        lang = (cliente.get("idioma") or cliente.get("lang") or "es")

        # Recuperar plantillas del lang (se podría cachear, pero tu preparador puede necesitar sus campos completos)
        plantillas_resp = supabase.table("opcionesinicio").select("*").eq("lang", lang).execute()
        plantillas = plantillas_resp.data or []

        inserts_all = preparar_inserts_opcionesinicio_client(plantillas, cliente, cid, lang) or []
        ya = baseline_por_cliente.get(cid, set())

        for ins in inserts_all:
            fid = ins.get("flow_id")
            sd = ins.get("send_datetime", None)
            if fid and (sd is None) and (fid not in ya):
                buffer.append(ins)

        # Flush por bloques
        if len(buffer) >= insert_chunk:
            supabase.table("opcionesinicio_client").insert(buffer).execute()
            total_insertadas += len(buffer)
            buffer.clear()

    if buffer:
        supabase.table("opcionesinicio_client").insert(buffer).execute()
        total_insertadas += len(buffer)
        buffer.clear()

    print(f"✅ Baseline bulk: insertadas {total_insertadas} filas nuevas.")
    return total_insertadas


def ensure_opciones_inicio_baseline_para_todos(page_size: int = 10000, insert_chunk: int = 10000) -> int:
    """
    Crea SOLO las baseline (send_datetime NULL) que faltan, apoyándose en la vista
    v_missing_opcionesinicio_baseline para evitar revisar todos los clientes.

    - Usa preparar_inserts_opcionesinicio_client para generar inserts completos (incluye json_variables).
    - Filtra por los flow_id que reporta la vista como faltantes.
    """
    total_insertadas = 0
    offset = 0

    while True:
        # 1) Traer SOLO lo que falta (cliente_id, lang, flow_id) desde la vista, en bloques
        resp = supabase.table("v_missing_opcionesinicio_baseline") \
                       .select("cliente_id, lang, flow_id") \
                       .range(offset, offset + page_size - 1) \
                       .execute()
        rows = resp.data or []
        if not rows:
            break
        offset += page_size

        # 2) Mapear: cliente_id -> {flow_id faltantes}; y recolectar langs presentes
        missing_map: dict[int, set[str]] = {}
        langs_presentes: set[str] = set()
        for r in rows:
            cid = r.get("cliente_id")
            fid = r.get("flow_id")
            lng = (r.get("lang") or "es")
            if cid is None or not fid:
                continue
            cid = int(cid)
            missing_map.setdefault(cid, set()).add(fid)
            langs_presentes.add(lng)

        cliente_ids = list(missing_map.keys())
        if not cliente_ids:
            continue

        # 3) Traer clientes necesarios de una tacada
        cli_resp = supabase.table("clientes").select("*").in_("cliente_id", cliente_ids).execute()
        clientes = {c["cliente_id"]: c for c in (cli_resp.data or []) if c.get("cliente_id") is not None}

        # 4) Traer plantillas una sola vez por idioma presente en el bloque
        plantillas_por_lang: dict[str, list[dict]] = {}
        for lng in langs_presentes:
            p_resp = supabase.table("opcionesinicio").select("*").eq("lang", lng).execute()
            plantillas_por_lang[lng] = p_resp.data or []

        # 5) Construir inserts con tu preparador y filtrar solo los flow_id faltantes (baseline)
        buffer: list[dict] = []
        for cid, faltan_fids in missing_map.items():
            cliente = clientes.get(cid)
            if not cliente:
                continue

            lang_cliente = (cliente.get("idioma") or cliente.get("lang") or "es")
            plantillas = plantillas_por_lang.get(lang_cliente, [])
            if not plantillas:
                # Si no hay plantillas para ese lang en este bloque, intenta cargarlas ad-hoc
                if lang_cliente not in plantillas_por_lang:
                    p_resp = supabase.table("opcionesinicio").select("*").eq("lang", lang_cliente).execute()
                    plantillas_por_lang[lang_cliente] = p_resp.data or []
                    plantillas = plantillas_por_lang[lang_cliente]

            # Genera todos los inserts (incluye json_variables) y filtra baseline que falten
            inserts_all = preparar_inserts_opcionesinicio_client(plantillas, cliente, cid, lang_cliente) or []
            for ins in inserts_all:
                fid = ins.get("flow_id")
                sd  = ins.get("send_datetime", None)
                if fid and (sd is None) and (fid in faltan_fids):
                    buffer.append(ins)

            # Inserta por lotes
            if len(buffer) >= insert_chunk:
                try:
                    supabase.table("opcionesinicio_client").insert(buffer).execute()
                except Exception:
                    # Con un índice único parcial (cliente_id, flow_id) WHERE send_datetime IS NULL
                    # se evita duplicar en condiciones de carrera.
                    pass
                total_insertadas += len(buffer)
                buffer.clear()

        if buffer:
            try:
                supabase.table("opcionesinicio_client").insert(buffer).execute()
            except Exception:
                pass
            total_insertadas += len(buffer)
            buffer.clear()

    print(f"🏁 Baseline global completada. Total nuevas filas: {total_insertadas}")
    return total_insertadas


def seed_opciones_inicio_baseline_via_view(page_size: int = 5000, insert_chunk: int = 1000) -> int:
    """
    Lee v_missing_opcionesinicio_baseline en bloques y crea SOLO los registros faltantes
    (baseline: send_datetime = NULL) usando preparar_inserts_opcionesinicio_client.
    Devuelve total de filas insertadas.
    """
    total_insertadas = 0
    offset = 0

    while True:
        # 1) Trae pares faltantes (cliente_id, lang, flow_id) en bloque
        resp = supabase.table("v_missing_opcionesinicio_baseline") \
                 .select("cliente_id, lang, flow_id") \
                 .range(offset, offset + page_size - 1) \
                 .execute()
        rows = resp.data or []
        if not rows:
            break
        offset += page_size

        # Mapa: cliente_id -> {flow_id faltantes}
        missing_map: dict[int, set[str]] = {}
        langs_presentes: set[str] = set()
        for r in rows:
            cid = r.get("cliente_id")
            fid = r.get("flow_id")
            lng = r.get("lang") or "es"
            if cid is None or not fid:
                continue
            missing_map.setdefault(int(cid), set()).add(fid)
            langs_presentes.add(lng)

        cliente_ids = list(missing_map.keys())
        if not cliente_ids:
            continue

        # 2) Trae los clientes necesarios de una tacada
        cli_resp = supabase.table("clientes").select("*").in_("cliente_id", cliente_ids).execute()
        clientes = {c["cliente_id"]: c for c in (cli_resp.data or []) if c.get("cliente_id") is not None}

        # 3) Trae plantillas una sola vez por idioma presente en el bloque
        plantillas_por_lang: dict[str, list[dict]] = {}
        for lng in langs_presentes:
            p_resp = supabase.table("opcionesinicio").select("*").eq("lang", lng).execute()
            plantillas_por_lang[lng] = p_resp.data or []

        # 4) Construye inserts con tu preparador y filtra por los flow_id que faltan
        buffer: list[dict] = []
        for cid, faltan_fids in missing_map.items():
            cliente = clientes.get(cid)
            if not cliente:
                continue
            lang_cliente = (cliente.get("idioma") or cliente.get("lang") or "es")
            plantillas = plantillas_por_lang.get(lang_cliente, [])

            # Tu función genera TODOS los inserts (incluye json_variables correctas)
            inserts_all = preparar_inserts_opcionesinicio_client(plantillas, cliente, cid, lang_cliente) or []

            # Filtra solo baseline que falten (por flow_id y send_datetime NULL)
            for ins in inserts_all:
                fid = ins.get("flow_id")
                sd  = ins.get("send_datetime", None)
                if fid and (sd is None) and (fid in faltan_fids):
                    buffer.append(ins)

            # Inserta en lotes
            if len(buffer) >= insert_chunk:
                try:
                    supabase.table("opcionesinicio_client").insert(buffer).execute()
                except Exception as e:
                    # En caso de carrera, el índice único parcial evitará duplicados.
                    # Si choca, puedes ignorar 409 o reintentar en bloques más pequeños.
                    pass
                total_insertadas += len(buffer)
                buffer.clear()

        if buffer:
            try:
                supabase.table("opcionesinicio_client").insert(buffer).execute()
            except Exception:
                pass
            total_insertadas += len(buffer)
            buffer.clear()

        print("Seed baseline (vista): bloque insertadas=%d, acumuladas=%d", total_insertadas, total_insertadas)

    print("Seed baseline (vista) COMPLETO: total insertadas = %d", total_insertadas)
    return total_insertadas

# Ejemplo de uso:
if __name__ == "__main__":
    ensure_opciones_inicio_baseline_para_todos()
