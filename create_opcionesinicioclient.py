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


# Ejemplo de uso:
if __name__ == "__main__":
    cargar_opcionesinicio_client(client_id=5)
