# Analisis de afectacion por cambios en BBDD

Fecha de revision: 2026-05-26

Este documento conserva el análisis previo del modelo Drumwit7. La implementación Drumwit8, la validación incremental y los pasos actuales de despliegue están en `README.md`; las consultas siguientes son una referencia histórica.

## Resumen ejecutivo

El proyecto usa dos fuentes de datos:

- MySQL origen, leido desde `MYSQL_*` en `.env`.
- Supabase/PostgREST destino, leido y escrito desde `SUPABASE_*` en `.env`.

La parte mas sensible a cambios de esquema MySQL esta en:

- `volcado_clientes.py`
- `volcado_reservas.py`
- `bbdd_conection.py`

La parte mas sensible a cambios de esquema Supabase esta en:

- `volcado_clientes.py`
- `volcado_reservas.py`
- `create_opcionesinicioclient.py`
- `autoschedule_confirmados.py`
- `autoschedule_viajes.py`

## Conexiones localizadas

| Archivo | Tipo | Conexion | Uso |
|---|---|---|---|
| `bbdd_conection.py` | MySQL | SQLAlchemy `create_engine("mysql+pymysql")` | Helper generico `run_query(query)` |
| `volcado_reservas.py` | MySQL | `pymysql.connect(...)` | Queries directas con cursor |
| `volcado_clientes.py` | Supabase | `create_client(SUPABASE_URL, SUPABASE_KEY)` | Lee/inserta/actualiza `clientes` |
| `volcado_reservas.py` | Supabase | `create_client(SUPABASE_URL, SUPABASE_KEY)` | Lee `clientes`, upsert `cliente_reservas` |
| `create_opcionesinicioclient.py` | Supabase | `create_client(SUPABASE_URL, SUPABASE_KEY)` | Lee `clientes`, `opcionesinicio`, vista y escribe `opcionesinicio_client` |
| `autoschedule_confirmados.py` | Supabase | `create_client(SUPABASE_URL, SUPABASE_KEY)` | Lee reservas/clientes y escribe `opcionesinicio_client` |
| `autoschedule_viajes.py` | Supabase | `create_client(SUPABASE_URL, SUPABASE_KEY)` | Lee reservas/clientes y escribe `opcionesinicio_client` |
| `volcado_clientes.py` | Supabase REST | `requests.post({SUPABASE_URL}/rest/v1/{table})` | Upsert bulk alternativo sobre `clientes` |

## Queries localizadas tal cual estan

### MySQL - `volcado_clientes.py`

`extract_reservas(run_query, full=True|False)`:

```sql
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
```

Cuando `full=False`, se concatena este filtro:

```sql
WHERE r.fecha_reserva > DATE_SUB(CURDATE(), INTERVAL 1 DAY)
```

`extract_pasajero_reserva(run_query)`:

```sql
SELECT
    id_reserva AS reserva_id,
    id_pasajero AS pasajero_id
FROM pasajeros__reserva
```

`extract_pasajeros(run_query)`:

```sql
SELECT
    id AS pasajero_id,
    nombre,
    apellido1 AS apellido1,
    COALESCE(apellido2, '') AS apellido2,
    tipo_pasajero AS tipo
FROM pasajero
```

### MySQL - `volcado_reservas.py`

`SQL_RESERVA_BASE`:

```sql
SELECT
    r.id AS reserva_id,
    COALESCE(NULLIF(r.localizador,''), CAST(r.id AS CHAR)) AS id_reserva,
    r.fecha_reserva,
    r.fecha_salida,
    r.fecha_llegada,
    -- Telefono efectivo: principal o comprador_regalo si el principal esta vacio
    COALESCE(NULLIF(r.telefono, ''), NULLIF(r.telefono_comprador_regalo, '')) AS telefono_eff,
    -- Nombre efectivo (por si luego lo necesitas)
    COALESCE(NULLIF(r.nombre_regalo, ''), NULLIF(r.nombre_comprador_regalo, ''), NULLIF(r.nickname, '')) AS nombre_eff,
    r.idioma,
    r.regalo,
    r.id_reserva_compradora_regalo,
    r.num_viajeros as numero_viajeros
FROM reserva r
WHERE
r.estado > 0 AND {where_clause}
```

`SQL_BOOKING_ENRICH`:

```sql
SELECT
    r.id AS reserva_id,
    COALESCE(NULLIF(r.localizador,''), CAST(r.id AS CHAR)) AS id_reserva,
    r.fecha_reserva,   -- blindaje NOT NULL en enriquecimiento
    r.fecha_salida,
    r.fecha_llegada,
    COALESCE(NULLIF(r.telefono, ''), NULLIF(r.telefono_comprador_regalo, '')) AS telefono_eff,
    COALESCE(NULLIF(r.nombre_regalo, ''), NULLIF(r.nombre_comprador_regalo, ''), NULLIF(r.nickname, '')) AS nombre_eff,
    r.idioma,
    r.regalo,
    r.id_reserva_compradora_regalo,
    bf.departure_time AS hora_ida,
    bf.return_time    AS hora_vuelta,
    d.nombre_es       AS destino_nombre
FROM reserva r
JOIN booking_flights bf ON bf.booking_id = r.id
LEFT JOIN destino d ON d.id = bf.destination_id
WHERE
r.estado > 0 AND {where_clause}
```

`where_clause` se construye asi:

```python
conds = [
    "(r.fecha_salida IS NULL OR r.fecha_salida <> '0000-00-00')",
    "(r.fecha_llegada IS NULL OR r.fecha_llegada <> '0000-00-00')",
    "(r.fecha_llegada IS NULL OR r.fecha_llegada >= '1970-01-02')",
]

if mode == "incremental":
    conds.insert(
        0,
        f"(r.ultima_modificacion >= NOW() - INTERVAL {hours} HOUR "
        f"OR r.fecha_reserva >= NOW() - INTERVAL {hours} HOUR)"
    )

return " AND ".join(conds)
```

Ejecucion:

```python
cur.execute(SQL_RESERVA_BASE.format(where_clause=where))
cur.execute(SQL_BOOKING_ENRICH.format(where_clause=where))
```

### MySQL - `bbdd_conection.py`

Conexion:

```python
url = URL.create(
    "mysql+pymysql",
    username=os.getenv("MYSQL_USER"),
    password=os.getenv("MYSQL_PASSWORD"),
    host=os.getenv("MYSQL_HOST"),
    port=int(os.getenv("MYSQL_PORT")),
    database=os.getenv("MYSQL_DATABASE"),
)
return create_engine(url, pool_pre_ping=True)
```

Ejecucion generica de SQL:

```python
eng = engine or get_engine()
sql = text(query)

if chunksize:
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params, chunksize=chunksize)

with eng.connect() as conn:
    if as_dataframe:
        return pd.read_sql(sql, conn, params=params)
    else:
        result = conn.execute(sql, params or {})
        cols = result.keys()
        return [dict(zip(cols, row)) for row in result.fetchall()]
```

Query de ejemplo en el `__main__`:

```sql
SELECT *
FROM pasajero
limit 100
```

### Supabase/PostgREST - `volcado_clientes.py`

Lectura de telefonos existentes:

```python
q = sb.table("clientes").select("cliente_id,telefono").order("cliente_id", desc=False).limit(page_size)
if last_id is not None:
    q = q.gt("cliente_id", last_id)
resp = q.execute()
```

Insert de clientes nuevos:

```python
sb.table("clientes").insert(payload).execute()
```

Upsert HTTP alternativo sobre `clientes`:

```python
url = f"{SUPABASE_URL}/rest/v1/{table_name}"
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates,return=minimal"
}
params = {"on_conflict": on_conflict}
resp = requests.post(url, params=params, headers=headers, json=rows, timeout=120)
```

Llamadas reales al upsert HTTP:

```python
_http_upsert_bulk("clientes", payload, on_conflict="telefono")
_http_upsert_bulk("clientes", payload, on_conflict="telefono")
```

### Supabase/PostgREST - `volcado_reservas.py`

Lectura de clientes para mapa telefono -> cliente_id:

```python
resp = supabase.table("clientes") \
    .select("cliente_id,telefono") \
    .gte("cliente_id", last_id + 1) \
    .order("cliente_id", desc=False) \
    .limit(page_size) \
    .execute()
```

Upsert de reservas:

```python
_ = supabase.table("cliente_reservas") \
    .upsert(batch, on_conflict="cliente_id,id_reserva") \
    .execute()
```

### Supabase/PostgREST - `create_opcionesinicioclient.py`

Cliente por `cliente_id`:

```python
cliente_resp = supabase.table("clientes").select("*").eq("cliente_id", client_id).single().execute()
```

Plantillas por idioma:

```python
plantillas_resp = (
    supabase
    .table("opcionesinicio")
    .select("*")
    .eq("lang", lang_cliente)
    .execute()
)
```

Insert de opciones de inicio:

```python
supabase.table("opcionesinicio_client").insert(inserts).execute()
```

Comprobacion de clientes con opciones:

```python
resp = (
    supabase.table("opcionesinicio_client")
    .select("cliente_id", count="exact")
    .in_("cliente_id", chunk)
    .execute()
)
```

Carga de clientes por lote:

```python
r = (supabase.table("clientes")
     .select("*")
     .in_("cliente_id", chunk)
     .execute())
```

Plantillas por `lang`:

```python
plantillas_resp = supabase.table("opcionesinicio").select("*").eq("lang", lang).execute()
```

Insert bulk de `opcionesinicio_client`:

```python
supabase.table("opcionesinicio_client").insert(payload).execute()
```

Baseline existente por cliente:

```python
baseline_resp = (
    supabase.table("opcionesinicio_client")
    .select("flow_id")
    .eq("cliente_id", client_id)
    .is_("send_datetime", "null")
    .execute()
)
```

Flow ids por idioma:

```python
resp = supabase.table("opcionesinicio").select("flow_id").eq("lang", (lang or "es")).execute()
```

Clientes por lista de ids:

```python
cli_resp = supabase.table("clientes").select("*").in_("cliente_id", cliente_ids).execute()
```

Baseline existente por conjunto de clientes:

```python
base_resp = (
    supabase.table("opcionesinicio_client")
    .select("cliente_id, flow_id, send_datetime")
    .in_("cliente_id", list(by_id.keys()))
    .is_("send_datetime", "null")
    .execute()
)
```

Vista de baseline faltante:

```python
resp = supabase.table("v_missing_opcionesinicio_baseline") \
               .select("cliente_id, lang, flow_id") \
               .range(offset, offset + page_size - 1) \
               .execute()
```

### Supabase/PostgREST - `autoschedule_confirmados.py`

Reservas desde una fecha:

```python
r = (
    supabase.table("cliente_reservas")
    .select("id_reserva, cliente_id, fecha_reserva, regalo, id_reserva_compradora_regalo, numero_viajeros")
    .gte("fecha_reserva", since_iso)
    .order("fecha_reserva", desc=False)
    .execute()
)
```

Cliente por id:

```python
r = supabase.table("clientes").select("*").eq("cliente_id", cliente_id).single().execute()
```

Reservas previas por cliente:

```python
r = (
    supabase.table("cliente_reservas")
    .select("id_reserva")
    .eq("cliente_id", cliente_id)
    .lt("fecha_reserva", antes_iso)
    .neq("id_reserva", excluir_id_reserva)
    .limit(1)
    .execute()
)
```

Cliente por telefono:

```python
cl = (supabase.table("clientes").select("cliente_id").eq("telefono", telefono).execute())
```

Reservas previas por telefono:

```python
r = (
    supabase.table("cliente_reservas")
    .select("id_reserva, cliente_id")
    .in_("cliente_id", ids)
    .lt("fecha_reserva", antes_iso)
    .neq("id_reserva", excluir_id_reserva)
    .limit(1)
    .execute()
)
```

Reserva ids existentes en `opcionesinicio_client`:

```python
resp = (
    supabase.table("opcionesinicio_client")
    .select("id,reserva_id")
    .order("id", desc=False)
    .range(start, start + page_size - 1)
    .execute()
)
```

Insert individual:

```python
supabase.table("opcionesinicio_client").insert(payload, returning="minimal").execute()
```

### Supabase/PostgREST - `autoschedule_viajes.py`

Clientes por ids:

```python
r = (supabase.table("clientes")
     .select("*")
     .in_("cliente_id", ids)
     .execute())
```

Reservas por fecha de ida:

```python
r = (supabase.table("cliente_reservas")
     .select("id_reserva, cliente_id, destino, fecha_ida, hora_vuelo_ida, fecha_reserva, numero_viajeros")
     .in_("fecha_ida", dias_str)
     .execute())
```

Reservas por fecha de vuelta:

```python
r = (supabase.table("cliente_reservas")
     .select("id_reserva, cliente_id, destino, fecha_vuelta, fecha_reserva, numero_viajeros")
     .eq("fecha_vuelta", d.isoformat())
     .execute())
```

Existencia de `como_ha_ido`:

```python
resp = (
    supabase.table("opcionesinicio_client")
    .select("reserva_id")
    .eq("flow_type", "como_ha_ido")
    .in_("reserva_id", rids)
    .execute()
)
```

Existentes por tipo y dia:

```python
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
```

Insert de mensajes:

```python
supabase.table("opcionesinicio_client").insert(chunk, returning="minimal").execute()
```

Reservas previas por cliente:

```python
r = (
    supabase.table("cliente_reservas")
    .select("id_reserva")
    .eq("cliente_id", cliente_id)
    .lt("fecha_reserva", antes_iso)
    .neq("id_reserva", excluir_id_reserva)
    .limit(1)
    .execute()
)
```

Cliente por telefono:

```python
cl = (supabase.table("clientes").select("cliente_id").eq("telefono", telefono).execute())
```

Reservas previas por telefono:

```python
r = (
    supabase.table("cliente_reservas")
    .select("id_reserva, cliente_id")
    .in_("cliente_id", ids)
    .lt("fecha_reserva", antes_iso)
    .neq("id_reserva", excluir_id_reserva)
    .limit(1)
    .execute()
)
```

## MySQL origen: tablas y columnas usadas

### `reserva`

Usada en `volcado_clientes.py` y `volcado_reservas.py`.

Columnas usadas:

- `id`
- `localizador`
- `fecha_reserva`
- `fecha_salida`
- `fecha_llegada`
- `ultima_modificacion`
- `nickname`
- `telefono`
- `telefono_comprador_regalo`
- `nombre_regalo`
- `nombre_comprador_regalo`
- `apellido1_comprador_regalo`
- `apellido2_comprador_regalo`
- `idioma`
- `regalo`
- `id_reserva_compradora_regalo`
- `num_viajeros`
- `estado`

Riesgo si cambia:

- Alto. Alimenta clientes, reservas y los autoschedules posteriores.
- Si cambian nombres o tipos de fechas, fallaran las queries o las normalizaciones.
- Si cambia `localizador`, afecta a `id_reserva` en Supabase.
- Si cambia `telefono` o campos de regalo, puede dejar reservas sin `cliente_id`.

### `pasajeros__reserva`

Usada en `volcado_clientes.py`.

Columnas usadas:

- `id_reserva`
- `id_pasajero`

Riesgo si cambia:

- Medio/alto. Se usa para construir nombre de cliente desde pasajeros.
- Si cambia la tabla relacional, el ETL de clientes puede generar nombres incompletos o fallar.

### `pasajero`

Usada en `volcado_clientes.py`.

Columnas usadas:

- `id`
- `nombre`
- `apellido1`
- `apellido2`
- `tipo_pasajero`

Riesgo si cambia:

- Medio. Afecta a `nombre` y `nickname` de `clientes`.

### `booking_flights`

Usada en `volcado_reservas.py`.

Columnas usadas:

- `booking_id`
- `destination_id`
- `departure_time`
- `return_time`

Riesgo si cambia:

- Alto para enriquecimiento de reservas.
- Si cambia, `cliente_reservas` puede quedar con `destino = "Pendiente"` o sin horas de vuelo.

### `destino`

Usada en `volcado_reservas.py`.

Columnas usadas:

- `id`
- `nombre_es`

Riesgo si cambia:

- Medio. Afecta al destino guardado en Supabase y a mensajes con ciudad/destino.

## Supabase destino: tablas/vistas y columnas usadas

### `clientes`

Leida/escrita en varios modulos.

Columnas leidas:

- `cliente_id`
- `telefono`
- `nombre`
- `nickname`
- `idioma`
- `lang`
- `*` en varios puntos, porque se usa para rellenar variables dinamicas de plantillas.

Columnas escritas/actualizadas desde `volcado_clientes.py`:

- `nombre`
- `telefono`
- `created_at`
- `destino`
- `status_drumwit`
- `status_reserva`
- `is_user_required`
- `is_ai_mode`
- `idioma`
- `status_boarding_passes`
- `nickname`

Restricciones esperadas:

- `clientes.telefono` debe ser unico para el upsert HTTP.
- `cliente_id` se usa para paginar y relacionar.

Riesgo si cambia:

- Alto. Es tabla central para resolver `cliente_id` desde telefono.
- Si se elimina o cambia `telefono`, no se podran vincular reservas.
- Si se eliminan columnas usadas en insert, fallaran los inserts de clientes.
- Si se elimina `idioma`/`lang`, se generaran plantillas con idioma por defecto `es`.

### `cliente_reservas`

Leida/escrita en `volcado_reservas.py`, `autoschedule_confirmados.py`, `autoschedule_viajes.py`.

Columnas leidas:

- `id_reserva`
- `cliente_id`
- `fecha_reserva`
- `fecha_ida`
- `hora_vuelo_ida`
- `fecha_vuelta`
- `destino`
- `regalo`
- `id_reserva_compradora_regalo`
- `numero_viajeros`

Columnas escritas/upsert:

- `cliente_id`
- `id_reserva`
- `destino`
- `regalo`
- `numero_viajeros`
- `fecha_ida`
- `fecha_vuelta`
- `fecha_reserva`
- `id_reserva_compradora_regalo`
- Campos de enriquecimiento derivados de vuelos, segun `reservas_enrichment_rows`.

Restricciones esperadas:

- Upsert con `on_conflict="cliente_id,id_reserva"`.

Riesgo si cambia:

- Alto. Es la base para los autoschedules.
- Si cambia `fecha_reserva`, no se generan confirmados correctamente.
- Si cambia `fecha_ida`/`fecha_vuelta`, fallan `como_ha_ido` y `vuestra_aventura`.
- Si cambia la restriccion unica `cliente_id,id_reserva`, el upsert puede duplicar o fallar.

### `opcionesinicio`

Leida en `create_opcionesinicioclient.py`.

Columnas leidas:

- `*`
- `flow_id`
- `texto`
- `lang`

Riesgo si cambia:

- Medio/alto. Define plantillas y variables para generar `opcionesinicio_client`.
- Si cambia `texto` o `flow_id`, no se crean baseline ni mensajes.

### `opcionesinicio_client`

Leida/escrita en `create_opcionesinicioclient.py`, `autoschedule_confirmados.py`, `autoschedule_viajes.py`.

Columnas leidas:

- `id`
- `cliente_id`
- `flow_id`
- `flow_type`
- `reserva_id`
- `send_datetime`

Columnas insertadas:

- `flow_id`
- `flow_type`
- `cliente_id`
- `json_variables`
- `send_datetime`
- `is_send`
- `is_canceled`
- `idioma`
- `lang`
- `reserva_id`

Restricciones esperadas:

- Hay comentarios que asumen indice unico parcial `(cliente_id, flow_id) WHERE send_datetime IS NULL`.
- `autoschedule_confirmados.py` captura duplicados `23505`.
- `autoschedule_viajes.py` evita duplicados consultando por `(flow_type, reserva_id, dia)`.

Riesgo si cambia:

- Alto. Es la cola/salida de mensajes programados.
- Si cambian `flow_type`, `reserva_id` o `send_datetime`, las reglas anti-duplicado dejan de funcionar.
- Si cambia `json_variables`, puede romper el payload de plantillas.

### `v_missing_opcionesinicio_baseline`

Vista leida en `create_opcionesinicioclient.py`.

Columnas leidas:

- `cliente_id`
- `lang`
- `flow_id`

Riesgo si cambia:

- Medio. Afecta solo a procesos de seed/baseline global.

## Endpoints que disparan procesos con BBDD

| Endpoint | Funcion | Afectacion |
|---|---|---|
| `POST /api/v1/etl` | ETL general antiguo | Ejecuta ETL clientes incremental/nocturno |
| `POST /api/v1/etl/clientes` | `clientes_incremental` / `clientes_nocturno` | MySQL `reserva`, `pasajeros__reserva`, `pasajero`; Supabase `clientes` |
| `POST /api/v1/etl/reservas` | `reservas_incremental` / `reservas_nocturno` | MySQL `reserva`, `booking_flights`, `destino`; Supabase `clientes`, `cliente_reservas` |
| `POST /api/v1/autoschedule/confirmados` | `run_incremental_confirmados` | Supabase `cliente_reservas`, `clientes`, `opcionesinicio_client` |
| `POST /api/v1/autoschedule/como_ha_ido` | `plan_como_ha_ido_para_dia` | Supabase `cliente_reservas`, `clientes`, `opcionesinicio_client` |
| `POST /api/v1/autoschedule/vuestra_aventura` | `plan_vuestra_aventura_para_dia` | Supabase `cliente_reservas`, `clientes`, `opcionesinicio_client` |

## Puntos concretos a revisar cuando se sepan los cambios

1. Si cambian tablas/columnas MySQL:
   - Revisar `extract_reservas`, `extract_pasajero_reserva`, `extract_pasajeros` en `volcado_clientes.py`.
   - Revisar `SQL_RESERVA_BASE`, `SQL_BOOKING_ENRICH` y `build_where` en `volcado_reservas.py`.

2. Si cambian tipos de fecha:
   - Revisar `clean_date_mysql`, `clean_datetime_mysql`, `_safe_set_fecha_vuelta` en `volcado_reservas.py`.
   - Revisar filtros `DATE_SUB(CURDATE(), INTERVAL 1 DAY)` y `NOW() - INTERVAL ... HOUR`.

3. Si cambian identificadores de reserva:
   - Revisar generacion de `id_reserva` desde `localizador/id`.
   - Revisar `on_conflict="cliente_id,id_reserva"` en `cliente_reservas`.
   - Revisar anti-duplicados por `reserva_id` en `opcionesinicio_client`.

4. Si cambian telefonos/clientes:
   - Revisar normalizacion de telefono en `volcado_clientes.py` y `volcado_reservas.py`.
   - Revisar `clientes.telefono` como clave unica.
   - Revisar carga de mapa `telefono -> cliente_id`.

5. Si cambian plantillas o mensajes:
   - Revisar `opcionesinicio`, `opcionesinicio_client`, `flow_id`, `flow_type`, `json_variables`.

## Riesgos adicionales detectados

- Hay consultas `select("*")` en Supabase. Son comodas pero fragiles si se eliminan columnas usadas por variables dinamicas.
- Existen dos mecanismos distintos de conexion MySQL: SQLAlchemy en `bbdd_conection.py` y PyMySQL directo en `volcado_reservas.py`.
- `create_autoschedule.py` parece ser una funcion pura/incompleta con dependencias inyectadas; no abre BBDD directamente, pero genera payloads para `opcionesinicio_client`.
- Los secretos estan en `.env`; no conviene compartir este archivo ni pegar sus valores en tickets.

## Prioridad de validacion tras cambios

1. Ejecutar ETL clientes incremental contra entorno de prueba.
2. Ejecutar ETL reservas incremental contra entorno de prueba.
3. Verificar que se crean/actualizan filas en `clientes` y `cliente_reservas`.
4. Ejecutar autoschedule confirmados.
5. Ejecutar autoschedule `como_ha_ido` y `vuestra_aventura` para un dia controlado.
6. Comparar conteos antes/despues y revisar duplicados en `opcionesinicio_client`.
