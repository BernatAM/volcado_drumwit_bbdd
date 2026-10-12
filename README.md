# ETL Drumwit8

Este servicio vuelca contactos, reservas y regalos desde MySQL a Supabase y conserva los endpoints y las columnas que utilizan los planificadores de Nubita. La adaptación sigue el documento del cliente «Nubita - Cambios en la base de datos», migración de drumwit7 a drumwit8.

## Cambios aplicados

- `volcado_clientes.py` lee `booking` y `gift` con sus tablas de contacto. Los viajeros se obtienen de `traveler`, cuya FK es `booking_id`; un regalo nunca hereda los viajeros de una reserva con el mismo ID numérico. Se conserva la selección del primer adulto y se usa el contacto si no existe un adulto. Los compradores de regalos se obtienen de `gift_contact_details`.
- Los teléfonos concatenan el país y el número del contacto. El idioma no añade otro prefijo. Los contactos sin número o sin código de país no generan teléfonos compuestos sólo por un prefijo.
- `volcado_reservas.py` importa reservas y regalos `PAID` con `UNION ALL`. `id_reserva` contiene `booking.code` o `gift.code`, también para identificar las filas durante el enriquecimiento. Las reservas calculan la vuelta con `departure_date + trip_length_in_nights`; los regalos tienen fechas de viaje nulas. El número de viajeros se obtiene de `traveler` o de los contadores del regalo.
- Sólo las reservas se enriquecen con `booking_flights` y `destination.name_es`. Los canjes conservan `booking.gift_id` en `id_reserva_compradora_regalo`: ese campo mantiene el contrato existente y sirve para seleccionar el flujo de canje, no como identificador global.
- El incremental de reservas utiliza `updated_at` o `created_at` de cada tabla. El incremental de clientes conserva la ventana por `created_at` del último día indicada en el PDF. El nocturno carga el histórico. Los CLI respetan `ETL_MODE`.
- La actualización de clientes existentes continúa limitada a teléfono, nombre, nickname e idioma; conserva sus flags operativos.

`gift_card_details` y `accommodation` no aparecen en consultas de este servicio, por lo que no necesitan una consulta nueva. Los backends de WhatsApp e IA siguen consumiendo las tablas de Supabase, con el mismo contrato.

## Conexión y despliegue

Configuración local actualizada el 12/10/2026:

| Archivo | Entorno | Host | Puerto | Base |
| --- | --- | --- | --- | --- |
| `.env` | Configuración del servicio preparada para PROD Drumwit8 | `213.192.239.166` | `3306` | `dR5mW3t_8_Pr4d_BD` |
| `.env.prod` | Conexión PROD para validación de sólo lectura | `213.192.239.166` | `3306` | `dR5mW3t_8_Pr4d_BD` |
| `.env.legacy` | Referencia Drumwit7 de sólo lectura | `www.drumwit.com` | `3306` | `dR5Mwit_Pr4dBdV3` |
| `.env.test` | TEST | `213.192.239.166` | `3306` | `dr5MWT2st_BD` |

El cliente ha facilitado la conexión real de PROD con `usrNubitaPROD_8`. `.env` y `.env.prod` están actualizados; las variables de Supabase de `.env` se conservan. La autenticación funciona, el modelo es Drumwit8 y las columnas requeridas están presentes. PROD todavía tiene cero filas en reservas, regalos, viajeros, contactos, vuelos y destinos; el usuario ha confirmado que **la carga de datos está pendiente**. No se han reiniciado procesos, ejecutado ETL de escritura ni activado mensajes. Esta preparación de configuración no acredita una puesta en producción.

La conexión TEST funciona con la contraseña propia de `usrNubitaTEST`. `.env.legacy` conserva la conexión original y sus credenciales; la base `dR5Mwit_Pr4dBdV3` contiene el modelo Drumwit7. `.env.test`, `.env.legacy` y `.env.prod` contienen sólo MySQL, están excluidos de Git y se destinan a lectores que fuerzan transacciones de sólo lectura. Las configuraciones de WhatsApp e IA siguen apuntando a su destino actual. Las variables definidas en el entorno de un proceso pueden prevalecer sobre `.env`; revisar también la configuración del servicio al desplegar.

### Actualización del código en EC2

Este cambio corresponde únicamente a `volcado_drumwit_bbdd`. Los endpoints que llama Airflow se mantienen; no requiere actualizar los `.env` de WhatsApp ni IA. El `.env` del servicio deja de estar versionado: **guardar una copia antes del pull**, porque Git retirará el archivo que seguía en el repositorio. Las credenciales locales de este equipo no se transfieren con Git.

En EC2, desde la carpeta del servicio y con los DAGs pausados y los trabajos en curso terminados, ejecutar los pasos en orden. Si alguno falla, detenerse antes del siguiente:

```bash
cp -p .env ../volcado_drumwit_bbdd.env.pre-drumwit8
test -s ../volcado_drumwit_bbdd.env.pre-drumwit8
# La copia anterior conserva la configuración de EC2; restaurar sólo el archivo versionado para permitir el pull.
git restore -- .env
git pull --ff-only origin main
cp -p ../volcado_drumwit_bbdd.env.pre-drumwit8 .env
```

Actualizar en ese `.env` únicamente la conexión MySQL con las credenciales facilitadas por el cliente: host `213.192.239.166`, puerto `3306`, base `dR5mW3t_8_Pr4d_BD`, usuario `usrNubitaPROD_8` y su contraseña. Conservar Supabase y las demás variables del servidor. Si el servicio define `MYSQL_*` fuera del archivo, actualizar también esas variables. Reiniciar el proceso del ETL para recargar la conexión; el arranque incremental usa `ETL_MODE=incremental` e `INCREMENTAL_HOURS=24`.

La carga de PROD debe estar terminada antes de ejecutar el ETL. Validar primero clientes y después reservas en modo incremental, comprobar las escrituras y repetir el lote para comprobar que no duplica filas. Reactivar los DAGs y los envíos cuando esa comprobación sea correcta. El código se puede descargar antes de que termine la carga, manteniendo la ingesta pausada.

La configuración original se conserva localmente en `.local-audit/env-backup-20261012/.env`, y la configuración inmediatamente anterior a PROD8 en `.env-before-prod8-214410` dentro de esa misma carpeta. El código anterior a los ajustes de validación está en `.local-audit/code-before-validation-20261012/`. El ETL original Drumwit7 se identifica por el commit inmutable `31ebe1de772f0f0bc341c2ab310dfe93ae0410a1`, registrado en `tools/legacy_baseline.json`. Estas copias no sustituyen un backup de las bases. Los archivos privados y las copias locales se excluyen de Git; compartir únicamente plantillas sin credenciales.

TEST supera el preflight real del esquema: columnas necesarias presentes, tablas requeridas InnoDB y 13 controles de calidad sin incidencias. Evidencias en `reports/migration-20261012/test-preflight.json`, `test-source-counts.json` y `legacy-access.json`; los intentos de acceso anteriores son evidencia histórica ya superada. TEST termina el 10/10/2026, por lo que el ensayo legacy/TEST excluye las altas del 11 y 12. La comprobación posterior de PROD está en `prod8-preflight.json` y `prod8-preflight.html` dentro de la misma carpeta: estado **AWAITING_PRODUCTION_DATA**. Los controles de integridad a cero sobre tablas vacías no prueban la calidad de la carga. Se repetirá la comparación de salidas cuando PROD contenga los datos del cliente.

## Extracción de prueba reproducible

Desde la carpeta del servicio, con las dependencias de `requirements.txt` instaladas:

```powershell
python -B tools/validate_migration.py --legacy-env .env.legacy --test-env .env.test --source-only --as-of 2026-10-11T00:00:00 --max-rows 400000 --output reports/migration-20261012/legacy-vs-test
python -B tools/audit_supabase.py --env .env --output reports/migration-20261012/supabase-history.json
```

En este equipo se puede sustituir `python` por `C:/Repository/Drumwit/.tmp/drumwit8-tests/Scripts/python.exe`. Para repetir el ensayo con el corte exacto del clon, añadir `--as-of "YYYY-MM-DDTHH:MM:SS"` con la fecha real acordada, sin offset y en la zona horaria del servidor. `--hours` controla la ventana incremental de reservas (24 por defecto); clientes conserva su ventana del día anterior. Un corte de altas no revierte cambios posteriores en filas existentes: para una comparación definitiva hace falta un clon sincronizado o detener las escrituras durante el corte.

`validate_migration.py` usa snapshots de sólo lectura de MySQL, carga las definiciones de ambos ETL sin sus conexiones ni SDK de escritura y captura localmente sus payloads completos e incrementales. Detecta el modelo de la referencia legacy y utiliza el commit original para Drumwit7. `--source-only` limita el ensayo a las dos bases MySQL: el histórico actual de Supabase, que ya incluye altas de los días 11 y 12, queda fuera. `--prod-env` se mantiene como alias antiguo de `--legacy-env`; no consulta una tercera base.

Se comparan clientes por teléfono, reservas/vuelos por código y la clasificación de confirmaciones, regalos y canjes. La comparación conserva duplicados, distingue NULL de vacío/cero, cuenta diferencias por campo y genera huellas SHA-256. Las únicas equivalencias de forma son fechas y compradora opcionales ausentes frente a NULL; el payload estricto también se informa. Se verifican columnas, códigos globales, contactos, referencias huérfanas, tipos de viajero, fechas, filas omitidas e índices necesarios. La comparación entre orígenes simula un mapa común de `cliente_id`; después, `--supabase-env` (por defecto `.env`) contrasta esos teléfonos y códigos con los enlaces reales de Supabase, sin escribir, y señala clientes cambiados, claves ambiguas y referencias ausentes. Las altas legítimas y el histórico cancelado o archivado requieren interpretación de negocio.

Cada ejecución genera `migration-report.json` y `migration-report.html`. No exporta nombres, teléfonos ni claves. Límite por extracción: 200.000 filas, ampliable con `--max-rows`; superar el límite falla y nunca se acepta una muestra como histórico completo. Códigos de salida:

| Código | Estado | Interpretación |
| --- | --- | --- |
| 0 | `SOURCE_PARITY_VERIFIED` | Paridad de las salidas comparadas y controles de origen superados; aún no acredita un despliegue |
| 2 | `DIFFERENCES_FOUND` | Hay diferencias, duplicados, datos omitidos o invariantes incumplidas que revisar |
| 3 | `INCOMPLETE` | No se pudo terminar; nunca equivale a un resultado correcto |

La auditoría de Supabase lee páginas completas, verifica los conteos y busca teléfonos normalizados duplicados, claves de conflicto repetidas y referencias de cola ausentes. Sólo acredita las filas visibles con la clave actual; REST no permite un snapshot común y no prueba la existencia de restricciones UNIQUE. Las referencias ausentes pueden ser histórico archivado o flujos manuales: se revisan antes de decidir cualquier corrección.

## Resultado real: legacy frente a TEST, 12/10/2026

El informe vigente es `reports/migration-20261012/migration-report.html`, con detalle reproducible en el JSON del mismo nombre. Estado **DIFFERENCES_FOUND** por las diferencias históricas. El incremental al corte común `2026-10-10T13:32:24` tiene paridad lógica: 43 clientes, 27 reservas y su clasificación; ninguno de los dos lotes tiene vuelos incrementales. El payload estricto de 13 reservas distingue campos opcionales ausentes en legacy frente a NULL en TEST (fecha de vuelta y compradora del regalo). El histórico completo todavía no es equivalente:

| Extracción completa al corte común | LEGACY | TEST |
| --- | ---: | ---: |
| Clientes | 97.556 | 97.558 |
| Reservas | 104.522 | 124.723 |
| Vuelos | 92.105 | 92.102 |

Los 104.498 códigos comunes conservan su clasificación. TEST incluye 19.676 regalos que el filtro de fechas cero de legacy excluía y 536 códigos de regalo históricos sin correspondencia. Legacy devuelve 24 códigos ausentes del payload TEST al corte común: 22 bookings con código ausente de TEST, uno que pasa a PENDING y un gift con problema de teléfono. En códigos comunes cambian 1.052 teléfonos/enlaces, 4.778 regalos pasan de tener viajeros a cero y dos fechas de vuelta difieren. Otros 99 recuentos de bookings cambian porque el antiguo `num_viajeros` discrepa del número de pasajeros vinculados: TEST coincide con las relaciones de legacy en los 99 casos. No se han corregido fuentes ni deduplicado viajeros para forzar igualdad.

El ensayo hasta el final del día 10 está conservado en `legacy-vs-test/`; el ensayo al corte común está en `legacy-vs-test-clone-cut/`, dentro de esa carpeta de informes. El corte común se deduce de la última fecha observada en TEST, no certifica el instante real del clon. Las 37 altas de legacy posteriores a esa fecha dentro del día 10, de las que 27 llegan al payload, se distinguen del resto de diferencias. No se incluyen Supabase ni la futura PROD, ni altas de los días 11 o 12.

Los diagnósticos agregados están en `legacy-vs-test/difference-diagnosis.json`, `field-diagnosis.json` y `traveler-diagnosis.json`. Los casos concretos con código, sin teléfonos ni nombres, se conservan fuera de Git en `.local-audit/legacy-test-review-cases-20261012.json` y `.local-audit/traveler-review-cases-20261012.json`. Los snapshots privados sí contienen datos de cliente y permanecen excluidos de Git. El usuario acepta las diferencias históricas conocidas y prioriza la continuidad del incremental; esas diferencias no bloquean la subida del código. Los informes conservan sus resultados para revisar los efectos del nocturno sobre el histórico.

## Comprobaciones de producción

1. Confirmar que la carga de PROD ha terminado y que la conexión desde EC2 devuelve datos del modelo nuevo. La conexión y el esquema ya se comprobaron desde este equipo; falta validar los datos de la carga definitiva. El incremental debe recoger reservas y regalos creados o actualizados dentro de la ventana, conservar códigos y clasificación y no producir duplicados al repetirlo. Las diferencias históricas aceptadas se mantienen visibles en los informes.
2. El responsable de Supabase ejecuta `sql/supabase_drumwit8_preflight.sql`: deben existir los campos usados por los consumidores, NULL permitido en ambas fechas y UNIQUE sobre `(cliente_id, id_reserva)` y `clientes.telefono`. Las lecturas actuales ya encontraron NULL en ambas fechas; falta confirmar los índices con acceso al catálogo. `sql/mysql_drumwit8_preflight.sql` también permite revisar el esquema y los conteos desde el cliente SQL del responsable.
3. Reconciliar `booking.code`/`gift.code` con `cliente_reservas.id_reserva` y `opcionesinicio_client.reserva_id`, además de teléfono → `cliente_id`. El origen anterior utilizaba `localizador` o un ID numérico. Una clave diferente puede crear reservas y confirmaciones duplicadas. Revisar también las fechas antiguas de regalos: el nuevo payload envía NULL y puede limpiar valores anteriores. Mantener la semántica de `booking.gift_id` para canjes. Cualquier mapeo debe ser validado antes de cargar el histórico; no borrar ni reprogramar automáticamente.
4. Conservar backups verificables del origen anterior y de `clientes`, `cliente_reservas` y la cola en Supabase, junto al código desplegado y sus variables. El responsable debe confirmar cómo restaurarlos y ensayar la restauración en una copia aislada. Nosotros somos externos: las copias y cambios de infraestructura los realiza su responsable.
5. Ensayar la escritura en una copia de Supabase de TEST independiente, con env y credenciales propios: clientes primero y reservas después. Repetir el mismo lote debe conservar claves y conteos sin duplicar filas; revisar flags operativos de clientes y enriquecimiento final de vuelos. Mantener desactivados los envíos reales durante el ensayo. `.env.test` actual permite únicamente el ensayo de lectura.

## Secuencia de paso a producción y recuperación

Detener la ingesta y los planificadores, comprobar que no queda un ETL en curso y esperar a la carga definitiva del cliente. Aplicar la actualización de EC2 descrita arriba. La validación prioritaria es el primer incremental contra la nueva PROD; conservar sus conteos y resultados. El ensayo legacy/TEST ya aporta paridad lógica de ese modo al corte común, pero no acredita los datos pendientes de PROD.

Instalar `requirements.txt` en el entorno del servicio, aplicar código y variables como una misma versión y reiniciar los procesos para que recarguen la conexión. Ejecutar clientes antes de reservas, comprobar los resultados en Supabase y repetir el lote para acreditar idempotencia. Mantener la cola y los envíos detenidos hasta validar identificadores, clasificación y número de mensajes previstos. Reactivar los procesos de forma controlada y observar al menos un ciclo incremental y el siguiente nocturno: errores, duración, filas omitidas, duplicados y crecimiento de la cola deben coincidir con lo validado.

Si falla un control, detener ETL y planificadores. Restaurar conjuntamente la versión anterior del código y sus variables; usar la conexión antigua sólo si el responsable confirma que esa base sigue disponible y coherente. Si hubo escrituras, revertir únicamente los lotes identificados o restaurar el backup aprobado de Supabase; una reversión de `.env` no revierte datos. Verificar de nuevo claves y cola antes de reanudar. Los mensajes ya enviados no se pueden deshacer, por lo que los envíos se habilitan al final.

Las rutas `POST /api/v1/etl/clientes` y `POST /api/v1/etl/reservas` escriben datos reales. Las rutas de autoschedule crean mensajes: no son pruebas en seco.

## Pruebas sin conexiones externas

```powershell
python -m pip install -r requirements.txt
python -m pip install pytest
python -B -m pytest -p no:cacheprovider tests -q
```

Las pruebas bloquean las conexiones de red y no leen el `.env` real. Ejecutan las consultas sobre SQLite con fixtures del modelo nuevo; únicamente adaptan las expresiones de fechas de MySQL. Comprueban reservas y regalos con el mismo ID, viajeros ADULT/CHILD/INFANT, contactos sin viajeros, teléfonos internacionales, datos nulos, pago, ventanas incrementales, enriquecimiento y payloads de Supabase. También comprueban duplicados, diferencias sin datos personales, rechazo de escrituras, paginación incompleta y que un fallo de conexión nunca produzca éxito. Esta batería no sustituye una comprobación del dialecto, esquema, permisos y datos reales.
