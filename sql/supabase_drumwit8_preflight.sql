-- Diagnóstico de sólo lectura; ejecutar con un usuario que pueda ver el esquema.
-- Para regalos, fecha_ida/fecha_vuelta deben devolver is_nullable = YES.
SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = 'public'
  AND (
    (table_name = 'cliente_reservas' AND column_name IN (
        'cliente_id', 'id_reserva', 'fecha_reserva', 'fecha_ida', 'fecha_vuelta',
        'regalo', 'id_reserva_compradora_regalo', 'numero_viajeros'
    ))
    OR (table_name = 'clientes' AND column_name IN ('cliente_id', 'telefono'))
    OR (table_name = 'opcionesinicio_client' AND column_name = 'reserva_id')
  )
ORDER BY table_name, ordinal_position;

-- Revisar UNIQUE(cliente_id, id_reserva) y UNIQUE(telefono), sin tocar índices.
SELECT tablename, indexname, indexdef
FROM pg_catalog.pg_indexes
WHERE schemaname = 'public'
  AND tablename IN ('cliente_reservas', 'clientes')
ORDER BY tablename, indexname;

-- Sólo conteos: no exponer teléfonos, nombres ni datos de mensajes.
SELECT count(*) AS total,
       count(*) FILTER (WHERE regalo::text IN ('1', 'true', 't')) AS regalos,
       count(*) FILTER (WHERE regalo::text IN ('1', 'true', 't') AND (fecha_ida IS NOT NULL OR fecha_vuelta IS NOT NULL))
           AS regalos_con_fechas,
       count(*) FILTER (WHERE id_reserva IS NULL OR btrim(id_reserva::text) = '')
           AS sin_identificador
FROM public.cliente_reservas;
