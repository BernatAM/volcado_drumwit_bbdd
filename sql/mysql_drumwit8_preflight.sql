-- Diagnóstico de sólo lectura en la nueva base ya seleccionada.
SELECT DATABASE() AS base_seleccionada;

SELECT table_name, column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_schema = DATABASE()
  AND table_name IN (
    'booking', 'gift', 'booking_contact_details', 'gift_contact_details',
    'traveler', 'booking_flights', 'destination'
  )
ORDER BY table_name, ordinal_position;

-- Debe devolver cero filas: el ETL usa code como identificador global.
SELECT code, COUNT(*) AS apariciones
FROM (
    SELECT code FROM booking
    UNION ALL
    SELECT code FROM gift
) codes
GROUP BY code
HAVING code IS NULL OR TRIM(code) = '' OR COUNT(*) > 1;

-- Comprobar el volumen esperado de reservas/regalos que se importarán.
SELECT 'booking' AS origen, status, COUNT(*) AS registros
FROM booking GROUP BY status
UNION ALL
SELECT 'gift' AS origen, status, COUNT(*) AS registros
FROM gift GROUP BY status;
