-- Comptages apres creation + insertion + import_zones.
SELECT 'vendors' AS table_name, COUNT(*) AS rows_count FROM dw.dim_vendor
UNION ALL SELECT 'payments', COUNT(*) FROM dw.dim_payment
UNION ALL SELECT 'ratecodes', COUNT(*) FROM dw.dim_ratecode
UNION ALL SELECT 'boroughs', COUNT(*) FROM dw.dim_borough
UNION ALL SELECT 'service_zones', COUNT(*) FROM dw.dim_service_zone
UNION ALL SELECT 'locations', COUNT(*) FROM dw.dim_location
UNION ALL SELECT 'dates', COUNT(*) FROM dw.dim_date
UNION ALL SELECT 'hours', COUNT(*) FROM dw.dim_hour
UNION ALL SELECT 'trips', COUNT(*) FROM dw.fact_trip
ORDER BY table_name;

-- Verifier la relation en flocon (zone -> borough + service zone).
SELECT l.location_id, l.zone_name, b.borough_name, s.service_zone_name
FROM dw.dim_location l
JOIN dw.dim_borough b ON b.borough_id = l.borough_id
JOIN dw.dim_service_zone s ON s.service_zone_id = l.service_zone_id
ORDER BY l.location_id
LIMIT 12;

-- Pour le moment 0 courses : c'est normal, la branche Spark -> JDBC
-- sera implementee ensuite dans l'exercice 2, branche 2.
