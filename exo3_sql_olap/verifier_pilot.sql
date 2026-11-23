-- Controle du chargement pilote de 100 courses.
SELECT COUNT(*) AS total_fact_trip FROM dw.fact_trip;

SELECT source_file, COUNT(*) AS nb_courses
FROM dw.fact_trip
GROUP BY source_file
ORDER BY source_file;

SELECT f.trip_id, f.pickup_datetime, v.vendor_name,
       p.payment_label, l.zone_name AS depart,
       f.trip_distance, f.total_amount, f.quality_status
FROM dw.fact_trip AS f
JOIN dw.dim_vendor AS v ON v.vendor_id = f.vendor_id
JOIN dw.dim_payment AS p ON p.payment_type = f.payment_type
JOIN dw.dim_location AS l ON l.location_id = f.pickup_location_id
WHERE f.source_file = 'yellow_tripdata_2026-01.parquet#pilot100'
ORDER BY f.trip_id
LIMIT 10;
