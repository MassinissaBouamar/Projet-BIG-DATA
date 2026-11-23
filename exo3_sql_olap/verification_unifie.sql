-- Exercice 2 - controles non destructifs du pipeline unifie
-- Attendu pour janvier 2026 : 3 724 888 lignes apres nettoyage.
SELECT source_file, COUNT(*) AS nb_courses
FROM dw.fact_trip
GROUP BY source_file
ORDER BY source_file;

SELECT quality_status, COUNT(*) AS nb_courses
FROM dw.fact_trip
WHERE source_file = 'yellow_tripdata_2026-01.parquet'
GROUP BY quality_status
ORDER BY quality_status;

SELECT
  COUNT(*) AS nb_janvier,
  MIN(pickup_datetime) AS premier_depart,
  MAX(pickup_datetime) AS dernier_depart,
  COUNT(*) FILTER (WHERE trip_duration_seconds < 0) AS durees_negatives
FROM dw.fact_trip
WHERE source_file = 'yellow_tripdata_2026-01.parquet';

SELECT
  v.vendor_name,
  COUNT(*) AS nb_courses,
  ROUND(SUM(f.total_amount), 2) AS montant_total
FROM dw.fact_trip f
JOIN dw.dim_vendor v ON f.vendor_id = v.vendor_id
WHERE f.source_file = 'yellow_tripdata_2026-01.parquet'
GROUP BY v.vendor_name
ORDER BY nb_courses DESC;
