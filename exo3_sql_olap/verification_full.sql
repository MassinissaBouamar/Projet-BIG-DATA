-- Les verifications sont non destructives, sauf le DELETE final optionnel.
SELECT source_file, COUNT(*) AS nb_courses
FROM dw.fact_trip
GROUP BY source_file
ORDER BY source_file;

SELECT COUNT(*) AS courses_janvier
FROM dw.fact_trip
WHERE source_file = 'yellow_tripdata_2026-01.parquet';

SELECT quality_status, COUNT(*) AS nombre
FROM dw.fact_trip
WHERE source_file = 'yellow_tripdata_2026-01.parquet'
GROUP BY quality_status ORDER BY quality_status;

SELECT COUNT(*) AS staging_count FROM staging.fact_trip_load;

-- APRES verification du chargement complet, on peut supprimer le pilote :
-- DELETE FROM dw.fact_trip
-- WHERE source_file = 'yellow_tripdata_2026-01.parquet#pilot100';
