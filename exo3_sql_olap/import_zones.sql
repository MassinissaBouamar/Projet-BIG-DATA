-- Import du referentiel officiel NYC TLC (taxi_zone_lookup.csv).
-- Mettre le CSV a l'emplacement /sql/taxi_zone_lookup.csv dans le conteneur.
-- Le dossier exo3_sql_olap est monte en lecture seule sur /sql.
-- Le fichier CSV a 4 colonnes : LocationID, Borough, Zone, service_zone.

BEGIN;
TRUNCATE TABLE staging.taxi_zone_lookup;

COPY staging.taxi_zone_lookup(location_id, borough, zone, service_zone)
FROM '/sql/taxi_zone_lookup.csv'
WITH (FORMAT csv, HEADER true, ENCODING 'UTF8');

INSERT INTO dw.dim_borough(borough_name)
SELECT DISTINCT COALESCE(NULLIF(TRIM(borough), ''), 'Unknown')
FROM staging.taxi_zone_lookup
WHERE TRUE
ON CONFLICT (borough_name) DO NOTHING;

INSERT INTO dw.dim_service_zone(service_zone_name)
SELECT DISTINCT COALESCE(NULLIF(TRIM(service_zone), ''), 'Unknown')
FROM staging.taxi_zone_lookup
WHERE TRUE
ON CONFLICT (service_zone_name) DO NOTHING;

INSERT INTO dw.dim_location(location_id, zone_name, borough_id, service_zone_id)
SELECT
    z.location_id,
    COALESCE(NULLIF(TRIM(z.zone), ''), 'Unknown'),
    b.borough_id,
    s.service_zone_id
FROM staging.taxi_zone_lookup z
JOIN dw.dim_borough b
  ON b.borough_name = COALESCE(NULLIF(TRIM(z.borough), ''), 'Unknown')
JOIN dw.dim_service_zone s
  ON s.service_zone_name = COALESCE(NULLIF(TRIM(z.service_zone), ''), 'Unknown')
WHERE z.location_id > 0
ON CONFLICT (location_id) DO UPDATE
SET zone_name = EXCLUDED.zone_name,
    borough_id = EXCLUDED.borough_id,
    service_zone_id = EXCLUDED.service_zone_id;

COMMIT;
