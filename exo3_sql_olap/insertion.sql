-- Exercice 3 - Donnees de reference et dimensions temporelles
-- Executer APRES creation.sql. Idempotent (ON CONFLICT).
-- Source des codes : NYC TLC Yellow Taxi Trip Records Data Dictionary.

BEGIN;

INSERT INTO dw.dim_vendor(vendor_id, vendor_name) VALUES
    (-1, 'Missing / invalid'),
    (1, 'Creative Mobile Technologies'),
    (2, 'Curb Mobility'),
    (6, 'Myle Technologies'),
    (7, 'Helix')
ON CONFLICT (vendor_id) DO UPDATE SET vendor_name = EXCLUDED.vendor_name;

INSERT INTO dw.dim_payment(payment_type, payment_label) VALUES
    (-1, 'Missing / invalid'),
    (0, 'Flex Fare'),
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip')
ON CONFLICT (payment_type) DO UPDATE SET payment_label = EXCLUDED.payment_label;

INSERT INTO dw.dim_ratecode(ratecode_id, ratecode_label) VALUES
    (-1, 'Missing / invalid'),
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
    (99, 'Unknown / null')
ON CONFLICT (ratecode_id) DO UPDATE SET ratecode_label = EXCLUDED.ratecode_label;

-- Codes de substitution ; les autres zones sont importees depuis le CSV NYC.
INSERT INTO dw.dim_borough(borough_name) VALUES ('Unknown')
ON CONFLICT (borough_name) DO NOTHING;

INSERT INTO dw.dim_service_zone(service_zone_name) VALUES ('Unknown')
ON CONFLICT (service_zone_name) DO NOTHING;

INSERT INTO dw.dim_location(location_id, zone_name, borough_id, service_zone_id)
SELECT 0, 'Unknown / invalid location', b.borough_id, s.service_zone_id
FROM dw.dim_borough b
CROSS JOIN dw.dim_service_zone s
WHERE b.borough_name = 'Unknown' AND s.service_zone_name = 'Unknown'
ON CONFLICT (location_id) DO NOTHING;

-- Calendrier pre-rempli de 2020 a 2035. Il couvre les donnees de janvier 2026.
INSERT INTO dw.dim_date(
    date_key, calendar_date, year_num, quarter_num,
    month_num, day_num, iso_week, iso_weekday, is_weekend
)
SELECT
    EXTRACT(YEAR FROM d)::INTEGER * 10000
        + EXTRACT(MONTH FROM d)::INTEGER * 100
        + EXTRACT(DAY FROM d)::INTEGER,
    d::DATE,
    EXTRACT(YEAR FROM d)::SMALLINT,
    EXTRACT(QUARTER FROM d)::SMALLINT,
    EXTRACT(MONTH FROM d)::SMALLINT,
    EXTRACT(DAY FROM d)::SMALLINT,
    EXTRACT(WEEK FROM d)::SMALLINT,
    EXTRACT(ISODOW FROM d)::SMALLINT,
    (EXTRACT(ISODOW FROM d) IN (6, 7))
FROM generate_series(
    DATE '2020-01-01', DATE '2035-12-31', INTERVAL '1 day'
) AS x(d)
ON CONFLICT (date_key) DO NOTHING;

INSERT INTO dw.dim_hour(hour_id, hour_label, day_period)
SELECT h::SMALLINT,
       LPAD(h::TEXT, 2, '0') || ':00',
       CASE
           WHEN h BETWEEN 5 AND 11 THEN 'Morning'
           WHEN h BETWEEN 12 AND 17 THEN 'Afternoon'
           WHEN h BETWEEN 18 AND 22 THEN 'Evening'
           ELSE 'Night'
       END
FROM generate_series(0, 23) AS t(h)
ON CONFLICT (hour_id) DO NOTHING;

COMMIT;
