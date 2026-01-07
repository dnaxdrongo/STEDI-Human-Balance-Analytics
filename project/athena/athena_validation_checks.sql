-- ============================================================
-- Athena Validation & Evidence Queries (STEDI Lakehouse)
-- ============================================================
-- Purpose:
--   Provide a consistent set of queries to validate each lakehouse
--   zone (Landing → Trusted → Curated) and to capture screenshots
--   that demonstrate correctness and data-quality expectations.
--
-- Conventions:
--   - Run in order (top to bottom)
--   - Capture screenshots of query + results
--   - Save screenshots under: project/screenshots/athena/
-- ============================================================

-- ---------------------------
-- LANDING ZONE (raw JSON)
-- ---------------------------

-- [Screenshot: customer_landing.png]
-- Spot-check raw customer records and visually confirm that
-- shareWithResearchAsOfDate includes blank / null values in landing.
SELECT *
FROM stedi_db.customer_landing
LIMIT 25;

-- [Screenshot: customer_landing_count.png]
SELECT COUNT(*) AS customer_landing_count
FROM stedi_db.customer_landing;

-- Optional: quantify blanks/nulls in shareWithResearchAsOfDate (landing quality issue evidence)
SELECT
  SUM(CASE WHEN sharewithresearchasofdate IS NULL THEN 1 ELSE 0 END) AS null_share_with_research,
  SUM(CASE WHEN CAST(sharewithresearchasofdate AS varchar) = '' THEN 1 ELSE 0 END) AS empty_string_share_with_research
FROM stedi_db.customer_landing;

-- [Screenshot: accelerometer_landing.png]
SELECT *
FROM stedi_db.accelerometer_landing
LIMIT 25;

-- [Screenshot: accelerometer_landing_count.png]
SELECT COUNT(*) AS accelerometer_landing_count
FROM stedi_db.accelerometer_landing;

-- [Screenshot: step_trainer_landing.png]
SELECT *
FROM stedi_db.step_trainer_landing
LIMIT 25;

-- [Screenshot: step_trainer_landing_count.png]
SELECT COUNT(*) AS step_trainer_landing_count
FROM stedi_db.step_trainer_landing;

-- ---------------------------
-- TRUSTED ZONE (consent enforced)
-- ---------------------------

-- [Screenshot: customer_trusted.png]
-- Customers in trusted must have a non-null research consent timestamp.
SELECT *
FROM stedi_db.customer_trusted
LIMIT 25;

-- [Screenshot: customer_trusted_count.png]
SELECT COUNT(*) AS customer_trusted_count
FROM stedi_db.customer_trusted;

-- Evidence: trusted should have no blank/null shareWithResearchAsOfDate
SELECT COUNT(*) AS trusted_missing_share_with_research
FROM stedi_db.customer_trusted
WHERE sharewithresearchasofdate IS NULL
   OR CAST(sharewithresearchasofdate AS varchar) = '';

-- [Screenshot: accelerometer_trusted_count.png]
SELECT COUNT(*) AS accelerometer_trusted_count
FROM stedi_db.accelerometer_trusted;

-- ---------------------------
-- CURATED ZONE (analysis/ML ready)
-- ---------------------------

-- [Screenshot: customer_curated_count.png]
SELECT COUNT(*) AS customer_curated_count
FROM stedi_db.customer_curated;

-- [Screenshot: step_trainer_trusted_count.png]
SELECT COUNT(*) AS step_trainer_trusted_count
FROM stedi_db.step_trainer_trusted;

-- [Screenshot: machine_learning_curated_count.png]
SELECT COUNT(*) AS machine_learning_curated_count
FROM stedi_db.machine_learning_curated;

-- ---------------------------
-- SANITY CHECKS (recommended evidence)
-- ---------------------------

-- [Screenshot: curated_without_accel.png]
-- Expect 0: every curated customer should have accelerometer evidence.
SELECT COUNT(*) AS curated_without_accel
FROM stedi_db.customer_curated c
LEFT JOIN stedi_db.accelerometer_trusted a
  ON lower(c.email) = lower(a.user)
WHERE a.user IS NULL;

-- [Screenshot: orphan_step_rows.png]
-- Expect 0: every step trainer trusted row should map to curated customers.
SELECT COUNT(*) AS orphan_step_rows
FROM stedi_db.step_trainer_trusted s
LEFT JOIN stedi_db.customer_curated c
  ON lower(s.serialnumber) = lower(c.serialnumber)
WHERE c.serialnumber IS NULL;
