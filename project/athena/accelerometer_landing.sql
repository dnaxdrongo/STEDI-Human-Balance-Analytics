-- =====================================================================
-- Accelerometer Landing (JSON on S3)
-- Purpose:
--   Register raw accelerometer landing-zone JSON as an external table.
--
-- Notes:
--   * "user" is the email identifier in the dataset.
--   * "timestamp" is an epoch-millis event timestamp.
--   * If you ever hit identifier conflicts in queries, alias columns in
--     SELECT statements (e.g., SELECT "user" AS user_email ...).
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.accelerometer_landing (
  user       string,
  timestamp  bigint,
  x          double,
  y          double,
  z          double
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-dlbadley-2026/accelerometer/landing/';
