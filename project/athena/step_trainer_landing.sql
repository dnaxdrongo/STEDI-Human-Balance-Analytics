-- =====================================================================
-- Step Trainer Landing (JSON on S3)
-- Purpose:
--   Register raw step trainer landing-zone JSON as an external table.
--
-- Notes:
--   * sensorReadingTime aligns to accelerometer "timestamp" for the ML join.
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.step_trainer_landing (
  sensorReadingTime   bigint,
  serialNumber        string,
  distanceFromObject  int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-dlbadley-2026/step_trainer/landing/';
