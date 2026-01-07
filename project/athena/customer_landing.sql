-- =====================================================================
-- Customer Landing (JSON on S3)
-- Purpose:
--   Register the raw customer landing-zone JSON as an external table so
--   it can be queried in Athena and referenced by downstream Glue jobs.
--
-- Notes:
--   * Column names match JSON keys used by the STEDI landing dataset.
--   * Data remains in S3; this table is metadata only (external).
-- =====================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS stedi_db.customer_landing (
  customerName               string,
  email                      string,
  phone                      string,
  birthDay                   string,
  serialNumber               string,
  registrationDate           bigint,
  lastUpdateDate             bigint,
  shareWithResearchAsOfDate  bigint,
  shareWithPublicAsOfDate    bigint,
  shareWithFriendsAsOfDate   bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-dlbadley-2026/customer/landing/';
