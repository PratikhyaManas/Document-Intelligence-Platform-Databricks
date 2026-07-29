-- =====================================================================
-- 03_create_extended_tables.sql
-- Tables backing the "improvised" feature set: PII redaction, LLM
-- summaries, duplicate detection, anomaly detection, human-in-the-loop
-- review queue, data-quality results, and an audit log.
-- =====================================================================

USE CATALOG doc_intel;
USE SCHEMA pipeline;

-- ---------------------------------------------------------------------
-- PII detection & redaction (equivalent to Snowflake's AI_REDACT)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_redacted_documents (
  doc_id             STRING NOT NULL,
  redacted_text      STRING COMMENT 'full_text with PII spans replaced by [REDACTED:<TYPE>]',
  pii_entities_json  STRING COMMENT 'JSON array of {type, value_masked, count}',
  pii_types_found    ARRAY<STRING>,
  contains_pii       BOOLEAN,
  redaction_model    STRING,
  redacted_at        TIMESTAMP
)
USING DELTA
COMMENT 'PII-redacted text + detected entity inventory, safe for broader sharing';

-- ---------------------------------------------------------------------
-- Document summaries
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_document_summaries (
  doc_id          STRING NOT NULL,
  summary         STRING,
  key_points      ARRAY<STRING>,
  sentiment       STRING COMMENT 'positive | neutral | negative | n/a — mainly useful for reviews/feedback docs',
  summary_model   STRING,
  summarized_at   TIMESTAMP
)
USING DELTA
COMMENT 'LLM-generated executive summaries per document';

-- ---------------------------------------------------------------------
-- Duplicate / near-duplicate detection
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_duplicate_documents (
  doc_id              STRING NOT NULL,
  duplicate_of_doc_id STRING NOT NULL,
  similarity_score    DOUBLE,
  match_type          STRING COMMENT 'EXACT_HASH | NEAR_DUPLICATE_EMBEDDING',
  detected_at         TIMESTAMP
)
USING DELTA
COMMENT 'Pairs of documents flagged as exact or near-duplicates';

-- ---------------------------------------------------------------------
-- Anomaly detection on extracted numeric fields
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_document_anomalies (
  doc_id           STRING NOT NULL,
  document_type    STRING,
  anomaly_type     STRING COMMENT 'AMOUNT_OUTLIER | DUPLICATE_INVOICE_NUMBER | FUTURE_DATE | MISSING_REQUIRED_FIELD',
  metric_name      STRING,
  metric_value     DOUBLE,
  expected_range   STRING,
  severity         STRING COMMENT 'LOW | MEDIUM | HIGH',
  details          STRING,
  detected_at      TIMESTAMP
)
USING DELTA
COMMENT 'Statistical / rule-based anomalies flagged for review';

-- ---------------------------------------------------------------------
-- Human-in-the-loop review queue (low-confidence classification or
-- extraction, and anything flagged by anomaly detection)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS review_queue (
  review_id         STRING NOT NULL,
  doc_id            STRING NOT NULL,
  reason            STRING COMMENT 'LOW_CONFIDENCE_CLASSIFICATION | LOW_CONFIDENCE_EXTRACTION | ANOMALY | PII_FOUND | MANUAL_FLAG',
  status            STRING COMMENT 'PENDING | APPROVED | REJECTED | CORRECTED',
  original_payload  STRING COMMENT 'JSON snapshot of the field(s) under review',
  corrected_payload STRING COMMENT 'JSON of reviewer-submitted corrections, if any',
  reviewer          STRING,
  created_at        TIMESTAMP,
  reviewed_at       TIMESTAMP
)
USING DELTA
COMMENT 'Human-in-the-loop queue for low-confidence or flagged documents';

-- ---------------------------------------------------------------------
-- Data quality check results (Delta expectations-style, but explicit)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_quality_results (
  check_id       STRING NOT NULL,
  doc_id         STRING,
  check_name     STRING COMMENT 'e.g. non_null_vendor, valid_currency_code, amount_positive',
  passed         BOOLEAN,
  severity       STRING COMMENT 'WARN | ERROR',
  details        STRING,
  run_id         STRING,
  checked_at     TIMESTAMP
)
USING DELTA
COMMENT 'Row/field-level data quality check outcomes';

-- ---------------------------------------------------------------------
-- Audit log — who/what touched which document at which pipeline stage
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
  event_id     STRING NOT NULL,
  doc_id       STRING,
  event_type   STRING COMMENT 'INGESTED | PARSED | CLASSIFIED | EXTRACTED | REDACTED | REVIEWED | ALERTED | REPROCESSED',
  actor        STRING COMMENT 'service principal / user / job run id',
  detail       STRING,
  event_time   TIMESTAMP
)
USING DELTA
COMMENT 'Append-only audit trail across the pipeline';
