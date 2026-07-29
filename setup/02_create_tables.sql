-- =====================================================================
-- 02_create_tables.sql
-- Bronze / Silver / Gold Delta tables for the document pipeline.
-- =====================================================================

USE CATALOG doc_intel;
USE SCHEMA pipeline;

-- ---------------------------------------------------------------------
-- BRONZE: raw file metadata + bytes, as landed by Auto Loader
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze_raw_documents (
  doc_id            STRING NOT NULL COMMENT 'sha256(path) — stable document id',
  path              STRING NOT NULL,
  file_name         STRING,
  file_extension    STRING,
  file_size_bytes   BIGINT,
  content           BINARY COMMENT 'Raw file bytes',
  ingested_at       TIMESTAMP,
  modification_time TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Bronze: raw documents landed from the Volume via Auto Loader';

-- ---------------------------------------------------------------------
-- SILVER: parsed text/layout output from ai_parse_document()
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_parsed_documents (
  doc_id            STRING NOT NULL,
  file_name         STRING,
  page_count        INT,
  full_text         STRING COMMENT 'Concatenated plain text, reading-order preserved',
  layout_json       STRING COMMENT 'Raw ai_parse_document() JSON (layout, tables, bboxes)',
  parse_status      STRING COMMENT 'SUCCESS | FAILED',
  parse_error       STRING,
  parsed_at         TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Silver: ai_parse_document() output, LAYOUT mode';

-- ---------------------------------------------------------------------
-- SILVER: document classification
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver_classified_documents (
  doc_id            STRING NOT NULL,
  document_type     STRING COMMENT 'invoice | contract | resume | report | id_document | other',
  confidence        DOUBLE,
  classifier_model  STRING,
  classified_at     TIMESTAMP
)
USING DELTA
COMMENT 'Silver: zero-shot document classification via ai_query()';

-- ---------------------------------------------------------------------
-- GOLD: structured field extraction (schema varies per document_type,
-- stored as a JSON string + a few promoted columns for BI)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_extracted_fields (
  doc_id            STRING NOT NULL,
  document_type     STRING,
  extracted_json    STRING COMMENT 'Full structured extraction result as JSON',
  vendor_or_party    STRING COMMENT 'Promoted: vendor/counterparty name if present',
  amount_total      DOUBLE COMMENT 'Promoted: total monetary amount if present',
  currency          STRING,
  doc_date          DATE  COMMENT 'Promoted: document/invoice/effective date',
  extraction_model  STRING,
  extracted_at      TIMESTAMP
)
USING DELTA
COMMENT 'Gold: structured, queryable fields extracted per document';

-- ---------------------------------------------------------------------
-- GOLD: chunked text for vector search / RAG
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold_document_chunks (
  chunk_id          STRING NOT NULL,
  doc_id            STRING NOT NULL,
  file_name         STRING,
  document_type     STRING,
  chunk_index       INT,
  chunk_text        STRING,
  chunk_tokens      INT,
  created_at        TIMESTAMP
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Gold: chunked document text, source table for the Vector Search Delta Sync index';

-- ---------------------------------------------------------------------
-- Pipeline run log (lightweight observability, mirrors the Snowflake
-- article's "cost monitoring dashboard" idea at the run level)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_run_log (
  run_id            STRING,
  stage             STRING COMMENT 'ingest | parse | classify | extract | index',
  status            STRING COMMENT 'SUCCESS | FAILED',
  rows_processed    BIGINT,
  started_at        TIMESTAMP,
  ended_at          TIMESTAMP,
  details           STRING
)
USING DELTA
COMMENT 'Per-stage pipeline execution log';
