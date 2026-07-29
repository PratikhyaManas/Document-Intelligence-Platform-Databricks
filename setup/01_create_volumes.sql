-- =====================================================================
-- 01_create_volumes.sql
-- Unity Catalog Volumes — the Databricks equivalent of a Snowflake
-- internal/external stage. Raw documents land here and are never
-- copied into Delta as anything but a reference + optionally the bytes.
-- =====================================================================

USE CATALOG doc_intel;
USE SCHEMA pipeline;

-- Landing zone: drop PDFs / images / docx here (manually, via CLI, or
-- via an external Auto Loader source directory if you prefer cloud storage).
CREATE VOLUME IF NOT EXISTS raw_docs
  COMMENT 'Landing zone for incoming documents (PDF, PNG, JPG, DOCX, TIFF)';

-- Auto Loader checkpoint + schema location (keeps state out of raw_docs)
CREATE VOLUME IF NOT EXISTS _checkpoints
  COMMENT 'Auto Loader checkpoint and schema-inference state';

-- Quarantine for documents that fail parsing/classification
CREATE VOLUME IF NOT EXISTS quarantine
  COMMENT 'Documents that failed parsing or classification for manual review';
