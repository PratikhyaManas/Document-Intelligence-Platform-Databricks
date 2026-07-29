-- =====================================================================
-- 00_create_catalog_schema.sql
-- Creates the Unity Catalog catalog/schema that host the whole platform.
-- Equivalent to `CREATE DATABASE` / `CREATE SCHEMA` in Snowflake.
-- =====================================================================

CREATE CATALOG IF NOT EXISTS doc_intel
  COMMENT 'Document Intelligence Platform — catalog';

USE CATALOG doc_intel;

CREATE SCHEMA IF NOT EXISTS pipeline
  COMMENT 'Bronze/silver/gold tables + volumes for the document pipeline';

USE SCHEMA pipeline;

-- Optional: dedicated schema for the vector search + agent artifacts
CREATE SCHEMA IF NOT EXISTS ai
  COMMENT 'Vector search indexes, registered models, agent artifacts';
