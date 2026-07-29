-- =====================================================================
-- 04_governance_grants.sql
-- Example Unity Catalog governance setup — grants scoped by role,
-- mirroring the kind of role-based access control you'd configure in
-- Snowflake for a governed document pipeline. Adjust group names to
-- match your workspace's identity provider groups.
-- =====================================================================

USE CATALOG doc_intel;

-- Pipeline service principal: full read/write on pipeline + ai schemas
GRANT USE CATALOG, USE SCHEMA ON CATALOG doc_intel TO `svc-doc-intel-pipeline`;
GRANT ALL PRIVILEGES ON SCHEMA pipeline TO `svc-doc-intel-pipeline`;
GRANT ALL PRIVILEGES ON SCHEMA ai TO `svc-doc-intel-pipeline`;

-- Analysts: read gold tables + explore dashboards, no access to raw
-- documents (which may contain PII) or bronze bytes
GRANT USE CATALOG, USE SCHEMA ON CATALOG doc_intel TO `analysts`;
GRANT SELECT ON TABLE pipeline.gold_extracted_fields TO `analysts`;
GRANT SELECT ON TABLE pipeline.gold_document_summaries TO `analysts`;
GRANT SELECT ON TABLE pipeline.gold_document_anomalies TO `analysts`;
GRANT SELECT ON TABLE pipeline.gold_redacted_documents TO `analysts`;
-- Explicitly withheld: pipeline.bronze_raw_documents, silver_parsed_documents
-- (contain raw text / bytes, which may include unredacted PII)

-- Reviewers: can read + write the review queue, and read redacted text
GRANT USE CATALOG, USE SCHEMA ON CATALOG doc_intel TO `doc-reviewers`;
GRANT SELECT, MODIFY ON TABLE pipeline.review_queue TO `doc-reviewers`;
GRANT SELECT ON TABLE pipeline.gold_redacted_documents TO `doc-reviewers`;
GRANT SELECT ON TABLE pipeline.gold_extracted_fields TO `doc-reviewers`;

-- Compliance/audit: read-only on audit_log and data_quality_results,
-- plus full unredacted access for investigation purposes
GRANT USE CATALOG, USE SCHEMA ON CATALOG doc_intel TO `compliance`;
GRANT SELECT ON TABLE pipeline.audit_log TO `compliance`;
GRANT SELECT ON TABLE pipeline.data_quality_results TO `compliance`;
GRANT SELECT ON SCHEMA pipeline TO `compliance`;

-- Row-level masking example: mask PII in silver_parsed_documents.full_text
-- for anyone not in the `pii-unmasked-readers` group.
CREATE OR REPLACE FUNCTION pipeline.mask_if_not_authorized(full_text STRING)
RETURNS STRING
RETURN CASE
  WHEN is_account_group_member('pii-unmasked-readers') THEN full_text
  ELSE '[TEXT MASKED - see gold_redacted_documents for a PII-safe version]'
END;

ALTER TABLE pipeline.silver_parsed_documents
  ALTER COLUMN full_text SET MASK pipeline.mask_if_not_authorized;
