"""Reprocess specific documents end-to-end by deleting their downstream
rows (silver/gold/review/anomaly/dedup/redaction/summary) and, if
requested, triggering the pipeline job so Auto Loader / the notebooks
pick them back up on the next run.

Usage:
    python scripts/reprocess_documents.py --doc-ids abc123,def456
    python scripts/reprocess_documents.py --doc-ids abc123 --trigger-job 123456789
    python scripts/reprocess_documents.py --all-failed-parses

Requires the Databricks CLI to be configured (`databricks auth login`)
or DATABRICKS_HOST / DATABRICKS_TOKEN env vars set.
"""

import argparse
import sys
from itertools import islice

from databricks.sdk import WorkspaceClient

DOWNSTREAM_TABLES = [
    "silver_parsed_documents",
    "silver_classified_documents",
    "gold_extracted_fields",
    "gold_document_chunks",
    "gold_redacted_documents",
    "gold_document_summaries",
    "gold_document_anomalies",
    "gold_duplicate_documents",
    "review_queue",
    "data_quality_results",
]


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _chunked(values: list[str], size: int):
    iterator = iter(values)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            return
        yield chunk


def build_delete_statements(table_name: str, table_fullname: str, doc_ids: list[str], batch_size: int) -> list[str]:
    statements = []
    for chunk in _chunked(doc_ids, batch_size):
        id_list = ", ".join(_sql_quote(d) for d in chunk)
        if table_name == "gold_duplicate_documents":
            where_clause = f"doc_id IN ({id_list}) OR duplicate_of_doc_id IN ({id_list})"
        else:
            where_clause = f"doc_id IN ({id_list})"
        statements.append(f"DELETE FROM {table_fullname} WHERE {where_clause}")
    return statements


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="doc_intel")
    parser.add_argument("--schema", default="pipeline")
    parser.add_argument("--warehouse-id", required=True, help="SQL warehouse id to run DELETEs on")
    parser.add_argument("--doc-ids", help="Comma-separated doc_id list to reprocess")
    parser.add_argument(
        "--all-failed-parses", action="store_true",
        help="Reprocess every doc_id currently in silver_parsed_documents with parse_status='FAILED'",
    )
    parser.add_argument("--trigger-job", help="Job id to run_now() after clearing downstream rows")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500, help="DELETE batch size per statement")
    args = parser.parse_args()

    if args.batch_size <= 0:
        print("--batch-size must be > 0", file=sys.stderr)
        sys.exit(1)

    w = WorkspaceClient()

    doc_ids = []
    if args.doc_ids:
        doc_ids = [d.strip() for d in args.doc_ids.split(",") if d.strip()]
    elif args.all_failed_parses:
        query = (
            f"SELECT doc_id FROM {args.catalog}.{args.schema}.silver_parsed_documents "
            "WHERE parse_status = 'FAILED'"
        )
        result = w.statement_execution.execute_statement(
            warehouse_id=args.warehouse_id, statement=query, catalog=args.catalog, schema=args.schema
        )
        doc_ids = [row[0] for row in (result.result.data_array or [])]
    else:
        print("Provide --doc-ids or --all-failed-parses", file=sys.stderr)
        sys.exit(1)

    if not doc_ids:
        print("No matching doc_ids found — nothing to do.")
        return

    print(f"Reprocessing {len(doc_ids)} document(s): {doc_ids[:5]}{'...' if len(doc_ids) > 5 else ''}")

    for table in DOWNSTREAM_TABLES:
        table_fullname = f"{args.catalog}.{args.schema}.{table}"
        statements = build_delete_statements(table, table_fullname, doc_ids, args.batch_size)
        for stmt in statements:
            print(f"  {stmt}")
            if not args.dry_run:
                w.statement_execution.execute_statement(
                    warehouse_id=args.warehouse_id, statement=stmt, catalog=args.catalog, schema=args.schema
                )

    print("Downstream rows cleared. Bronze rows are left intact so the "
          "parse stage will pick these documents back up.")

    if args.trigger_job and not args.dry_run:
        run = w.jobs.run_now(job_id=int(args.trigger_job))
        print(f"Triggered job run: {run.run_id}")


if __name__ == "__main__":
    main()
