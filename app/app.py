"""Document Intelligence Platform — Databricks App (Streamlit).

Four tabs, mirroring the Snowflake reference architecture's
Streamlit-in-Snowflake dashboard:
  1. Upload      — drop documents into the raw_docs Volume
  2. Explore      — browse parsed / classified / extracted documents
  3. RAG Chat     — ask questions grounded in the document corpus
  4. Cost Monitor — spend by SKU / day from system.billing.usage

Auth: Databricks Apps inject workspace credentials automatically via
the SDK's default auth chain — no secrets to manage here.
"""

import io
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Document Intelligence Platform", layout="wide")

CATALOG = os.getenv("DOC_INTEL_CATALOG", "doc_intel")
SCHEMA = os.getenv("DOC_INTEL_SCHEMA", "pipeline")
AI_SCHEMA = os.getenv("DOC_INTEL_AI_SCHEMA", "ai")
WAREHOUSE_HTTP_PATH = os.getenv("DATABRICKS_WAREHOUSE_HTTP_PATH", "")
SERVING_AGENT_ENDPOINT = os.getenv("DOC_INTEL_AGENT_ENDPOINT", "doc_intelligence_rag_agent")
CURRENT_USER = os.getenv("DATABRICKS_APP_USER", "app_user")

w = WorkspaceClient()


@st.cache_resource
def get_sql_connection():
    cfg = w.config
    return dbsql.connect(
        server_hostname=cfg.host.replace("https://", ""),
        http_path=WAREHOUSE_HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )


def run_query(query: str) -> pd.DataFrame:
    conn = get_sql_connection()
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall_arrow().to_pandas()


def run_statement(statement: str) -> None:
    conn = get_sql_connection()
    with conn.cursor() as cur:
        cur.execute(statement)


st.title("📄 Document Intelligence Platform")
st.caption("Databricks-native: Unity Catalog + ai_parse_document + ai_query + Vector Search + Agent Framework")

tab_upload, tab_explore, tab_chat, tab_review, tab_monitor, tab_cost = st.tabs(
    ["⬆️ Upload", "🔎 Explore", "💬 RAG Chat", "🧑‍⚖️ Review Queue", "⚠️ Anomalies & Duplicates", "💰 Cost Monitor"]
)

# ---------------------------------------------------------------------
# Tab 1 · Upload
# ---------------------------------------------------------------------
with tab_upload:
    st.subheader("Upload documents to the raw_docs Volume")
    st.write(
        "Files land in `/Volumes/{}/{}/raw_docs` and are picked up by the "
        "Auto Loader ingestion job automatically (file-arrival trigger).".format(
            CATALOG, SCHEMA
        )
    )
    uploaded_files = st.file_uploader(
        "Choose PDF / image / DOCX files",
        type=["pdf", "png", "jpg", "jpeg", "tif", "tiff", "docx"],
        accept_multiple_files=True,
    )
    if uploaded_files and st.button("Upload"):
        volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/raw_docs"
        progress = st.progress(0.0)
        for i, uf in enumerate(uploaded_files):
            target = f"{volume_path}/{uf.name}"
            w.files.upload(target, io.BytesIO(uf.getvalue()), overwrite=True)
            progress.progress((i + 1) / len(uploaded_files))
        st.success(f"Uploaded {len(uploaded_files)} file(s) to {volume_path}.")
        st.info("Trigger the pipeline job now, or wait for the next scheduled/file-arrival run.")
        if st.button("Run pipeline now"):
            job_id = os.getenv("DOC_INTEL_JOB_ID")
            if job_id:
                run = w.jobs.run_now(job_id=int(job_id))
                st.success(f"Started run {run.run_id}")
            else:
                st.warning("Set DOC_INTEL_JOB_ID env var on the app to enable this button.")

# ---------------------------------------------------------------------
# Tab 2 · Explore
# ---------------------------------------------------------------------
with tab_explore:
    st.subheader("Processed documents")
    col1, col2 = st.columns([1, 3])
    with col1:
        doc_type_filter = st.selectbox(
            "Filter by document type",
            ["All", "invoice", "contract", "resume", "financial_report", "id_document", "other"],
        )
    where_clause = "" if doc_type_filter == "All" else f"WHERE g.document_type = '{doc_type_filter}'"

    query = f"""
        SELECT
          g.doc_id, g.document_type, g.vendor_or_party, g.amount_total,
          g.currency, g.doc_date, p.file_name, p.page_count, g.extracted_at,
          s.summary, COALESCE(r.contains_pii, false) AS contains_pii
        FROM {CATALOG}.{SCHEMA}.gold_extracted_fields g
        JOIN {CATALOG}.{SCHEMA}.silver_parsed_documents p USING (doc_id)
        LEFT JOIN {CATALOG}.{SCHEMA}.gold_document_summaries s USING (doc_id)
        LEFT JOIN {CATALOG}.{SCHEMA}.gold_redacted_documents r USING (doc_id)
        {where_clause}
        ORDER BY g.extracted_at DESC
        LIMIT 500
    """
    try:
        df = run_query(query)
        st.dataframe(df, use_container_width=True)

        if not df.empty:
            c1, c2 = st.columns(2)
            with c1:
                type_counts = df["document_type"].value_counts().reset_index()
                type_counts.columns = ["document_type", "count"]
                st.plotly_chart(
                    px.pie(type_counts, names="document_type", values="count", title="Documents by type"),
                    use_container_width=True,
                )
            with c2:
                if df["amount_total"].notna().any():
                    st.plotly_chart(
                        px.bar(
                            df.dropna(subset=["amount_total"]).sort_values("amount_total", ascending=False).head(20),
                            x="vendor_or_party",
                            y="amount_total",
                            title="Top amounts by vendor/party",
                        ),
                        use_container_width=True,
                    )

        selected = st.selectbox("Inspect a document", [""] + df["doc_id"].tolist() if not df.empty else [""])
        if selected:
            detail = run_query(
                f"""
                SELECT extracted_json FROM {CATALOG}.{SCHEMA}.gold_extracted_fields
                WHERE doc_id = '{selected}'
                """
            )
            if not detail.empty:
                st.json(detail.iloc[0]["extracted_json"])
    except Exception as e:
        st.error(f"Couldn't load data — has the pipeline run yet? ({e})")

# ---------------------------------------------------------------------
# Tab 3 · RAG Chat
# ---------------------------------------------------------------------
with tab_chat:
    st.subheader("Ask questions about your documents")
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    question = st.chat_input("e.g. What's the total amount across all invoices from Acme Corp?")
    if question:
        st.session_state.messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.write(question)

        with st.chat_message("assistant"):
            with st.spinner("Searching documents..."):
                try:
                    response = w.serving_endpoints.query(
                        name=SERVING_AGENT_ENDPOINT,
                        dataframe_records=[{"question": question}],
                    )
                    prediction = response.predictions[0]
                    answer = prediction.get("answer", str(prediction))
                    sources = prediction.get("sources", [])
                    st.write(answer)
                    if sources:
                        st.caption("Sources: " + ", ".join(sorted(set(sources))))
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                except Exception as e:
                    st.error(f"Agent endpoint not reachable yet ({e}). Run notebook 06 to deploy it.")

# ---------------------------------------------------------------------
# Tab 4 · Review Queue (human-in-the-loop)
# ---------------------------------------------------------------------
with tab_review:
    st.subheader("Documents awaiting human review")
    st.caption("Low-confidence classifications, detected anomalies, and PII findings land here.")

    try:
        pending = run_query(
            f"""
            SELECT review_id, doc_id, reason, original_payload, created_at
            FROM {CATALOG}.{SCHEMA}.review_queue
            WHERE status = 'PENDING'
            ORDER BY created_at DESC
            LIMIT 100
            """
        )
        if pending.empty:
            st.success("Nothing pending review. 🎉")
        else:
            reason_filter = st.multiselect(
                "Filter by reason",
                sorted(pending["reason"].unique().tolist()),
                default=sorted(pending["reason"].unique().tolist()),
            )
            filtered = pending[pending["reason"].isin(reason_filter)]
            st.write(f"{len(filtered)} item(s) pending.")

            for _, row in filtered.iterrows():
                with st.expander(f"{row['reason']} — doc_id {row['doc_id'][:12]}… (queued {row['created_at']})"):
                    st.json(row["original_payload"])
                    c1, c2, c3 = st.columns(3)
                    if c1.button("✅ Approve", key=f"approve_{row['review_id']}"):
                        run_statement(
                            f"""UPDATE {CATALOG}.{SCHEMA}.review_queue
                                SET status = 'APPROVED', reviewer = '{CURRENT_USER}', reviewed_at = current_timestamp()
                                WHERE review_id = '{row['review_id']}'"""
                        )
                        st.rerun()
                    if c2.button("❌ Reject", key=f"reject_{row['review_id']}"):
                        run_statement(
                            f"""UPDATE {CATALOG}.{SCHEMA}.review_queue
                                SET status = 'REJECTED', reviewer = '{CURRENT_USER}', reviewed_at = current_timestamp()
                                WHERE review_id = '{row['review_id']}'"""
                        )
                        st.rerun()
                    correction = c3.text_input("Correction JSON (optional)", key=f"correction_{row['review_id']}")
                    if correction and st.button("💾 Save correction", key=f"save_{row['review_id']}"):
                        escaped = correction.replace("'", "''")
                        run_statement(
                            f"""UPDATE {CATALOG}.{SCHEMA}.review_queue
                                SET status = 'CORRECTED', corrected_payload = '{escaped}',
                                    reviewer = '{CURRENT_USER}', reviewed_at = current_timestamp()
                                WHERE review_id = '{row['review_id']}'"""
                        )
                        st.rerun()
    except Exception as e:
        st.error(f"Couldn't load the review queue ({e})")

# ---------------------------------------------------------------------
# Tab 5 · Anomalies & Duplicates
# ---------------------------------------------------------------------
with tab_monitor:
    st.subheader("Anomalies")
    try:
        anomalies = run_query(
            f"""
            SELECT doc_id, document_type, anomaly_type, severity, metric_name,
                   metric_value, expected_range, details, detected_at
            FROM {CATALOG}.{SCHEMA}.gold_document_anomalies
            ORDER BY detected_at DESC
            LIMIT 200
            """
        )
        if anomalies.empty:
            st.info("No anomalies detected yet.")
        else:
            sev_counts = anomalies["severity"].value_counts().reset_index()
            sev_counts.columns = ["severity", "count"]
            st.plotly_chart(
                px.bar(sev_counts, x="severity", y="count", color="severity", title="Anomalies by severity"),
                use_container_width=True,
            )
            st.dataframe(anomalies, use_container_width=True)
    except Exception as e:
        st.warning(f"Couldn't load anomalies ({e})")

    st.subheader("Duplicate documents")
    try:
        dupes = run_query(
            f"""
            SELECT doc_id, duplicate_of_doc_id, similarity_score, match_type, detected_at
            FROM {CATALOG}.{SCHEMA}.gold_duplicate_documents
            ORDER BY detected_at DESC
            LIMIT 200
            """
        )
        if dupes.empty:
            st.info("No duplicates detected yet.")
        else:
            st.dataframe(dupes, use_container_width=True)
    except Exception as e:
        st.warning(f"Couldn't load duplicates ({e})")

    st.subheader("Data quality check results")
    try:
        dq = run_query(
            f"""
            SELECT check_name, passed, severity, COUNT(*) AS n
            FROM {CATALOG}.{SCHEMA}.data_quality_results
            GROUP BY check_name, passed, severity
            ORDER BY check_name
            """
        )
        if not dq.empty:
            st.dataframe(dq, use_container_width=True)
    except Exception as e:
        st.warning(f"Couldn't load data quality results ({e})")

# ---------------------------------------------------------------------
# Tab 6 · Cost Monitor
# ---------------------------------------------------------------------
with tab_cost:
    st.subheader("Pipeline cost (last 30 days)")
    cost_query = f"""
        SELECT
          u.usage_date,
          u.sku_name,
          SUM(u.usage_quantity * p.pricing.default) AS estimated_cost_usd
        FROM system.billing.usage u
        JOIN system.billing.list_prices p
          ON u.sku_name = p.sku_name
          AND u.usage_end_time >= p.price_start_time
          AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
        WHERE u.usage_date >= date_sub(current_date(), 30)
          AND (u.custom_tags['project'] = 'doc_intelligence' OR u.custom_tags IS NULL)
        GROUP BY u.usage_date, u.sku_name
        ORDER BY u.usage_date
    """
    try:
        cost_df = run_query(cost_query)
        if cost_df.empty:
            st.info("No usage recorded yet for this project's tags.")
        else:
            total = cost_df["estimated_cost_usd"].sum()
            st.metric("Estimated spend (30d)", f"${total:,.2f}")
            st.plotly_chart(
                px.bar(cost_df, x="usage_date", y="estimated_cost_usd", color="sku_name", title="Daily cost by SKU"),
                use_container_width=True,
            )
            st.dataframe(cost_df, use_container_width=True)
    except Exception as e:
        st.warning(
            "Cost data requires `system.billing.usage` access (account admin must "
            f"enable system schemas). ({e})"
        )
