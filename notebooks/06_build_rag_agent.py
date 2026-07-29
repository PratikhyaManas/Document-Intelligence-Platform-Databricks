# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Build & Deploy the RAG Agent
# MAGIC Defines a retrieval-augmented agent (LangChain-style pyfunc) that:
# MAGIC 1. embeds the user question and searches the Vector Search index,
# MAGIC 2. stuffs the top-k chunks into a prompt for the LLM endpoint,
# MAGIC 3. answers with citations back to `doc_id` / `file_name`.
# MAGIC
# MAGIC Logs the agent with MLflow, registers it to Unity Catalog, and
# MAGIC deploys a Model Serving endpoint — equivalent to a Snowflake Cortex
# MAGIC Agent + Snowflake Intelligence.

# COMMAND ----------
# MAGIC %pip install -q databricks-vectorsearch mlflow databricks-agents
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import sys

sys.path.append("../src")
from doc_intelligence.config import CONFIG  # noqa: E402

dbutils.widgets.text("catalog", CONFIG.catalog)
dbutils.widgets.text("schema", CONFIG.schema)
dbutils.widgets.text("ai_schema", CONFIG.ai_schema)
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
ai_schema = dbutils.widgets.get("ai_schema")

index_fullname = f"{catalog}.{ai_schema}.{CONFIG.vector_index_name}"
registered_model_name = f"{catalog}.{ai_schema}.doc_intelligence_rag_agent"

# COMMAND ----------
agent_code = f'''
import mlflow
from databricks.vector_search.client import VectorSearchClient
from mlflow.pyfunc import PythonModel

VS_ENDPOINT = "{CONFIG.vector_search_endpoint}"
VS_INDEX = "{index_fullname}"
LLM_ENDPOINT = "{CONFIG.llm_endpoint}"


class DocIntelligenceRAGAgent(PythonModel):
    def load_context(self, context):
        self.vsc = VectorSearchClient()
        self.index = self.vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

    def _retrieve(self, question, k=5, document_type=None):
        filters = {{"document_type": document_type}} if document_type else None
        results = self.index.similarity_search(
            query_text=question,
            columns=["doc_id", "file_name", "document_type", "chunk_text"],
            num_results=k,
            filters=filters,
        )
        return results.get("result", {{}}).get("data_array", [])

    def predict(self, context, model_input):
        import mlflow.deployments

        client = mlflow.deployments.get_deploy_client("databricks")
        outputs = []
        for row in model_input.to_dict(orient="records"):
            question = row["question"]
            document_type = row.get("document_type")
            chunks = self._retrieve(question, document_type=document_type)

            context_block = "\\n\\n".join(
                f"[Source: {{c[1]}} | doc_id={{c[0]}}]\\n{{c[3]}}" for c in chunks
            )
            prompt = (
                "You are a document intelligence assistant. Answer the question "
                "using ONLY the context below. Cite the source file name(s) you "
                "used. If the answer isn't in the context, say so.\\n\\n"
                f"Context:\\n{{context_block}}\\n\\nQuestion: {{question}}\\nAnswer:"
            )
            response = client.predict(
                endpoint=LLM_ENDPOINT,
                inputs={{"messages": [{{"role": "user", "content": prompt}}]}},
            )
            answer = response["choices"][0]["message"]["content"]
            outputs.append({{"answer": answer, "sources": [c[1] for c in chunks]}})
        import pandas as pd

        return pd.DataFrame(outputs)
'''

with open("_rag_agent_module.py", "w") as f:
    f.write(agent_code)

# COMMAND ----------
import mlflow
import pandas as pd
from _rag_agent_module import DocIntelligenceRAGAgent

mlflow.set_registry_uri("databricks-uc")

input_example = pd.DataFrame({"question": ["What invoices are from Acme Corp?"]})

with mlflow.start_run(run_name="doc_intelligence_rag_agent"):
    model_info = mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=DocIntelligenceRAGAgent(),
        input_example=input_example,
        registered_model_name=registered_model_name,
        pip_requirements=[
            "mlflow",
            "databricks-vectorsearch",
            "pandas",
        ],
    )

print(f"Registered model: {registered_model_name}")
print(f"Run ID: {model_info.run_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Deploy a serving endpoint for the agent
# MAGIC Uses the Agent Framework deployment helper, which wires up
# MAGIC review-app + feedback logging in addition to a REST endpoint.

# COMMAND ----------
from databricks import agents

latest_version = mlflow.tracking.MlflowClient().get_latest_versions(
    registered_model_name
)[0].version

deployment = agents.deploy(
    model_name=registered_model_name,
    model_version=latest_version,
    scale_to_zero=True,
)
print(f"Agent endpoint: {deployment.endpoint_name}")
print(f"Review app URL: {deployment.review_app_url}")
