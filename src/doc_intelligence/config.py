"""Central configuration for the document intelligence pipeline.

Values here can be overridden via Databricks job/task parameters or
widgets in the notebooks — these are just sane defaults.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "doc_intel"
    schema: str = "pipeline"
    ai_schema: str = "ai"

    volume_raw_docs: str = "raw_docs"
    volume_checkpoints: str = "_checkpoints"
    volume_quarantine: str = "quarantine"

    # Foundation Model Serving endpoints (pay-per-token, no provisioning needed)
    llm_endpoint: str = "databricks-claude-sonnet-4"
    embedding_endpoint: str = "databricks-gte-large-en"

    # Vector Search
    vector_search_endpoint: str = "doc_intel_vs_endpoint"
    vector_index_name: str = "gold_document_chunks_index"

    # Chunking
    chunk_size_tokens: int = 512
    chunk_overlap_tokens: int = 64

    document_types: tuple = (
        "invoice",
        "contract",
        "resume",
        "financial_report",
        "id_document",
        "other",
    )

    # Review queue thresholds
    classification_confidence_threshold: float = 0.75
    anomaly_severity_for_review: str = "MEDIUM"  # MEDIUM or HIGH triggers a review item

    # Alerting
    slack_webhook_url: str = ""  # set via job/app env var, not committed
    alert_min_severity: str = "HIGH"

    # Duplicate detection
    near_duplicate_similarity_threshold: float = 0.92

    @property
    def full_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    @property
    def full_ai_schema(self) -> str:
        return f"{self.catalog}.{self.ai_schema}"

    def volume_path(self, name: str) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{name}"

    def table(self, name: str) -> str:
        return f"{self.full_schema}.{name}"


CONFIG = PipelineConfig()
