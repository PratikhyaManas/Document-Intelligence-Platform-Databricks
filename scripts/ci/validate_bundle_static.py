"""Static Databricks bundle sanity checks for CI.

This script intentionally performs only local file/YAML structure checks
and requires no Databricks authentication.
"""

from pathlib import Path

import yaml


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    databricks_yml = root / "databricks.yml"
    jobs_yml = root / "jobs" / "document_pipeline_job.yml"
    benchmark_notebook = root / "notebooks" / "13_duplicate_detection_benchmark.py"

    assert databricks_yml.exists(), "databricks.yml not found"
    assert jobs_yml.exists(), "jobs/document_pipeline_job.yml not found"
    assert benchmark_notebook.exists(), "Benchmark notebook not found"

    with databricks_yml.open("r", encoding="utf-8") as f:
        bundle = yaml.safe_load(f)

    includes = bundle.get("include", [])
    assert any("jobs/*.yml" in str(i) for i in includes), "Bundle include must reference jobs/*.yml"

    with jobs_yml.open("r", encoding="utf-8") as f:
        jobs_cfg = yaml.safe_load(f)

    jobs = jobs_cfg.get("resources", {}).get("jobs", {})
    benchmark = jobs.get("document_intelligence_benchmark")
    assert benchmark is not None, "document_intelligence_benchmark job missing"

    tasks = benchmark.get("tasks", [])
    assert tasks, "document_intelligence_benchmark has no tasks"
    task = tasks[0]
    path = task.get("notebook_task", {}).get("notebook_path")
    assert path == "../notebooks/13_duplicate_detection_benchmark.py", (
        f"Unexpected benchmark notebook path: {path}"
    )

    print("Static bundle sanity checks passed.")


if __name__ == "__main__":
    main()
