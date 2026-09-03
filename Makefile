.PHONY: sync test app

sync:
	uv sync

test:
	uv run pytest -q

app:
	uv run streamlit run app/app.py --server.headless true --server.port 8501
