param(
    [ValidateSet("sync", "test", "app")]
    [string]$Command = "test"
)

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

switch ($Command) {
    "sync" {
        uv sync
    }
    "test" {
        $env:UV_PROJECT_ENVIRONMENT = "C:\Temp\docintel-uv"
        uv run pytest -q
    }
    "app" {
        $env:UV_PROJECT_ENVIRONMENT = "C:\Temp\docintel-uv"
        uv run streamlit run app/app.py --server.headless true --server.port 8501
    }
}
