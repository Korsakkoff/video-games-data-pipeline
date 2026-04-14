from pathlib import Path

def get_project_root():
    current = Path(__file__).resolve()
    for parent in [current.parent] + list(current.parents):
        if (parent / "pyproject.toml").exists() or (parent / ".git").exists():
            return parent
    raise FileNotFoundError("Project root not found. Could not locate pyproject.toml or .git")