"""Load SQL files bundled in ``inciweb_pipeline/sql/``.
"""
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"


def read_sql(name: str) -> str:
    return (SQL_DIR / name).read_text()
