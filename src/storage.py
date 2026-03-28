from pathlib import Path
import sqlite3
import pandas as pd


def save_to_csv(df: pd.DataFrame, path: str) -> None:
    """Save the scraped DataFrame to a CSV file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False)


def save_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str = "jobs") -> None:
    """Save the scraped DataFrame to a SQLite database."""
    target = Path(db_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(target) as connection:
        df.to_sql(table_name, connection, if_exists="replace", index=False)
