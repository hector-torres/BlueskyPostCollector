# database.py
import os
import pandas as pd
from sqlalchemy import create_engine, inspect, exc

class DatabaseClient:
    """
    Encapsulates all database operations. Reads database filepath from the
    environment variable DATABASE_URL (fallback to 'data/posts.sqlite').
    Swap out this module to support another DB system without touching ingest logic.
    """
    def __init__(self, schema: dict[str, str], logger):
        # Determine project root and load DB path from env
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_url = os.getenv('DATABASE_URL', 'data/posts.sqlite')

        # Resolve relative paths against project root
        if not os.path.isabs(db_url):
            db_path = os.path.normpath(os.path.join(base_dir, db_url))
        else:
            db_path = db_url

        # Ensure parent directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        self.db_path = db_path
        # Allow table name override via env, default 'posts'
        self.table_name = os.getenv('TABLE_NAME', 'posts')
        self.schema = schema
        self.logger = logger

        self.engine = self._create_engine()
        self._ensure_table()

    def _create_engine(self):
        return create_engine(f"sqlite:///{self.db_path}", future=True)

    def _ensure_table(self):
        insp = inspect(self.engine)
        if not insp.has_table(self.table_name):
            df = pd.DataFrame({col: pd.Series(dtype=dtype) for col, dtype in self.schema.items()})
            df.to_sql(self.table_name, self.engine, index=False, if_exists="fail")
            self.logger.info(
                f"Created table '{self.table_name}' at '{self.db_path}' with columns: {list(self.schema.keys())}"
            )

    def get_existing_uuids(self) -> set[str]:
        insp = inspect(self.engine)
        if not insp.has_table(self.table_name):
            return set()
        df = pd.read_sql_table(self.table_name, self.engine, columns=["uuid"])
        return set(df["uuid"].dropna().unique())

    def append_dataframe(self, df: pd.DataFrame):
        try:
            df.to_sql(self.table_name, self.engine, if_exists="append", index=False)
            self.logger.info(
                f"Inserted {len(df)} new rows into '{self.table_name}'"
            )
        except exc.SQLAlchemyError:
            self.logger.exception("DB insert failed")
            raise