"""Feature store reader — point-in-time correct feature retrieval."""
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import structlog

log = structlog.get_logger()


class FeatureReader:
    """
    Reads features as-of a given timestamp (point-in-time correct).
    Prevents label leakage by only returning features available before the label event.
    """

    def __init__(self, db_url: str, table: str):
        self.engine = create_engine(db_url)
        self.table  = table

    def get_features_as_of(self, entity_ids: list[str], as_of: datetime) -> pd.DataFrame:
        """
        Return the latest feature row for each entity_id that was created
        before or at `as_of`. Guarantees no future data leakage.
        """
        query = text(f"""
            SELECT DISTINCT ON (entity_id) *
            FROM {self.table}
            WHERE entity_id = ANY(:ids)
              AND event_timestamp <= :as_of
            ORDER BY entity_id, event_timestamp DESC
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"ids": entity_ids, "as_of": as_of})
        log.info("features_retrieved", n=len(df), as_of=as_of.isoformat())
        return df
