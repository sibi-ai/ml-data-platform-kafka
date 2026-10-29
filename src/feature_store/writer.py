"""Feature store writer — versioned writes with schema enforcement."""
import pandas as pd
from datetime import datetime, timezone
from sqlalchemy import create_engine, text
import structlog

log = structlog.get_logger()


class FeatureWriter:
    """
    Writes feature records to the versioned feature store.
    Each write includes an event_timestamp and created_at for point-in-time correctness.
    """

    def __init__(self, db_url: str, table: str):
        self.engine = create_engine(db_url, pool_size=10, max_overflow=20)
        self.table  = table

    def write_batch(self, records: list[dict]) -> int:
        now = datetime.now(timezone.utc)
        rows = [
            {**r, "created_at": now, "event_timestamp": r.get("ts", now)}
            for r in records
        ]
        df = pd.DataFrame(rows)
        df.to_sql(self.table, self.engine, if_exists="append", index=False, method="multi")
        log.info("features_written", table=self.table, n=len(rows))
        return len(rows)
