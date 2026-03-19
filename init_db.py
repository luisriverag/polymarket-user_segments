import os
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2 import OperationalError

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/postgres")
SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def main():
    try:
        conn = psycopg2.connect(PG_DSN)
    except OperationalError as e:
        print("Could not connect to PostgreSQL.")
        print(f"PG_DSN={PG_DSN}")
        print()
        print("Make sure PostgreSQL is running and accessible.")
        print("Examples:")
        print("  sudo systemctl start postgresql")
        print("  pg_isready -h localhost -p 5432")
        print()
        print(f"Original error: {e}")
        sys.exit(1)

    conn.autocommit = True

    with conn.cursor() as cur:
        schema_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        cur.execute(schema_sql)

        backfill_minutes = int(os.getenv("BACKFILL_MINUTES", "10"))
        since_ts = int((datetime.now(timezone.utc) - timedelta(minutes=backfill_minutes)).timestamp())

        cur.execute(
            """
            insert into ingestion_state(key, value, updated_at)
            values ('trades_cursor', jsonb_build_object('since_ts', %s), now())
            on conflict (key) do nothing
            """,
            (since_ts,),
        )

    conn.close()
    print("Database initialized successfully.")


if __name__ == "__main__":
    main()
