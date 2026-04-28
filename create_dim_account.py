

from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase


URI = ""
USER = ""
PASSWORD = ""
DATABASE = ""

OUT_DIR = Path("powerbi_exports")
OUT_DIR.mkdir(exist_ok=True)

QUERY = """
MATCH (a:Account)
RETURN
  a.id AS account_id,
  coalesce(a.flagged, 0) AS flagged,
  coalesce(a.risk_score, 0) AS risk_score,
  coalesce(a.fan_in_flag, 0) AS fan_in_flag,
  coalesce(a.fan_out_flag, 0) AS fan_out_flag,
  coalesce(a.drain_flag, 0) AS drain_flag,
  coalesce(a.transfer_cashout_flag, 0) AS transfer_cashout_flag,
  coalesce(a.in_suspicious_ring_flag, 0) AS in_suspicious_ring_flag,
  coalesce(a.in_cycle_flag, 0) AS in_cycle_flag,
  coalesce(a.guilt_by_association_flag, 0) AS guilt_by_association_flag,
  coalesce(a.similar_to_flagged_flag, 0) AS similar_to_flagged_flag,
  coalesce(a.community_id, -1) AS community_id
"""


def export_dim_account() -> None:
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    rows = []

    try:
        with driver.session(database=DATABASE) as session:
            result = session.run(QUERY)
            for record in result:
                rows.append(record.data())
    finally:
        driver.close()

    dim_account = pd.DataFrame(rows)
    output_path = OUT_DIR / "dim_account.parquet"
    dim_account.to_parquet(output_path, index=False)

    print(f"dim_account exported to {output_path.resolve()}")
    print(f"Rows exported: {len(dim_account):,}")
    print(dim_account.head())


if __name__ == "__main__":
    export_dim_account()
