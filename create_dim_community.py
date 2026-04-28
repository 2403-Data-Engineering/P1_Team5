

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
WHERE a.community_id IS NOT NULL
WITH a.community_id AS community_id,
     count(a) AS member_count,
     sum(coalesce(a.flagged, 0)) AS flagged_member_count,
     sum(coalesce(a.risk_score, 0)) AS total_risk_score,
     avg(coalesce(a.risk_score, 0)) AS avg_risk_score
OPTIONAL MATCH (x:Account)-[t:TRANSACTION]->(y:Account)
WHERE x.community_id = community_id
WITH community_id,
     member_count,
     flagged_member_count,
     total_risk_score,
     avg_risk_score,
     sum(CASE WHEN y.community_id = community_id THEN t.amount ELSE 0 END) AS internal_volume,
     sum(CASE WHEN y.community_id <> community_id THEN t.amount ELSE 0 END) AS outgoing_volume
RETURN
  community_id AS community_id,
  member_count AS member_count,
  flagged_member_count AS flagged_member_count,
  total_risk_score AS total_risk_score,
  avg_risk_score AS avg_risk_score,
  coalesce(internal_volume, 0) AS internal_volume,
  coalesce(outgoing_volume, 0) AS outgoing_volume,
  CASE
    WHEN coalesce(outgoing_volume, 0) = 0 THEN coalesce(internal_volume, 0)
    ELSE coalesce(internal_volume, 0) * 1.0 / outgoing_volume
  END AS internal_to_outgoing_ratio
"""


EMPTY_COLUMNS = [
    "community_id",
    "member_count",
    "flagged_member_count",
    "total_risk_score",
    "avg_risk_score",
    "internal_volume",
    "outgoing_volume",
    "internal_to_outgoing_ratio",
]


def export_dim_community() -> None:
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))
    rows = []

    try:
        with driver.session(database=DATABASE) as session:
            result = session.run(QUERY)
            for record in result:
                rows.append(record.data())
    finally:
        driver.close()

    dim_community = pd.DataFrame(rows)

    if dim_community.empty:
        dim_community = pd.DataFrame(columns=EMPTY_COLUMNS)

    output_path = OUT_DIR / "dim_community.parquet"
    dim_community.to_parquet(output_path, index=False)

    print(f"dim_community exported to {output_path.resolve()}")
    print(f"Rows exported: {len(dim_community):,}")
    print(dim_community.head())


if __name__ == "__main__":
    export_dim_community()
