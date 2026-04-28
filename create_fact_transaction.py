

from pathlib import Path
import pandas as pd
from neo4j import GraphDatabase



URI = ""
USER = ""
PASSWORD = ""
DATABASE = ""

OUT_DIR = Path("powerbi_exports")
OUT_DIR.mkdir(exist_ok=True)

QMATCH (orig:Account)-[t:TRANSACTION]->(dest:Account)
RETURN
  elementId(t) AS transaction_id,
  orig.id AS sender_account_id,
  dest.id AS receiver_account_id,
  coalesce(orig.community_id, -1) AS community_id,   
  t.step AS step,
  t.type AS transaction_type,
  t.amount AS amount,
  t.oldbalanceOrg AS oldBalanceOrg,
  t.newbalanceOrig AS newBalanceOrig,
  t.oldbalanceDest AS oldBalanceDest,
  t.newbalanceDest AS newBalanceDest,
  coalesce(t.transfer_cashout_flag, 0) AS transfer_cashout_flag,
  coalesce(orig.flagged, 0) AS sender_flagged,
  coalesce(dest.flagged, 0) AS receiver_flagged,
  coalesce(orig.risk_score, 0) AS sender_risk_score,
  coalesce(dest.risk_score, 0) AS receiver_risk_score


def export_fact_transaction() -> None:
    driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

    rows = []
    try:
        with driver.session(database=DATABASE) as session:
            result = session.run(QUERY)
            for record in result:
                rows.append(record.data())
    finally:
        driver.close()

    fact_transaction = pd.DataFrame(rows)

    base_datetime = pd.Timestamp("2024-01-01 00:00:00")

    fact_transaction["datetime"] = fact_transaction["step"].apply(
        lambda s: base_datetime + pd.Timedelta(hours=s - 1)
    )

    fact_transaction["date"] = fact_transaction["datetime"].dt.date.astype(str)

    output_path = OUT_DIR / "fact_transaction"

    fact_transaction.to_parquet(
        output_path,
        index=False,
        partition_cols=["date"]
    )

    print(f"fact_transaction exported as partitioned parquet to {output_path.resolve()}")
    print(f"Rows exported: {len(fact_transaction):,}")
    print(fact_transaction.head())


if __name__ == "__main__":
    export_fact_transaction()
    

    fact_transaction = pd.DataFrame(rows)

    output_path = OUT_DIR / "fact_transaction.parquet"
    fact_transaction.to_parquet(output_path, index=False)

    print(f"fact_transaction exported to {output_path.resolve()}")
    print(f"Rows exported: {len(fact_transaction):,}")
    print(fact_transaction.head())


if __name__ == "__main__":
    export_fact_transaction()
