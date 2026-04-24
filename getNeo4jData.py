import os
from neo4j import GraphDatabase
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

#Add .ENV file with NEO4J_PASSWORD=your_password
URI = "bolt://localhost:7687" 
AUTH = ("neo4j", os.getenv("NEO4J_PASSWORD"))
driver = GraphDatabase.driver(URI, auth=AUTH)


def get_transaction_types(driver):
    query = """
        MATCH (a:Account)-[t:TRANSACTION]->(:Account)
WITH t.type AS type_label,
     count(t) AS total,
     sum(CASE WHEN a.flagged = 1 THEN 1 ELSE 0 END) AS flagged_count
WITH type_label,
     total,
     flagged_count,
     toFloat(flagged_count) / total AS fraud_rate
RETURN type_label,
       fraud_rate > 0.01 AS is_high_risk_type,
       round(fraud_rate * 100, 2) AS fraud_rate_pct
ORDER BY fraud_rate DESC
    """
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]
        # df = pd.DataFrame([record.data() for record in result])
        # return df


result = get_transaction_types(driver)
# result.to_parquet("TableParquet/transactions.parquet", index=False)
# print(f"Written {len(result)} rows")
# result = get_transaction_types(driver)
for row in result:
    print(row)

