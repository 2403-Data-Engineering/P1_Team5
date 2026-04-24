import os
from neo4j import GraphDatabase
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

#Add .ENV file with NEO4J_PASSWORD=your_password
URI = "bolt://localhost:7687" 
AUTH = ("neo4j", os.getenv("NEO4J_PASSWORD"))
driver = GraphDatabase.driver(URI, auth=AUTH)


def create_dim_account(driver):
    query = """
        MATCH (a:Account)
        RETURN a.id,a.risk_score,a.flagged,a.drain_flag,a.fan_in_flag,a.fan_out_flag,a.in_cycle_flag,
        a.guilt_by_association_flag
      
    """
    with driver.session() as session:
        result = session.run(query)
        #return [record.data() for record in result]
        df = pd.DataFrame([record.data() for record in result])
        return df


result = create_dim_account(driver)
result.to_parquet("TableParquet/dim_account.parquet", index=False)
print(f"Written {len(result)} rows")
#result = get_transaction_types(driver)
# for row in result:
#     print(row)
