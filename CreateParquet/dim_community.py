import pandas as pd
import neo4j_connection_manager

driver = neo4j_connection_manager.get_connection()
def create_dim_community():
    query = """
        MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
        WHERE a.community_id = b.community_id
        RETURN a.community_id AS community_id,
            count(DISTINCT a) AS member_count,
            sum(t.amount) AS total_internal_volume

        LIMIT 100
    """
    with driver.session() as session:
        result = session.run(query)
        return [record.data() for record in result]
        # df = pd.DataFrame([record.data() for record in result])
        # return df


# result = create_dim_account()
# result.to_parquet("TableParquet/dim_account.parquet", index=False)
# print(f"Written {len(result)} rows")
result = create_dim_community()
for row in result:
    print(row)
