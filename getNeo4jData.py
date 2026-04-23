from neo4j import GraphDatabase
import os
from dotenv import load_dotenv

load_dotenv()

#Add .ENV file with NEO4J_PASSWORD=your_password
URI = "bolt://localhost:7687" 
AUTH = ("neo4j", os.getenv("NEO4J_PASSWORD"))
driver = GraphDatabase.driver(URI, auth=AUTH)

driver.verify_connectivity()
print("Connected!")