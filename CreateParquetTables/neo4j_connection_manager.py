import os
from neo4j import GraphDatabase
from dotenv import load_dotenv

load_dotenv()

#Add .ENV file with NEO4J_PASSWORD=your_password

def get_connection():
    URI = "bolt://localhost:7687" 
    AUTH = ("neo4j", os.getenv("NEO4J_PASSWORD"))
    return GraphDatabase.driver(URI, auth=AUTH)
   


