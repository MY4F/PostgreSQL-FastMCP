from fastmcp import FastMCP
import logging 
import psycopg
import urllib.parse
import os
from dotenv import load_dotenv
load_dotenv()
# Load environment variables and configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Initialize MCP server
mcp = FastMCP("postgresql-server")

def get_connection_uri():
    dbhost = os.environ['DBHOST']
    dbname = os.environ['DBNAME']
    dbuser = urllib.parse.quote(os.environ['DBUSER'])
    print(dbuser, urllib.parse.quote(os.environ['DBPASSWORD']))
    password = os.environ['DBPASSWORD']
    sslmode = os.environ['SSLMODE']
    db_uri = f"host={dbhost} dbname={dbname} user={dbuser} password={password} sslmode={sslmode}"
    return db_uri

@mcp.tool(description="Retrieve a detailed overview of all tables and their columns in the connected PostgreSQL database. This tool returns the schema, including table names and each column's data type, to help users understand the database structure for query building, data exploration, or integration tasks.")
def get_postgres_schema() -> str:
    """Retrieve schema of PostgreSQL database."""
    try:
        conn_string = get_connection_uri()
        conn = psycopg.connect(conn_string)
        cursor = conn.cursor()
        logger.info(f"Connected successfully")
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        tables = cursor.fetchall()
        schema = ""
        for table in tables:
            table_name = table[0]
            cursor.execute(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = '{table_name}';
            """)
            columns = cursor.fetchall()
            schema += f"Table: {table_name}\n"
            for column in columns:
                schema += f"  - {column[0]} ({column[1]})\n"
            schema += "\n"
        cursor.close()
        conn.close()
        return schema
    except Exception as e:
        logger.error(f"Error retrieving schema: {e}")
        return f"Error retrieving schema: {e}"

@mcp.tool(description="Execute a raw SQL SELECT query against the connected PostgreSQL database and return the results in a readable format. This tool is ideal for data analysis, reporting, and ad-hoc data exploration, allowing users to retrieve specific data by writing custom SELECT statements. Only SELECT queries are supported; modification or DDL queries are not permitted.")
def execute_postgres_query(query: str) -> str:
    """Execute a raw SQL SELECT query and return result as string."""
    try:
        conn_string = get_connection_uri()
        conn = psycopg.connect(conn_string)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        colnames = [desc[0] for desc in cursor.description]
        formatted = "\n".join([", ".join(str(cell) for cell in row) for row in rows])
        result = f"Columns: {', '.join(colnames)}\n{formatted}"
        cursor.close()
        conn.close()
        return result if rows else "No rows found."
    except Exception as e:
        logger.error(f"Query execution error: {e}")
        return f"Query error: {e}"

if __name__ == "__main__":
    mcp.run()
