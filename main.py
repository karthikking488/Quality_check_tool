import snowflake.connector
from snowflake.connector import DictCursor
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def create_snowflake_connection():
    """
    Create and return a connection to Snowflake using SSO authentication
    
    Required environment variables:
    - SNOWFLAKE_ACCOUNT: Account identifier (e.g., VOYA-VOYAIMTEST)
    - SNOWFLAKE_USER: Your Voya email/username
    - SNOWFLAKE_WAREHOUSE: Warehouse name
    - SNOWFLAKE_DATABASE: Database name
    - SNOWFLAKE_SCHEMA: Schema name
    - SNOWFLAKE_ROLE: Role name
    
    This will open a browser window for SSO authentication.
    """
    try:
        # Get connection parameters from environment variables
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE')
        schema = os.getenv('SNOWFLAKE_SCHEMA')
        role = os.getenv('SNOWFLAKE_ROLE')
        
        # Validate required parameters
        if not all([account, user, warehouse, database, schema, role]):
            missing = [var for var, val in {
                'SNOWFLAKE_ACCOUNT': account,
                'SNOWFLAKE_USER': user,
                'SNOWFLAKE_WAREHOUSE': warehouse,
                'SNOWFLAKE_DATABASE': database,
                'SNOWFLAKE_SCHEMA': schema,
                'SNOWFLAKE_ROLE': role
            }.items() if not val]
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
        
        print("Opening browser for SSO authentication...")
        
        # Create connection using SSO authentication
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            authenticator='externalbrowser',
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role
        )
        
        print("Successfully connected to Snowflake using SSO!")
        print(f"Connected to: {account}")
        # print(f"User: {user}")
        # print(f"Warehouse: {warehouse}")
        # print(f"Database: {database}")
        # print(f"Schema: {schema}")
        # print(f"Role: {role}")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        return None

def run_query(conn, query):
    """
    Execute a SQL query and return the results
    """
    try:
        cursor = conn.cursor(DictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        return None

def main():
    # Create connection
    conn = create_snowflake_connection()
    
    if conn:
        # Your custom query here
        # Example: Replace 'YOUR_TABLE_NAME' with your actual table name
        query = """
        SELECT * 
        FROM DIM_CUSTOMER 
        LIMIT 10
        """
        
        print("\nExecuting query...")
        results = run_query(conn, query)
        
        if results:
            print(f"\nQuery returned {len(results)} rows:")
            for row in results:
                print(row)
        
        # Close connection when done
        conn.close()
        print("\nConnection closed.")

if __name__ == "__main__":
    main()
