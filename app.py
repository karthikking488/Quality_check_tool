from flask import Flask, render_template, request, jsonify, send_file
import snowflake.connector
from snowflake.connector import DictCursor
import os
import re
from dotenv import load_dotenv
import atexit
from report_generator import generate_pdf_report

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Global connection object that persists for the lifetime of the application
global_connection = None

def create_snowflake_connection():
    """
    Create and return a connection to Snowflake using SSO authentication
    Connects at account level - database and schema can be selected dynamically
    """
    try:
        account = os.getenv('SNOWFLAKE_ACCOUNT')
        user = os.getenv('SNOWFLAKE_USER')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE')
        database = os.getenv('SNOWFLAKE_DATABASE', '')  # Optional
        schema = os.getenv('SNOWFLAKE_SCHEMA', '')      # Optional
        role = os.getenv('SNOWFLAKE_ROLE')
        
        print("Opening browser for SSO authentication...")
        print("Please authenticate in your browser...")
        
        # Connect without database/schema if not specified
        conn_params = {
            'account': account,
            'user': user,
            'authenticator': 'externalbrowser',
            'warehouse': warehouse,
            'role': role
        }
        
        # Only add database/schema if they are provided
        if database:
            conn_params['database'] = database
        if schema:
            conn_params['schema'] = schema
        
        conn = snowflake.connector.connect(**conn_params)
        
        print("✅ Successfully connected to Snowflake!")
        print(f"Connected to: {account}")
        if database:
            print(f"Database: {database}" + (f".{schema}" if schema else ""))
        else:
            print("Database: Not selected (dynamic selection enabled)")
        print(f"Warehouse: {warehouse}")
        print("-" * 50)
        
        return conn
    except Exception as e:
        print(f"❌ Error connecting to Snowflake: {str(e)}")
        return None

def get_connection():
    """
    Get the global connection, creating it if necessary
    """
    global global_connection
    
    if global_connection is None:
        global_connection = create_snowflake_connection()
    
    # Check if connection is still alive
    try:
        if global_connection:
            global_connection.cursor().execute("SELECT 1")
    except:
        print("Connection lost, reconnecting...")
        global_connection = create_snowflake_connection()
    
    return global_connection

def close_connection():
    """
    Close the global connection when the application shuts down
    """
    global global_connection
    if global_connection:
        try:
            global_connection.close()
            print("\n✅ Snowflake connection closed successfully")
        except:
            pass

# Register the cleanup function to run when the app shuts down
atexit.register(close_connection)

def execute_query(query):
    """
    Execute a SQL query and return the results using the persistent connection
    """
    try:
        conn = get_connection()
        if not conn:
            return {"error": "Failed to connect to Snowflake"}
        
        cursor = conn.cursor(DictCursor)
        cursor.execute(query)
        
        # Check if query returns results (SELECT, SHOW, DESCRIBE, etc.)
        # USE DATABASE/SCHEMA commands don't return result sets
        if cursor.description is None:
            cursor.close()
            return {
                "success": True,
                "columns": [],
                "data": [],
                "row_count": 0,
                "message": "Command executed successfully"
            }
        
        # Get column names
        columns = [desc[0] for desc in cursor.description]
        
        # Get all results
        results = cursor.fetchall()
        
        # Convert results to list of dictionaries
        data = []
        for row in results:
            data.append(dict(row))
        
        cursor.close()
        
        return {
            "success": True,
            "columns": columns,
            "data": data,
            "row_count": len(data)
        }
    except Exception as e:
        return {"error": str(e)}

def extract_referenced_objects(procedure_definition, schema):
    """
    Extract table and view names referenced in a procedure definition
    Returns a dict of {object_name: 'TABLE' or 'VIEW'}
    """
    import re
    
    referenced_objects = {}
    
    # Common SQL patterns for table/view references
    patterns = [
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # FROM schema.table
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # JOIN schema.table
        r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)',  # INSERT INTO schema.table
        r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)', # UPDATE schema.table
        r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # FROM table (same schema)
        r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # JOIN table (same schema)
        r'\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # INSERT INTO table (same schema)
        r'\bUPDATE\s+([a-zA-Z_][a-zA-Z0-9_]*)', # UPDATE table (same schema)
    ]
    
    for pattern in patterns:
        matches = re.findall(pattern, procedure_definition, re.IGNORECASE)
        for match in matches:
            # Skip if it's a common SQL keyword or function
            if match.upper() in ['SELECT', 'WHERE', 'AND', 'OR', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END']:
                continue
            
            # Extract just the table name (remove schema prefix if present)
            if '.' in match:
                obj_schema, obj_name = match.split('.')
                # Only include if it's the same schema we're working in
                if obj_schema.upper() == schema.upper():
                    referenced_objects[obj_name] = 'TABLE'  # We'll verify type later
            else:
                referenced_objects[match] = 'TABLE'
    
    return referenced_objects

def get_object_quick_metadata(full_schema, object_name, object_type):
    """
    Get quick metadata for a referenced table/view (columns, sample data, row count)
    full_schema should be in format 'database.schema'
    """
    metadata = {}
    full_name = f"{full_schema}.{object_name}"
    
    try:
        # Get column info
        desc_query = f"DESCRIBE TABLE {full_name}"
        desc_result = execute_query(desc_query)
        if "error" not in desc_result:
            metadata['columns'] = desc_result['data'][:10]  # First 10 columns
        
        # Get row count
        count_query = f"SELECT COUNT(*) as total_rows FROM {full_name}"
        count_result = execute_query(count_query)
        if "error" not in count_result:
            metadata['total_rows'] = count_result['data'][0]['TOTAL_ROWS']
        
        # Get sample data (just 3 rows)
        sample_query = f"SELECT * FROM {full_name} LIMIT 3"
        sample_result = execute_query(sample_query)
        if "error" not in sample_result:
            metadata['sample_data'] = sample_result['data']
    except:
        pass
    
    return metadata

@app.route('/')
def home():
    """
    Render the home page with SSO login and role selection
    """
    return render_template('home.html')

@app.route('/query-tool')
def index():
    """
    Render the main query tool page
    """
    return render_template('index.html')

@app.route('/ai-query')
def ai_query():
    """
    Render the AI query assistant page
    """
    return render_template('ai_query.html')

@app.route('/unit-test-generator')
def unit_test_generator():
    """
    Render the unit test generator page
    """
    return render_template('unit_test_generator.html')

# ============== NEW ENDPOINTS FOR AUTH & TEST STORAGE ==============

@app.route('/api/login-sso', methods=['POST'])
def login_sso():
    """
    Initiate SSO login and establish Snowflake connection
    """
    global global_connection
    try:
        # Force create a new connection for SSO
        print("\n🔐 Initiating SSO login...")
        global_connection = create_snowflake_connection()
        
        if global_connection is None:
            return jsonify({"error": "Failed to establish Snowflake connection"})
        
        # Get current user
        cursor = global_connection.cursor(DictCursor)
        cursor.execute("SELECT CURRENT_USER() as username")
        result = cursor.fetchone()
        username = result['USERNAME'] if result else 'Unknown'
        cursor.close()
        
        print(f"✅ SSO login successful for user: {username}")
        return jsonify({"success": True, "user": username})
    except Exception as e:
        print(f"❌ SSO login failed: {str(e)}")
        return jsonify({"error": str(e)})

@app.route('/api/check-connection', methods=['GET'])
def check_connection():
    """
    Check if there is an active Snowflake connection
    """
    global global_connection
    if global_connection is None:
        return jsonify({"connected": False})
    
    try:
        # Test the connection
        cursor = global_connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return jsonify({"connected": True})
    except:
        return jsonify({"connected": False})

@app.route('/api/roles', methods=['GET'])
def get_roles():
    """
    Get available roles for the current user
    """
    try:
        conn = get_connection()
        if conn is None:
            return jsonify({"error": "Not connected to Snowflake"})
        
        cursor = conn.cursor(DictCursor)
        
        # Get current role
        cursor.execute("SELECT CURRENT_ROLE() as current_role")
        current_role_result = cursor.fetchone()
        current_role = current_role_result['CURRENT_ROLE'] if current_role_result else None
        
        # Get all available roles for the user
        cursor.execute("SHOW ROLES")
        roles_result = cursor.fetchall()
        
        roles = []
        for row in roles_result:
            # SHOW ROLES returns 'name' column
            role_name = row.get('name', row.get('NAME', ''))
            if role_name:
                roles.append(role_name)
        
        cursor.close()
        
        return jsonify({
            "roles": sorted(roles),
            "current_role": current_role
        })
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/set-role', methods=['POST'])
def set_role():
    """
    Switch to a different role
    """
    try:
        data = request.get_json()
        role = data.get('role')
        
        if not role:
            return jsonify({"error": "Role is required"})
        
        conn = get_connection()
        if conn is None:
            return jsonify({"error": "Not connected to Snowflake"})
        
        cursor = conn.cursor()
        cursor.execute(f"USE ROLE {role}")
        cursor.close()
        
        return jsonify({"success": True, "role": role})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/check-cortex', methods=['GET'])
def check_cortex():
    """
    Check if the current role has Cortex AI access
    """
    try:
        conn = get_connection()
        if conn is None:
            return jsonify({"error": "Not connected to Snowflake", "has_access": False})
        
        cursor = conn.cursor()
        
        # Ensure warehouse is active
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'WH_CORTEX_POC_READ')
        try:
            cursor.execute(f"USE WAREHOUSE {warehouse}")
        except:
            pass
        
        # Try a simple Cortex call
        try:
            cursor.execute("SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', 'Say hello') as test")
            result = cursor.fetchone()
            cursor.close()
            return jsonify({"has_access": True})
        except Exception as cortex_error:
            cursor.close()
            error_msg = str(cortex_error).lower()
            # Check if it's a permission/access error
            if 'not authorized' in error_msg or 'access' in error_msg or 'permission' in error_msg or 'not found' in error_msg:
                return jsonify({"has_access": False, "reason": str(cortex_error)})
            # For other errors (like syntax), assume access exists
            return jsonify({"has_access": False, "reason": str(cortex_error)})
            
    except Exception as e:
        return jsonify({"error": str(e), "has_access": False})

@app.route('/api/save-tests', methods=['POST'])
def save_tests():
    """
    Save generated test cases to the Snowflake table
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        object_name = data.get('object_name')
        object_type = data.get('object_type')
        test_cases = data.get('test_cases', [])
        
        if not all([database, schema, object_name, object_type, test_cases]):
            return jsonify({"error": "Missing required parameters"})
        
        conn = get_connection()
        if conn is None:
            return jsonify({"error": "Not connected to Snowflake"})
        
        full_schema = f"{database}.{schema}"
        cursor = conn.cursor()
        
        # Insert each test case
        saved_count = 0
        for test in test_cases:
            test_name = test.get('test_name', '').replace("'", "''")
            test_desc = test.get('description', '').replace("'", "''")
            test_query = test.get('query', '').replace("'", "''")
            expected_result = test.get('expected_type', '').replace("'", "''")
            
            insert_query = f"""
            INSERT INTO HACKATHON_POC.NN.DATA_QUALITY_TEST_CASES 
            (SCHEMA_NAME, OBJECT_TYPE, OBJECT_NAME, TEST_NAME, TEST_DESCRIPTION, TEST_QUERY, EXPECTED_RESULT)
            VALUES ('{full_schema}', '{object_type}', '{object_name}', '{test_name}', '{test_desc}', '{test_query}', '{expected_result}')
            """
            
            try:
                cursor.execute(insert_query)
                saved_count += 1
            except Exception as insert_error:
                print(f"Error inserting test case: {insert_error}")
        
        cursor.close()
        
        return jsonify({"success": True, "saved_count": saved_count})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/fetch-tests', methods=['POST'])
def fetch_tests():
    """
    Fetch existing test cases for an object from the Snowflake table
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        object_name = data.get('object_name')
        object_type = data.get('object_type')
        
        if not all([database, schema, object_name, object_type]):
            return jsonify({"error": "Missing required parameters"})
        
        conn = get_connection()
        if conn is None:
            return jsonify({"error": "Not connected to Snowflake"})
        
        full_schema = f"{database}.{schema}"
        cursor = conn.cursor(DictCursor)
        
        fetch_query = f"""
        SELECT TEST_ID, TEST_NAME, TEST_DESCRIPTION, TEST_QUERY, EXPECTED_RESULT, CREATED_AT
        FROM HACKATHON_POC.NN.DATA_QUALITY_TEST_CASES
        WHERE SCHEMA_NAME = '{full_schema}'
          AND OBJECT_TYPE = '{object_type}'
          AND OBJECT_NAME = '{object_name}'
        ORDER BY CREATED_AT DESC
        """
        
        cursor.execute(fetch_query)
        results = cursor.fetchall()
        cursor.close()
        
        # Convert to test case format
        test_cases = []
        for row in results:
            test_cases.append({
                "test_id": row.get('TEST_ID'),
                "test_name": row.get('TEST_NAME'),
                "description": row.get('TEST_DESCRIPTION'),
                "query": row.get('TEST_QUERY'),
                "expected_type": row.get('EXPECTED_RESULT'),
                "expected_description": row.get('TEST_DESCRIPTION'),
                "created_at": str(row.get('CREATED_AT')) if row.get('CREATED_AT') else None
            })
        
        return jsonify({"success": True, "test_cases": test_cases})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/delete-test', methods=['POST'])
def delete_test():
    """
    Delete a test case from the Snowflake table
    """
    try:
        data = request.get_json()
        test_id = data.get('test_id')
        
        if not test_id:
            return jsonify({"error": "Test ID is required"})
        
        conn = get_connection()
        if conn is None:
            return jsonify({"error": "Not connected to Snowflake"})
        
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM HACKATHON_POC.NN.DATA_QUALITY_TEST_CASES WHERE TEST_ID = {test_id}")
        cursor.close()
        
        return jsonify({"success": True})
    except Exception as e:
        return jsonify({"error": str(e)})

# ============== END NEW ENDPOINTS ==============

@app.route('/execute', methods=['POST'])
def execute():
    """
    Execute the SQL query from the frontend
    """
    data = request.get_json()
    query = data.get('query', '').strip()
    
    if not query:
        return jsonify({"error": "Query cannot be empty"})
    
    result = execute_query(query)
    return jsonify(result)

@app.route('/api/databases', methods=['GET'])
def get_databases():
    """
    Get list of all databases available in the Snowflake account
    """
    try:
        query = "SHOW DATABASES"
        result = execute_query(query)
        if "error" in result:
            return jsonify({"error": result["error"]})
        
        databases = [row['name'] for row in result['data']]
        return jsonify({"databases": databases})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/schemas', methods=['POST'])
def get_schemas():
    """
    Get list of all schemas in the specified database
    """
    try:
        data = request.get_json()
        database = data.get('database')
        
        if not database:
            return jsonify({"error": "Database is required"})
        
        # Switch to the database context
        query = f"SHOW SCHEMAS IN DATABASE {database}"
        result = execute_query(query)
        if "error" in result:
            return jsonify({"error": result["error"]})
        
        schemas = [row['name'] for row in result['data']]
        return jsonify({"schemas": schemas})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/objects', methods=['POST'])
def get_objects():
    """
    Get list of objects (tables, views, procedures, functions) in a schema
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        object_type = data.get('object_type')
        
        if not database or not schema or not object_type:
            return jsonify({"error": "Database, schema, and object_type are required"})
        
        # Use fully qualified schema name
        full_schema = f"{database}.{schema}"
        
        if object_type == 'TABLE':
            query = f"SHOW TABLES IN SCHEMA {full_schema}"
        elif object_type == 'VIEW':
            query = f"SHOW VIEWS IN SCHEMA {full_schema}"
        elif object_type == 'PROCEDURE':
            query = f"SHOW PROCEDURES IN SCHEMA {full_schema}"
        elif object_type == 'FUNCTION':
            query = f"SHOW FUNCTIONS IN SCHEMA {full_schema}"
        else:
            return jsonify({"error": "Invalid object type"})
        
        result = execute_query(query)
        if "error" in result:
            return jsonify({"error": result["error"]})
        
        objects = [row['name'] for row in result['data']]
        return jsonify({"objects": objects})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/object-metadata', methods=['POST'])
def get_object_metadata():
    """
    Get metadata for a specific object
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        object_name = data.get('object_name')
        object_type = data.get('object_type')
        
        if not all([database, schema, object_name, object_type]):
            return jsonify({"error": "Database, schema, object_name, and object_type are required"})
        
        # Build fully qualified name
        full_name = f"{database}.{schema}.{object_name}"
        
        # Set database context first
        use_db_query = f"USE DATABASE {database}"
        execute_query(use_db_query)
        
        metadata = {}
        
        if object_type == 'TABLE':
            # Get table structure
            query = f"DESCRIBE TABLE {full_name}"
            result = execute_query(query)
            if "error" not in result:
                metadata['columns'] = result['data']
            
            # Get row count - CRITICAL for AI to know scale
            count_query = f"SELECT COUNT(*) as total_rows FROM {full_name}"
            count_result = execute_query(count_query)
            if "error" not in count_result:
                metadata['total_rows'] = count_result['data'][0]['TOTAL_ROWS']
            
            # Get sample data
            sample_query = f"SELECT * FROM {full_name} LIMIT 5"
            sample_result = execute_query(sample_query)
            if "error" not in sample_result:
                metadata['sample_data'] = sample_result['data']
            
            # Get min/max for key numeric columns (first 3 numeric columns)
            if 'columns' in metadata:
                numeric_cols = [col for col in metadata['columns'] 
                               if 'NUMBER' in col.get('type', '').upper() or 
                                  'INT' in col.get('type', '').upper() or
                                  'FLOAT' in col.get('type', '').upper() or
                                  'DECIMAL' in col.get('type', '').upper()][:3]
                
                metadata['statistics'] = {}
                for col in numeric_cols:
                    col_name = col['name']
                    stat_query = f"""
                    SELECT 
                        MIN({col_name}) as min_val,
                        MAX({col_name}) as max_val,
                        AVG({col_name}) as avg_val
                    FROM {full_name}
                    """
                    stat_result = execute_query(stat_query)
                    if "error" not in stat_result and len(stat_result['data']) > 0:
                        metadata['statistics'][col_name] = stat_result['data'][0]
                
                # Get distinct values for VARCHAR/STRING columns (likely categorical)
                # This helps AI understand valid values for enum-like columns
                string_cols = [col for col in metadata['columns'] 
                              if 'VARCHAR' in col.get('type', '').upper() or 
                                 'STRING' in col.get('type', '').upper() or
                                 'TEXT' in col.get('type', '').upper() or
                                 'CHAR' in col.get('type', '').upper()]
                
                metadata['distinct_values'] = {}
                for col in string_cols[:5]:  # Check first 5 string columns
                    col_name = col['name']
                    # First check how many distinct values exist
                    count_distinct_query = f"SELECT COUNT(DISTINCT {col_name}) as cnt FROM {full_name}"
                    count_distinct_result = execute_query(count_distinct_query)
                    
                    if "error" not in count_distinct_result and len(count_distinct_result['data']) > 0:
                        distinct_count = count_distinct_result['data'][0]['CNT']
                        
                        # Only fetch distinct values if there are 50 or fewer (categorical column)
                        # If more than 50, it's likely not an enum/category column
                        if distinct_count and distinct_count <= 50:
                            distinct_query = f"SELECT DISTINCT {col_name} as val FROM {full_name} WHERE {col_name} IS NOT NULL ORDER BY {col_name} LIMIT 50"
                            distinct_result = execute_query(distinct_query)
                            if "error" not in distinct_result and len(distinct_result['data']) > 0:
                                values = [row['VAL'] for row in distinct_result['data'] if row['VAL'] is not None]
                                metadata['distinct_values'][col_name] = {
                                    'count': distinct_count,
                                    'values': values
                                }
                        else:
                            # Too many distinct values - note this for AI
                            metadata['distinct_values'][col_name] = {
                                'count': distinct_count,
                                'values': f"(Too many distinct values: {distinct_count})"
                            }
            
        elif object_type == 'VIEW':
                # Get view structure
                try:
                    query = f"DESCRIBE VIEW {full_name}"
                    result = execute_query(query)
                    if "error" not in result:
                        metadata['columns'] = result['data']
                    else:
                        metadata['columns'] = []
                except Exception:
                    metadata['columns'] = []

                # Get row count for views too
                try:
                    count_query = f"SELECT COUNT(*) as total_rows FROM {full_name}"
                    count_result = execute_query(count_query)
                    if "error" not in count_result:
                        metadata['total_rows'] = count_result['data'][0]['TOTAL_ROWS']
                    else:
                        metadata['total_rows'] = None
                except Exception:
                    metadata['total_rows'] = None

                # Get view definition
                try:
                    ddl_query = f"SELECT GET_DDL('VIEW', '{full_name}') as ddl"
                    ddl_result = execute_query(ddl_query)
                    if "error" not in ddl_result:
                        metadata['definition'] = ddl_result['data'][0]['DDL']
                    else:
                        metadata['definition'] = None
                except Exception:
                    metadata['definition'] = None

                # Get sample data
                try:
                    sample_query = f"SELECT * FROM {full_name} LIMIT 5"
                    sample_result = execute_query(sample_query)
                    if "error" not in sample_result:
                        metadata['sample_data'] = sample_result['data']
                    else:
                        metadata['sample_data'] = []
                except Exception:
                    metadata['sample_data'] = []

                # Parse base tables from view DDL so we can enrich categorical values
                base_tables = []
                definition = metadata.get('definition') or ''
                if definition:
                    base_table_patterns = [
                        r'\b(?:FROM|JOIN)\s+([A-Za-z0-9_"$]+\.[A-Za-z0-9_"$]+\.[A-Za-z0-9_"$]+)',
                        r'\b(?:FROM|JOIN)\s+([A-Za-z0-9_"$]+\.[A-Za-z0-9_"$]+)'
                    ]

                    for pattern in base_table_patterns:
                        matches = re.findall(pattern, definition, re.IGNORECASE)
                        for match in matches:
                            tbl = match.replace('"', '')
                            parts = tbl.split('.')
                            if len(parts) == 3:
                                normalized = f"{parts[0].upper()}.{parts[1].upper()}.{parts[2].upper()}"
                            elif len(parts) == 2:
                                normalized = f"{database.upper()}.{parts[0].upper()}.{parts[1].upper()}"
                            else:
                                continue

                            if normalized not in base_tables:
                                base_tables.append(normalized)

                metadata['base_tables'] = base_tables

                if metadata.get('columns'):
                    # Get min/max/avg statistics for key numeric columns from the view
                    numeric_cols = [col for col in metadata['columns']
                                   if 'NUMBER' in col.get('type', '').upper() or
                                      'INT' in col.get('type', '').upper() or
                                      'FLOAT' in col.get('type', '').upper() or
                                      'DECIMAL' in col.get('type', '').upper()][:3]

                    metadata['statistics'] = {}
                    for col in numeric_cols:
                        col_name = col['name']
                        stat_query = f"""
                        SELECT
                            MIN({col_name}) as min_val,
                            MAX({col_name}) as max_val,
                            AVG({col_name}) as avg_val
                        FROM {full_name}
                        """
                        stat_result = execute_query(stat_query)
                        if "error" not in stat_result and len(stat_result['data']) > 0:
                            metadata['statistics'][col_name] = stat_result['data'][0]

                    # Get distinct values for categorical columns.
                    # Prefer base table values when the column exists in a source table.
                    string_cols = [col for col in metadata['columns']
                                  if 'VARCHAR' in col.get('type', '').upper() or
                                     'STRING' in col.get('type', '').upper() or
                                     'TEXT' in col.get('type', '').upper() or
                                     'CHAR' in col.get('type', '').upper()]

                    metadata['distinct_values'] = {}
                    for col in string_cols[:8]:
                        col_name = col['name']
                        value_source = 'view'
                        source_object = full_name
                        distinct_count = None
                        values = None

                        # Try to source complete values from base table first.
                        for base_table in base_tables:
                            parts = base_table.split('.')
                            if len(parts) != 3:
                                continue

                            src_db, src_schema, src_table = parts
                            col_exists_query = f"""
                            SELECT 1 as found
                            FROM {src_db}.INFORMATION_SCHEMA.COLUMNS
                            WHERE TABLE_SCHEMA = '{src_schema}'
                              AND TABLE_NAME = '{src_table}'
                              AND UPPER(COLUMN_NAME) = UPPER('{col_name}')
                            LIMIT 1
                            """
                            col_exists_result = execute_query(col_exists_query)
                            if "error" in col_exists_result or not col_exists_result.get('data'):
                                continue

                            source_object = f"{src_db}.{src_schema}.{src_table}"
                            value_source = 'base_table'

                            source_count_query = f"SELECT COUNT(DISTINCT {col_name}) as cnt FROM {source_object}"
                            source_count_result = execute_query(source_count_query)
                            if "error" not in source_count_result and source_count_result.get('data'):
                                distinct_count = source_count_result['data'][0]['CNT']

                                # Keep explicit value list for categorical columns with manageable cardinality.
                                if distinct_count is not None and distinct_count <= 200:
                                    source_values_query = f"SELECT DISTINCT {col_name} as val FROM {source_object} WHERE {col_name} IS NOT NULL ORDER BY {col_name}"
                                    source_values_result = execute_query(source_values_query)
                                    if "error" not in source_values_result and source_values_result.get('data'):
                                        values = [row['VAL'] for row in source_values_result['data'] if row['VAL'] is not None]
                                break

                        # Fallback to view-level distinct values if base-table enrichment is not available.
                        if distinct_count is None:
                            view_count_query = f"SELECT COUNT(DISTINCT {col_name}) as cnt FROM {full_name}"
                            view_count_result = execute_query(view_count_query)
                            if "error" not in view_count_result and view_count_result.get('data'):
                                distinct_count = view_count_result['data'][0]['CNT']
                                if distinct_count is not None and distinct_count <= 200:
                                    view_values_query = f"SELECT DISTINCT {col_name} as val FROM {full_name} WHERE {col_name} IS NOT NULL ORDER BY {col_name}"
                                    view_values_result = execute_query(view_values_query)
                                    if "error" not in view_values_result and view_values_result.get('data'):
                                        values = [row['VAL'] for row in view_values_result['data'] if row['VAL'] is not None]

                        if distinct_count is not None:
                            if values is None:
                                values = f"(Too many distinct values: {distinct_count})"
                            metadata['distinct_values'][col_name] = {
                                'count': distinct_count,
                                'values': values,
                                'source': value_source,
                                'source_object': source_object
                            }
                
        elif object_type == 'PROCEDURE':
            # For procedures, we need to handle them differently
            # First try to get procedure details to understand signature
            desc_query = f"DESCRIBE PROCEDURE {full_name}"
            desc_result = execute_query(desc_query)
            if "error" not in desc_result and len(desc_result.get('data', [])) > 0:
                metadata['arguments'] = desc_result['data']
                
                # Try to get procedure definition with full signature
                try:
                    ddl_query = f"SELECT GET_DDL('PROCEDURE', '{full_name}') as ddl"
                    ddl_result = execute_query(ddl_query)
                    if "error" not in ddl_result:
                        procedure_definition = ddl_result['data'][0]['DDL']
                        metadata['definition'] = procedure_definition
                        
                        # Extract table/view names from the procedure definition
                        full_schema = f"{database}.{schema}"
                        referenced_objects = extract_referenced_objects(procedure_definition, schema)
                        metadata['referenced_objects'] = referenced_objects
                        
                        # Get metadata for each referenced table/view
                        metadata['referenced_metadata'] = {}
                        for obj_name, obj_type in referenced_objects.items():
                            try:
                                obj_meta = get_object_quick_metadata(full_schema, obj_name, obj_type)
                                if obj_meta:
                                    metadata['referenced_metadata'][obj_name] = obj_meta
                            except:
                                pass
                except:
                    pass
            else:
                # If DESCRIBE fails, try to get basic info from SHOW PROCEDURES
                full_schema = f"{database}.{schema}"
                show_query = f"SHOW PROCEDURES LIKE '{object_name}' IN SCHEMA {full_schema}"
                show_result = execute_query(show_query)
                if "error" not in show_result and len(show_result.get('data', [])) > 0:
                    proc_info = show_result['data'][0]
                    metadata['procedure_info'] = proc_info
                    
                    # Try to get DDL with the full signature from SHOW result
                    if 'arguments' in proc_info:
                        full_proc_name = f"{database}.{schema}.{object_name}{proc_info['arguments']}"
                        ddl_query = f"SELECT GET_DDL('PROCEDURE', '{full_proc_name}') as ddl"
                        ddl_result = execute_query(ddl_query)
                        if "error" not in ddl_result:
                            metadata['definition'] = ddl_result['data'][0]['DDL']
                
        elif object_type == 'FUNCTION':
            # Similar handling for functions
            desc_query = f"DESCRIBE FUNCTION {full_name}"
            desc_result = execute_query(desc_query)
            if "error" not in desc_result and len(desc_result.get('data', [])) > 0:
                metadata['arguments'] = desc_result['data']
                
                try:
                    ddl_query = f"SELECT GET_DDL('FUNCTION', '{full_name}') as ddl"
                    ddl_result = execute_query(ddl_query)
                    if "error" not in ddl_result:
                        metadata['definition'] = ddl_result['data'][0]['DDL']
                except:
                    pass
            else:
                # Fallback to SHOW FUNCTIONS
                full_schema = f"{database}.{schema}"
                show_query = f"SHOW FUNCTIONS LIKE '{object_name}' IN SCHEMA {full_schema}"
                show_result = execute_query(show_query)
                if "error" not in show_result and len(show_result.get('data', [])) > 0:
                    metadata['function_info'] = show_result['data'][0]
        
        return jsonify({"metadata": metadata})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/generate-tests', methods=['POST'])
def generate_tests():
    """
    Generate unit tests using Snowflake Cortex AI
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        object_name = data.get('object_name')
        object_type = data.get('object_type')
        metadata = data.get('metadata')
        user_test_request = data.get('user_test_request', '')  # User's custom test request
        test_case_count = data.get('test_case_count', 5)  # Number of test cases to generate
        
        if not all([database, schema, object_name, object_type, metadata]):
            return jsonify({"error": "Missing required parameters (database, schema, object_name, object_type, metadata)"})
        
        # Build fully qualified schema name
        full_schema = f"{database}.{schema}"
        
        # Set database context
        use_db_query = f"USE DATABASE {database}"
        execute_query(use_db_query)
        
        # Fetch ALL tables in the schema so AI knows correct table names
        all_tables_query = f"""
        SELECT TABLE_NAME 
        FROM {database}.INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema}' 
        ORDER BY TABLE_NAME
        """
        all_tables_result = execute_query(all_tables_query)
        schema_tables = []
        if "data" in all_tables_result:
            schema_tables = [row.get('TABLE_NAME', row.get('table_name', '')) for row in all_tables_result['data']]
        
        # Add schema tables to metadata so AI knows all available tables
        metadata['schema_tables'] = schema_tables
        
        # Create prompt for Cortex AI - pass full_schema instead of just schema
        # Also pass user's custom request and test case count
        prompt = create_test_generation_prompt(full_schema, object_name, object_type, metadata, user_test_request, test_case_count)
        
        # Call Cortex AI to generate test cases
        # Using mixtral-8x7b (more commonly available than mistral-large2)
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mixtral-8x7b',
            '{prompt}'
        ) as test_cases
        """
        
        result = execute_query(cortex_query)
        
        if "error" in result:
            return jsonify({"error": result["error"]})
        
        # Parse the generated test cases
        test_cases_text = result['data'][0]['TEST_CASES']
        test_cases = parse_test_cases(test_cases_text, schema, object_name, object_type)
        
        return jsonify({"test_cases": test_cases})
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/run-test', methods=['POST'])
def run_test():
    """
    Execute a specific test case
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        test_query = data.get('test_query')
        expected_type = data.get('expected_type', 'HAS_ROWS')
        expected_description = data.get('expected_description', '')
        
        if not test_query:
            return jsonify({"error": "Test query is required"})
        
        # Set database context if provided
        if database:
            use_db_query = f"USE DATABASE {database}"
            execute_query(use_db_query)
            
            if schema:
                use_schema_query = f"USE SCHEMA {database}.{schema}"
                execute_query(use_schema_query)
        
        # Execute the test query
        result = execute_query(test_query)
        
        # If we expected an error but got results, test fails
        if expected_type == "ERROR" and "error" not in result:
            return jsonify({
                "status": "FAILED",
                "actual_result": result['data'],
                "expected_type": expected_type,
                "expected_description": expected_description,
                "passed": False,
                "message": "Expected query to fail, but it succeeded"
            })
        
        # If query failed but we didn't expect error
        if "error" in result:
            if expected_type == "ERROR":
                # Expected error and got error - PASS
                return jsonify({
                    "status": "PASSED",
                    "error": result["error"],
                    "actual_result": None,
                    "expected_type": expected_type,
                    "expected_description": expected_description,
                    "passed": True,
                    "message": "Query failed as expected"
                })
            else:
                # Didn't expect error but got one - FAIL
                return jsonify({
                    "status": "FAILED",
                    "error": result["error"],
                    "actual_result": None,
                    "expected_type": expected_type,
                    "expected_description": expected_description,
                    "passed": False
                })
        
        actual_result = result['data']
        
        # Compare actual vs expected
        passed = compare_results(actual_result, expected_type, expected_description)
        
        # Create detailed message
        actual_count = len(actual_result) if actual_result else 0
        
        # Helper: Check if this looks like a COUNT query result (1 row, 1 column, numeric value)
        is_count_result = False
        count_value = None
        if actual_count == 1 and actual_result[0] and len(actual_result[0]) == 1:
            first_value = list(actual_result[0].values())[0]
            try:
                count_value = int(first_value)
                is_count_result = True
            except (ValueError, TypeError):
                pass
        
        # Build detailed message based on expected type
        if expected_type == "NO_ERROR":
            if actual_count > 0 and actual_result[0]:
                # Show the procedure status/result
                first_value = list(actual_result[0].values())[0]
                message = f"Procedure executed successfully. Status: {first_value}"
            else:
                message = "Procedure executed successfully"
        elif expected_type.startswith("VALUE_EQUALS:") or expected_type.startswith("SINGLE_VALUE:"):
            if actual_count > 0:
                first_value = list(actual_result[0].values())[0]
                expected_val = expected_type.split(":", 1)[1]
                message = f"Expected value: {expected_val}. Got: {first_value}"
            else:
                message = f"Expected value but got empty result set"
        elif expected_type.startswith("VALUE_GREATER_THAN:"):
            if actual_count > 0:
                first_value = list(actual_result[0].values())[0]
                threshold = expected_type.split(":")[1]
                message = f"Expected value > {threshold}. Got: {first_value}"
            else:
                message = f"Expected value but got empty result set"
        elif expected_type.startswith("VALUE_LESS_THAN:"):
            if actual_count > 0:
                first_value = list(actual_result[0].values())[0]
                threshold = expected_type.split(":")[1]
                message = f"Expected value < {threshold}. Got: {first_value}"
            else:
                message = f"Expected value but got empty result set"
        elif expected_type.startswith("ROW_COUNT:"):
            expected_rows = expected_type.split(":")[1]
            if is_count_result:
                # This is a COUNT query - show the count value, not row count
                message = f"Expected count: {expected_rows}. Got count: {count_value}"
            else:
                message = f"Expected {expected_rows} row(s). Got: {actual_count} row(s)"
        elif expected_type == "NO_ROWS":
            if is_count_result:
                # This is a COUNT query - show the count value
                message = f"Expected count: 0. Got count: {count_value}"
            else:
                message = f"Expected 0 rows. Got: {actual_count} row(s)"
        elif expected_type == "HAS_ROWS":
            if is_count_result:
                # This is a COUNT query - show the count value
                message = f"Expected count > 0. Got count: {count_value}"
            else:
                message = f"Expected data (>0 rows). Got: {actual_count} row(s)"
        else:
            if is_count_result:
                message = f"Query returned count: {count_value}"
            else:
                message = f"Got {actual_count} row(s)"
        
        return jsonify({
            "status": "PASSED" if passed else "FAILED",
            "actual_result": actual_result,
            "expected_type": expected_type,
            "expected_description": expected_description,
            "passed": passed,
            "message": message,
            "actual_row_count": actual_count
        })
    except Exception as e:
        return jsonify({"error": str(e)})

def create_test_generation_prompt(schema, object_name, object_type, metadata, user_test_request='', test_case_count=5):
    """
    Create a prompt for Cortex AI to generate test cases
    """
    # Extract key information for the prompt
    total_rows = metadata.get('total_rows', 'unknown')
    columns_info = metadata.get('columns', [])
    sample_data = metadata.get('sample_data', [])
    statistics = metadata.get('statistics', {})
    schema_tables = metadata.get('schema_tables', [])
    
    # Build a clear metadata summary
    metadata_summary = ""
    
    # Add list of ALL tables in schema so AI uses correct names
    if schema_tables:
        metadata_summary = f"""
=== ALL TABLES IN THIS SCHEMA (USE EXACT NAMES) ===
{chr(10).join([f"  - {schema}.{tbl}" for tbl in schema_tables])}
=== END TABLE LIST ===

CRITICAL: When referencing other tables, use ONLY the exact table names from the list above!
Do NOT guess or pluralize table names. For example, use DIM_CUSTOMER not DIM_CUSTOMERS.

"""
    
    # Handle different object types
    if object_type in ['TABLE', 'VIEW']:
        metadata_summary += f"""
Object Information:
- Total Rows: {total_rows}
- Columns: {len(columns_info)}

Column Details:
{chr(10).join([f"  - {col.get('name')}: {col.get('type')}" for col in columns_info[:10]]) if columns_info else 'No columns found'}

"""
    elif object_type in ['PROCEDURE', 'FUNCTION']:
        # For procedures and functions, focus on definition and arguments
        metadata_summary = f"""
Object Information:
- Type: {object_type}
- Name: {schema}.{object_name}

"""
        if metadata.get('arguments'):
            metadata_summary += "Arguments:\n"
            for arg in metadata['arguments']:
                arg_name = arg.get('argument_name', arg.get('name', 'unknown'))
                arg_type = arg.get('data_type', arg.get('type', 'unknown'))
                metadata_summary += f"  - {arg_name}: {arg_type}\n"
        
        if metadata.get('definition'):
            # Include FULL definition for AI to analyze
            metadata_summary += f"\n=== FULL PROCEDURE DEFINITION ===\n{metadata['definition']}\n=== END DEFINITION ===\n"
        
        # Add metadata about referenced tables/views
        if metadata.get('referenced_metadata'):
            metadata_summary += "\n=== REFERENCED TABLES/VIEWS METADATA ===\n"
            for obj_name, obj_meta in metadata['referenced_metadata'].items():
                metadata_summary += f"\nTable/View: {obj_name}\n"
                metadata_summary += f"  Total Rows: {obj_meta.get('total_rows', 'unknown')}\n"
                
                if obj_meta.get('columns'):
                    metadata_summary += "  Columns:\n"
                    for col in obj_meta['columns'][:5]:  # Show first 5 columns
                        col_name = col.get('name', 'unknown')
                        col_type = col.get('type', 'unknown')
                        metadata_summary += f"    - {col_name}: {col_type}\n"
                
                if obj_meta.get('sample_data'):
                    metadata_summary += "  Sample Data:\n"
                    for row in obj_meta['sample_data'][:2]:  # Show 2 sample rows
                        metadata_summary += f"    {row}\n"
            metadata_summary += "=== END REFERENCED METADATA ===\n"
        
        # Return early for procedures/functions with different prompt
        return create_procedure_function_prompt(schema, object_name, object_type, metadata_summary, user_test_request, test_case_count)
    
    else:
        metadata_summary = f"""
Object Information:
- Type: {object_type}
- Name: {schema}.{object_name}

Metadata:
{str(metadata)}
"""
    
    if statistics:
        metadata_summary += "Statistics (Numeric Columns):\n"
        for col_name, stats in statistics.items():
            avg_val = stats.get('AVG_VAL')
            if avg_val is not None:
                try:
                    avg_str = f"{float(avg_val):.2f}"
                except:
                    avg_str = str(avg_val)
            else:
                avg_str = 'N/A'
            metadata_summary += f"  - {col_name}: MIN={stats.get('MIN_VAL')}, MAX={stats.get('MAX_VAL')}, AVG={avg_str}\n"
    
    # Add distinct values for categorical columns - CRITICAL for AI to know ALL valid values
    distinct_values = metadata.get('distinct_values', {})
    if distinct_values:
        metadata_summary += "\n=== DISTINCT VALUES FOR CATEGORICAL COLUMNS (COMPLETE LIST) ===\n"
        metadata_summary += "IMPORTANT: These are ALL the valid values that exist in the data. Do NOT assume values outside this list are invalid.\n\n"
        for col_name, info in distinct_values.items():
            if isinstance(info, dict):
                count = info.get('count', 'unknown')
                values = info.get('values', [])
                if isinstance(values, list):
                    metadata_summary += f"  {col_name} ({count} distinct values): {values}\n"
                else:
                    metadata_summary += f"  {col_name}: {values}\n"
        metadata_summary += "=== END DISTINCT VALUES ===\n"
    
    if sample_data and len(sample_data) > 0:
        metadata_summary += f"\nSample Data (showing {min(len(sample_data), 3)} rows):\n"
        for i, row in enumerate(sample_data[:3]):
            # Convert row to string and limit length to prevent issues with large data
            row_str = str(row)
            if len(row_str) > 500:
                row_str = row_str[:500] + "... (truncated)"
            metadata_summary += f"  Row {i+1}: {row_str}\n"
    
    # Build user request section if provided
    user_request_section = ""
    if user_test_request:
        user_request_section = f"""
=== USER'S SPECIFIC REQUEST ===
The user has specifically asked for these kinds of test cases:
"{user_test_request}"

IMPORTANT: Prioritize generating test cases that match the user's request above.
Focus on what the user asked for while still ensuring the tests are valid and executable.
"""
    
    prompt = f"""You are an expert data quality engineer. Analyze the metadata below and generate comprehensive, intelligent test cases for this Snowflake {object_type}.

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!! CRITICAL SQL RULE - READ THIS FIRST - FAILURE TO FOLLOW CAUSES ERRORS !!!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
In Snowflake, DATE/TIMESTAMP/NUMBER columns CANNOT be compared to empty strings!
- WRONG (CAUSES ERROR): WHERE DATE_COLUMN != ''
- WRONG (CAUSES ERROR): WHERE DATE_COLUMN = ''  
- WRONG (CAUSES ERROR): WHERE NUMBER_COLUMN != ''
- CORRECT: WHERE DATE_COLUMN IS NOT NULL
- CORRECT: WHERE DATE_COLUMN IS NULL
- CORRECT: WHERE NUMBER_COLUMN IS NOT NULL

Empty string comparisons ('') are ONLY valid for VARCHAR/STRING/TEXT columns!
For DATE, TIMESTAMP, NUMBER, INTEGER, FLOAT columns: use IS NULL / IS NOT NULL ONLY!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

=== OBJECT DETAILS ===
Object: {schema}.{object_name}
Type: {object_type}
Total Rows: {total_rows}

{metadata_summary}
{user_request_section}
=== YOUR TASK ===
Analyze the metadata thoroughly and generate exactly {test_case_count} meaningful test cases that:
1. Validate data integrity and quality
2. Check business rules based on column names and data patterns
3. Ensure referential integrity where applicable
4. Test edge cases and potential data issues

=== GUIDELINES ===
- Use the actual column names from the metadata
- Use the actual row count ({total_rows}) when testing volume
- Use the DISTINCT VALUES section to know ALL valid values for categorical columns
- Use the STATISTICS section to understand numeric ranges
- Generate tests appropriate for the table's size and purpose
- Think about what could go wrong with this data

=== IMPORTANT NOTES ===
- For categorical columns: The DISTINCT VALUES section shows ALL valid values in the data
- For numeric columns: Use MIN/MAX/AVG from statistics to set reasonable thresholds
- Always use the fully qualified table name: {schema}.{object_name}
- REMEMBER: Never use != '' or = '' with DATE/TIMESTAMP/NUMBER columns (see CRITICAL RULE above)

=== OUTPUT FORMAT ===
Return ONLY a JSON array:
[
  {{
    "test_name": "Descriptive name",
    "description": "What this test validates",
    "query": "SQL query using {schema}.{object_name}",
    "expected_type": "VALUE_EQUALS:N | VALUE_GREATER_THAN:N | VALUE_LESS_THAN:N | HAS_ROWS | NO_ROWS | ROW_COUNT:N",
    "expected_description": "Expected outcome"
  }}
]

=== EXPECTED_TYPE REFERENCE ===
- VALUE_EQUALS:N - The first value in the result equals N (USE THIS for COUNT(*) queries!)
- VALUE_GREATER_THAN:N - First value > N
- VALUE_LESS_THAN:N - First value < N  
- HAS_ROWS - Query returns at least 1 row of data
- NO_ROWS - Query returns 0 rows OR COUNT(*) = 0
- ROW_COUNT:N - Query returns exactly N rows (for SELECT * queries, NOT for COUNT queries)

IMPORTANT: For COUNT(*) queries, use VALUE_EQUALS:N, not ROW_COUNT:N!
Example: SELECT COUNT(*) → returns 1 row with value 500 → use VALUE_EQUALS:500

Generate intelligent test cases based on your analysis. Return ONLY the JSON array."""
    
    # Escape single quotes for SQL
    escaped_prompt = prompt.replace("'", "''")
    
    # Safety check: if prompt is too large, it might cause SQL parsing issues
    if len(escaped_prompt) > 50000:
        # Truncate metadata_summary if needed
        metadata_summary = metadata_summary[:10000] + "\n... (metadata truncated due to size)\n"
        # Rebuild prompt with truncated metadata
        prompt = f"""You are a data quality engineer. Generate {test_case_count} test cases for this Snowflake {object_type}.

CRITICAL RULE: In Snowflake, DATE/TIMESTAMP/NUMBER columns CANNOT be compared to empty strings!
- WRONG: WHERE DATE_COLUMN != '' (causes Error 100040)
- CORRECT: WHERE DATE_COLUMN IS NOT NULL

Object: {schema}.{object_name}
Total Rows: {total_rows}
{user_request_section}
{metadata_summary}

Generate test cases as a JSON array with: test_name, description, query, expected_type, expected_description.
Use the actual column names and statistics from metadata.
Return ONLY the JSON array."""
        escaped_prompt = prompt.replace("'", "''")
    
    return escaped_prompt

def create_procedure_function_prompt(schema, object_name, object_type, metadata_summary, user_test_request='', test_case_count=5):
    """
    Create a specialized prompt for stored procedures and functions
    """
    # Build user request section if provided
    user_request_section = ""
    if user_test_request:
        user_request_section = f"""
=== USER'S SPECIFIC REQUEST ===
The user has specifically asked for these kinds of test cases:
"{user_test_request}"

IMPORTANT: Prioritize generating test cases that match the user's request above.
"""
    
    prompt = f"""You are an expert database testing specialist. Generate exactly {test_case_count} intelligent unit test cases for this Snowflake {object_type}.

{metadata_summary}
{user_request_section}
YOUR TASK - ANALYZE AND GENERATE SMART TESTS:

STEP 1: READ THE FULL DEFINITION ABOVE
- The procedure definition shows the complete code
- The Arguments section shows what parameters are required and their types
- The REFERENCED TABLES/VIEWS METADATA shows actual data from tables this procedure uses

STEP 2: UNDERSTAND THE PARAMETERS
Look at the procedure body to understand what each parameter does:
- DATE parameters: Used in WHERE clauses with date filters
- VARCHAR/STRING parameters: Used to match values in specific columns  
- NUMBER/INTEGER parameters: Used as IDs or counts
- Check the REFERENCED METADATA to see actual column values and data types

STEP 3: GENERATE REALISTIC PARAMETER VALUES
Use the REFERENCED METADATA to determine realistic values:

For DATE parameters:
- Use CURRENT_DATE, DATEADD(day, -7, CURRENT_DATE), '2024-01-01', etc.
- Look at date columns in the referenced tables

For STRING parameters:
- Look at the sample data in REFERENCED METADATA
- If parameter filters on a STATUS column, use actual status values you see
- If parameter matches NAME column, use actual names from sample data
- If parameter is a code/ID, use values that appear in the sample data

For NUMBER parameters:
- If it's an ID, use small values like 1, 2, 10, 100
- If it's a count/limit, use reasonable values like 10, 100, 1000
- Check sample data for actual numeric ranges

EXAMPLE:
If procedure has parameter @customer_status VARCHAR and you see in REFERENCED METADATA:
  Sample Data: {{'STATUS': 'ACTIVE'}}, {{'STATUS': 'INACTIVE'}}
Then generate: CALL procedure_name('ACTIVE'), CALL procedure_name('INACTIVE')

If procedure has @start_date DATE and @end_date DATE:
Then generate: 
- CALL procedure_name(DATEADD(day, -30, CURRENT_DATE), CURRENT_DATE)

For boolean type parameters, use TRUE/FALSE or 1/0 based on what you see in the procedure definition.

STEP 4: CREATE TEST CASES
Generate tests in this format:
{{
  "test_name": "Test with [describe parameters]",
  "description": "What this test validates",
  "query": "CALL {schema}.{object_name}(param1, param2, ...)",
  "expected_type": "NO_ERROR",
  "expected_description": "Should execute successfully with [context]"
}}

Test variety:
1. Normal valid input (using values from sample data)
2. Edge case - empty/recent dates
3. Different variations of string parameters from sample data
4. Boundary values for numbers
5. Invalid input test (expect ERROR)

FORMAT AS JSON ARRAY:
[
  {{
    "test_name": "...",
    "description": "...",
    "query": "CALL {schema}.{object_name}(...)",
    "expected_type": "NO_ERROR or ERROR",
    "expected_description": "..."
  }}
]

CRITICAL RULES:
✓ USE actual values from REFERENCED METADATA sample data
✓ USE date functions like CURRENT_DATE, DATEADD for date parameters
✓ MATCH string parameters to actual column values you see
✓ UNDERSTAND what each parameter does by reading the procedure code
✓ Generate at least one error test case with invalid inputs

✗ DON'T use fake placeholder values like 'param1', 'valid_value', 'test'
✗ DON'T call with empty () if parameters are required
✗ DON'T ignore the sample data - USE IT to generate realistic values
✗ DON'T make up values that don't appear anywhere in the metadata
✗ DON'T compare DATE/TIMESTAMP columns to empty strings ('') - use IS NULL or IS NOT NULL instead
✗ DON'T compare NUMBER/INTEGER columns to empty strings - they can only be NULL or numeric

If you CANNOT determine reasonable values even with the metadata:
Generate manual testing note:
{{
  "test_name": "Manual Testing Required",
  "description": "Parameters require specific external context not available in metadata",
  "query": "SELECT 'This procedure requires: [explain]' as note",
  "expected_type": "HAS_ROWS",
  "expected_description": "Manual testing needed"
}}

Generate exactly {test_case_count} test cases now. Analyze the metadata and be intelligent!

Only return the JSON array, no additional text."""
    
    # Escape single quotes and check size
    escaped_prompt = prompt.replace("'", "''")
    
    # Safety check for large prompts
    if len(escaped_prompt) > 50000:
        # If too large, truncate the metadata_summary
        if len(metadata_summary) > 10000:
            metadata_summary = metadata_summary[:10000] + "\n... (metadata truncated due to size)\n"
            # Rebuild with truncated metadata
            prompt = f"""You are an expert database testing specialist. Generate exactly {test_case_count} intelligent unit test cases for this Snowflake {object_type}.

{metadata_summary}
{user_request_section}
Generate realistic test cases with actual parameter values from the metadata.
Format: JSON array with test_name, description, query, expected_type, expected_description.

Only return the JSON array, no additional text."""
            escaped_prompt = prompt.replace("'", "''")
    
    return escaped_prompt

def parse_test_cases(test_cases_text, schema, object_name, object_type):
    """
    Parse the AI-generated test cases
    """
    import json
    import re
    
    try:
        # Extract JSON from the response
        json_match = re.search(r'\[[\s\S]*\]', test_cases_text)
        if json_match:
            test_cases_json = json_match.group(0)
            test_cases = json.loads(test_cases_json)
            
            # Add IDs and object info to each test case
            for i, test in enumerate(test_cases):
                test['id'] = i + 1
                test['schema'] = schema
                test['object_name'] = object_name
                test['object_type'] = object_type
                test['status'] = 'NOT_RUN'
            
            return test_cases
        else:
            return []
    except Exception as e:
        print(f"Error parsing test cases: {str(e)}")
        return []

def compare_results(actual, expected_type, expected_description):
    """
    Compare actual results with expected results based on type
    
    Expected types:
    - NO_ERROR: Query/procedure executed successfully without error
    - HAS_ROWS: Should return one or more rows
    - NO_ROWS: Should return empty result set OR COUNT(*) = 0
    - SINGLE_VALUE:value: Should return exactly one row with one column matching the value
    - VALUE_EQUALS:N: First column of first row should equal N
    - VALUE_GREATER_THAN:N: First column of first row should be > N
    - VALUE_LESS_THAN:N: First column of first row should be < N
    - ROW_COUNT:N: Should return exactly N rows
    - ERROR: Should have failed (checked before this function)
    """
    try:
        # Parse expected type
        if expected_type == "NO_ERROR":
            # For procedures - just check that we got results without error
            # Procedures typically return status messages
            return actual is not None
        
        elif expected_type == "NO_ROWS":
            # Expected empty result OR a COUNT query returning 0
            if actual is None:
                return False
            if len(actual) == 0:
                return True
            # Smart check: if it's a single row with a single value of 0, treat as NO_ROWS
            # This handles COUNT(*) queries that return 1 row with value 0
            if len(actual) == 1:
                first_row = actual[0]
                if len(first_row) == 1:
                    first_value = list(first_row.values())[0]
                    try:
                        if float(first_value) == 0:
                            return True
                    except (ValueError, TypeError):
                        pass
            return False
        
        elif expected_type == "HAS_ROWS":
            # Expected one or more rows with actual data
            if actual is None or len(actual) == 0:
                return False
            # Smart check: if it's a COUNT query returning 0, that's NOT "has rows"
            if len(actual) == 1:
                first_row = actual[0]
                if len(first_row) == 1:
                    first_value = list(first_row.values())[0]
                    try:
                        if float(first_value) == 0:
                            return False  # COUNT(*) = 0 means no data
                    except (ValueError, TypeError):
                        pass
            return True
        
        elif expected_type.startswith("SINGLE_VALUE:") or expected_type.startswith("VALUE_EQUALS:"):
            # Expected specific value in first row, first column
            if actual is None or len(actual) == 0:
                return False
            
            # Extract expected value
            if expected_type.startswith("SINGLE_VALUE:"):
                expected_value_str = expected_type.split(":", 1)[1]
            else:
                expected_value_str = expected_type.split(":", 1)[1]
            
            # Get actual value from first row, first column
            first_row = actual[0]
            actual_value = list(first_row.values())[0]
            
            # Try to convert to comparable types
            try:
                # Try numeric comparison
                expected_num = float(expected_value_str)
                actual_num = float(actual_value)
                return abs(expected_num - actual_num) < 0.0001  # Allow small float differences
            except (ValueError, TypeError):
                # Fall back to string comparison
                return str(actual_value).strip() == expected_value_str.strip()
        
        elif expected_type.startswith("VALUE_GREATER_THAN:"):
            # First value should be greater than threshold
            if actual is None or len(actual) == 0:
                return False
            
            threshold = float(expected_type.split(":")[1])
            first_row = actual[0]
            actual_value = float(list(first_row.values())[0])
            return actual_value > threshold
        
        elif expected_type.startswith("VALUE_LESS_THAN:"):
            # First value should be less than threshold
            if actual is None or len(actual) == 0:
                return False
            
            threshold = float(expected_type.split(":")[1])
            first_row = actual[0]
            actual_value = float(list(first_row.values())[0])
            return actual_value < threshold
        
        elif expected_type.startswith("ROW_COUNT:"):
            # Expected specific row count (for multi-row results, not COUNT queries)
            # BUT: Smart fix - if result is 1 row with COUNT value matching, treat as pass
            try:
                expected_count = int(expected_type.split(":")[1])
                
                if actual is None:
                    return False
                
                # Direct row count check
                if len(actual) == expected_count:
                    return True
                
                # Smart fix: If AI mistakenly used ROW_COUNT for a COUNT(*) query,
                # check if the single returned value equals the expected count
                if len(actual) == 1:
                    first_row = actual[0]
                    if len(first_row) == 1:
                        first_value = list(first_row.values())[0]
                        try:
                            if int(first_value) == expected_count:
                                return True
                        except (ValueError, TypeError):
                            pass
                
                return False
            except:
                return False
        
        elif expected_type == "ERROR":
            # Should have errored (but if we're here, it didn't)
            return False
        
        else:
            # Unknown type, default to checking if we got results
            return actual is not None and len(actual) > 0
            
    except Exception as e:
        print(f"Error in compare_results: {str(e)}")
        return False

@app.route('/api/table-metadata', methods=['POST'])
def get_table_metadata():
    """
    Get metadata for a specific table including columns and row count
    """
    try:
        data = request.get_json()
        schema = data.get('schema')
        table = data.get('table')
        
        if not schema or not table:
            return jsonify({"error": "Schema and table are required"})
        
        metadata = {}
        
        # Get column info
        desc_query = f"DESCRIBE TABLE {schema}.{table}"
        desc_result = execute_query(desc_query)
        if "error" not in desc_result:
            metadata['columns'] = desc_result['data']
        
        # Get row count
        count_query = f"SELECT COUNT(*) as total_rows FROM {schema}.{table}"
        count_result = execute_query(count_query)
        if "error" not in count_result:
            metadata['total_rows'] = count_result['data'][0]['TOTAL_ROWS']
        
        return jsonify(metadata)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/schema-metadata', methods=['POST'])
def get_schema_metadata():
    """
    Get metadata for entire schema including all tables and their structures
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        
        if not database or not schema:
            return jsonify({"error": "Database and schema are required"})
        
        # Set database context first
        use_db_query = f"USE DATABASE {database}"
        execute_query(use_db_query)
        
        # Build fully qualified schema name
        full_schema = f"{database}.{schema}"
        
        metadata = {"tables": [], "relationships": []}
        
        # Get all tables
        tables_query = f"SHOW TABLES IN SCHEMA {full_schema}"
        tables_result = execute_query(tables_query)
        
        if "error" not in tables_result:
            # Get ALL tables (removed limit) - but only fetch column metadata for first 50 to keep it fast
            all_tables = tables_result['data']
            
            for idx, table_row in enumerate(all_tables):
                table_name = table_row['name']
                table_info = {
                    "name": table_name,
                    "type": "TABLE",
                    "columns": []
                }
                
                # Only fetch detailed column info for first 50 tables to avoid timeout
                # AI will still see ALL table names for query generation
                if idx < 50:
                    desc_query = f"DESCRIBE TABLE {full_schema}.{table_name}"
                    desc_result = execute_query(desc_query)
                    if "error" not in desc_result:
                        table_info['columns'] = [
                            {"name": col['name'], "type": col['type']} 
                            for col in desc_result['data'][:15]  # First 15 columns per table
                        ]
                
                metadata['tables'].append(table_info)
        
        # Get views too
        views_query = f"SHOW VIEWS IN SCHEMA {full_schema}"
        views_result = execute_query(views_query)
        
        if "error" not in views_result:
            # Get ALL views
            all_views = views_result['data']
            
            for idx, view_row in enumerate(all_views):
                view_name = view_row['name']
                view_info = {
                    "name": view_name,
                    "type": "VIEW",
                    "columns": []
                }
                
                # Only fetch column details for first 30 views to keep it fast
                if idx < 30:
                    desc_query = f"DESCRIBE VIEW {full_schema}.{view_name}"
                    desc_result = execute_query(desc_query)
                    if "error" not in desc_result:
                        view_info['columns'] = [
                            {"name": col['name'], "type": col['type']} 
                            for col in desc_result['data'][:15]
                        ]
                
                metadata['tables'].append(view_info)
        
        return jsonify(metadata)
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/api/generate-sql-from-question', methods=['POST'])
def generate_sql_from_question():
    """
    Use Cortex AI to convert a natural language question into SQL with auto-discovery of tables and joins
    """
    try:
        data = request.get_json()
        database = data.get('database')
        schema = data.get('schema')
        question = data.get('question')
        metadata = data.get('metadata', {})
        
        if not database or not schema or not question:
            return jsonify({"error": "Database, schema, and question are required"})
        
        # Build fully qualified schema name
        full_schema = f"{database}.{schema}"
        
        # Build comprehensive schema information for AI with FULLY QUALIFIED table names
        schema_info = f"Database: {database}\nSchema: {schema}\nFully Qualified Schema: {full_schema}\n\nAvailable Tables (with FULL names):\n"
        
        if metadata.get('tables'):
            for table in metadata['tables']:
                # Show table with fully qualified name
                full_table_name = f"{full_schema}.{table['name']}"
                schema_info += f"\n{full_table_name} ({table['type']}):\n"
                if table.get('columns'):
                    columns_list = [f"{col['name']} ({col['type']})" for col in table['columns'][:10]]
                    schema_info += "  Columns: " + ", ".join(columns_list) + "\n"
        
        # Create intelligent prompt for multi-table queries
        # Note: We'll escape the entire prompt at the end, not individual parts
        prompt = f"""You are a Snowflake SQL expert with deep knowledge of database relationships and Snowflake-specific functions.

{schema_info}

User Question: {question}

Your Task:
1. Analyze which tables are needed to answer this question
2. Identify relationships between tables (look for ID columns, foreign keys, common column names)
3. Generate a complete SQL query with proper JOINs if multiple tables are needed
4. ALWAYS use the fully qualified schema name in your queries: {full_schema}
5. Use ONLY Snowflake-compatible functions

CRITICAL REQUIREMENT:
ALL table references MUST be fully qualified with the schema: {full_schema}.TABLE_NAME
Never use just TABLE_NAME without the schema prefix.

SNOWFLAKE-SPECIFIC SYNTAX (IMPORTANT):
- String aggregation: LISTAGG(column, delimiter) NOT STRING_AGG
- Date functions: DATEADD, DATEDIFF, DATE_TRUNC, YEAR(), MONTH()
- Window functions: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
- String functions: CONCAT, SUBSTR, UPPER, LOWER, TRIM
- Conditional: IFF(condition, true_value, false_value) or CASE WHEN

COMPLEX QUERY PATTERNS (USE CTEs FOR THESE):
- When comparing to an aggregate (e.g., "above average"): Use WITH clause to calculate aggregate first
- When needing nested aggregates: Break into CTEs - NEVER nest SUM/AVG/COUNT inside each other
- When filtering by aggregate results: Use HAVING or CTEs
- IMPORTANT: If you reference a CTE in WHERE clause, you MUST include it in FROM/JOIN clause!

Example of comparing to average (using CTE) - NOTE: avg_cte MUST be in FROM clause:
WITH customer_totals AS (
    SELECT CUSTOMERID, SUM(QUANTITY) as total_qty FROM {full_schema}.FACT_ORDERS GROUP BY CUSTOMERID
),
avg_cte AS (
    SELECT AVG(total_qty) as avg_total FROM customer_totals
)
SELECT ct.CUSTOMERID, ct.total_qty 
FROM customer_totals ct, avg_cte 
WHERE ct.total_qty > avg_cte.avg_total

CRITICAL RULE: Every CTE referenced in SELECT/WHERE must appear in FROM clause (use comma for cross join).

CRITICAL: Return ONLY a valid JSON object with this format:
{{"sql": "YOUR_COMPLETE_SQL_QUERY"}}

Rules for JOIN Detection:
- If question mentions "customers and orders" → JOIN CUSTOMERS and ORDERS tables
- Look for ID columns: CUSTOMER_ID, ORDER_ID, PRODUCT_ID, etc.
- Match FK patterns: table1.CUSTOMER_ID = table2.ID or table1.ID = table2.CUSTOMER_ID
- Use column names to infer relationships
- Always use LEFT JOIN for optional relationships, INNER JOIN for required

Examples (NOTICE THE FULLY QUALIFIED NAMES):
Question: "Show customers and their orders"
{{"sql": "SELECT c.*, o.* FROM {full_schema}.DIM_CUSTOMER c JOIN {full_schema}.FACT_ORDERS o ON c.CUSTOMERID = o.CUSTOMERID LIMIT 100"}}

Question: "Total sales by customer name"
{{"sql": "SELECT c.CUSTOMERNAME, SUM(o.AMOUNT) as total FROM {full_schema}.DIM_CUSTOMER c JOIN {full_schema}.FACT_ORDERS o ON c.CUSTOMERID = o.CUSTOMERID GROUP BY c.CUSTOMERNAME ORDER BY total DESC LIMIT 100"}}

Question: "Products with their names concatenated"
{{"sql": "SELECT LISTAGG(PRODUCTNAME, ', ') as products FROM {full_schema}.DIM_PRODUCT LIMIT 100"}}

Question: "Orders from 2022"
{{"sql": "SELECT * FROM {full_schema}.FACT_ORDERS WHERE YEAR(ORDERDATE) = 2022 LIMIT 100"}}

Question: "Customers who ordered more than average"
{{"sql": "WITH customer_totals AS (SELECT CUSTOMERID, SUM(QUANTITY) as total_qty FROM {full_schema}.FACT_ORDERS GROUP BY CUSTOMERID), avg_calc AS (SELECT AVG(total_qty) as avg_qty FROM customer_totals) SELECT ct.CUSTOMERID, ct.total_qty FROM customer_totals ct, avg_calc WHERE ct.total_qty > avg_calc.avg_qty LIMIT 100"}}

Question: "Products bought by high-spending customers"
{{"sql": "WITH customer_totals AS (SELECT CUSTOMERID, SUM(QUANTITY) as total_qty FROM {full_schema}.FACT_ORDERS GROUP BY CUSTOMERID), avg_calc AS (SELECT AVG(total_qty) as avg_qty FROM customer_totals) SELECT DISTINCT p.PRODUCTNAME FROM {full_schema}.FACT_ORDERS o JOIN {full_schema}.DIM_PRODUCT p ON o.PRODUCTID = p.PRODUCTID JOIN customer_totals ct ON o.CUSTOMERID = ct.CUSTOMERID CROSS JOIN avg_calc WHERE ct.total_qty > avg_calc.avg_qty LIMIT 100"}}

Now generate SQL for: {question}

REMEMBER: Use {full_schema}.TABLE_NAME format for ALL tables!

Return ONLY the JSON, nothing else."""

        # Escape the entire prompt for SQL (single quotes)
        escaped_prompt = prompt.replace("'", "''")
        
        # First, set the database context for this session
        use_db_query = f"USE DATABASE {database}"
        use_db_result = execute_query(use_db_query)
        if "error" in use_db_result:
            print(f"Warning: Could not set database context: {use_db_result['error']}")
        
        # Also set schema context
        use_schema_query = f"USE SCHEMA {database}.{schema}"
        use_schema_result = execute_query(use_schema_query)
        if "error" in use_schema_result:
            print(f"Warning: Could not set schema context: {use_schema_result['error']}")
        
        # Ensure warehouse is active for Cortex AI
        import os
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'WH_CORTEX_POC_READ')
        use_wh_query = f"USE WAREHOUSE {warehouse}"
        use_wh_result = execute_query(use_wh_query)
        if "error" in use_wh_result:
            print(f"Warning: Could not set warehouse: {use_wh_result['error']}")
        
        # Call Cortex AI
        # Using mixtral-8x7b (more commonly available than mistral-large2)
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'mixtral-8x7b',
            '{escaped_prompt}'
        ) as generated_sql
        """
        
        result = execute_query(cortex_query)
        
        if "error" in result:
            return jsonify({"error": f"AI generation failed: {result['error']}"})
        
        # Extract SQL from AI response
        ai_response = result['data'][0]['GENERATED_SQL'].strip()
        
        # Parse JSON to get SQL
        import json
        import re
        
        generated_sql = None
        
        # Remove markdown code block markers and language identifiers
        ai_response = ai_response.replace('```json', '').replace('```sql', '').replace('```', '').strip()
        # Remove standalone "json" or "sql" at the start if present
        if ai_response.lower().startswith('json'):
            ai_response = ai_response[4:].strip()
        if ai_response.lower().startswith('sql'):
            ai_response = ai_response[3:].strip()
        
        print(f"DEBUG - AI Response (first 500 chars): {ai_response[:500]}")
        
        try:
            # Try to find and parse JSON - look for { ... }
            # Find the first { and last }
            start_idx = ai_response.find('{')
            end_idx = ai_response.rfind('}')
            
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                json_str = ai_response[start_idx:end_idx+1]
                json_obj = json.loads(json_str)
                if 'sql' in json_obj:
                    generated_sql = json_obj['sql'].strip()
        except Exception as e:
            print(f"DEBUG - JSON parse error: {e}")
            pass
        
        # Fallback 1: Try to extract SQL from truncated JSON like {"sql": "SELECT ...
        if not generated_sql:
            # Look for {"sql": " pattern and extract everything after it
            sql_match = re.search(r'\{\s*"sql"\s*:\s*"(.+)', ai_response, re.DOTALL)
            if sql_match:
                extracted = sql_match.group(1)
                # Remove trailing "} if present, or just trailing quote/brace
                extracted = re.sub(r'"\s*\}?\s*$', '', extracted)
                # Also remove trailing incomplete parts
                extracted = extracted.rstrip('"').rstrip()
                if extracted:
                    generated_sql = extracted
                    print(f"DEBUG - Extracted from truncated JSON: {generated_sql[:100]}")
        
        # Fallback 2: Look for SELECT statement directly
        if not generated_sql:
            if '```sql' in ai_response:
                generated_sql = ai_response.split('```sql')[1].split('```')[0].strip()
            elif '```' in ai_response:
                generated_sql = ai_response.split('```')[1].split('```')[0].strip()
            else:
                # Try to find SELECT statement
                lines = [l.strip() for l in ai_response.split('\n') if l.strip()]
                for line in lines:
                    if line.upper().startswith('SELECT'):
                        generated_sql = line
                        break
                else:
                    generated_sql = lines[0] if lines else ai_response
        
        # Clean up - remove backslashes used for line continuation in JSON
        generated_sql = generated_sql.strip('"').strip("'").strip()
        # Remove all types of line continuation and escape characters
        generated_sql = generated_sql.replace('\\\n', ' ')  # Backslash + newline
        generated_sql = generated_sql.replace('\\n', ' ')   # Literal \n
        generated_sql = generated_sql.replace('\\ ', ' ')   # Backslash + space  
        generated_sql = generated_sql.replace('\\', ' ')    # Any remaining backslashes
        generated_sql = generated_sql.replace('\n', ' ')    # Actual newlines
        generated_sql = generated_sql.replace('\r', ' ')    # Carriage returns
        generated_sql = generated_sql.replace('\t', ' ')    # Tabs
        # Remove extra whitespace
        generated_sql = ' '.join(generated_sql.split())
        
        # SAFEGUARD: If SQL still starts with { or looks like JSON, extraction failed
        if generated_sql.startswith('{') or generated_sql.startswith('{"sql'):
            # One last attempt - try to manually extract SQL from truncated JSON
            import re
            sql_match = re.search(r'"sql"\s*:\s*"([^"]*)', ai_response)
            if sql_match:
                generated_sql = sql_match.group(1)
                # Clean it up again
                generated_sql = generated_sql.replace('\\n', ' ').replace('\\', ' ')
                generated_sql = ' '.join(generated_sql.split())
            else:
                return jsonify({
                    'success': False, 
                    'error': 'Failed to extract SQL from AI response. The AI may have returned a truncated response. Please try asking a simpler question or break it into smaller parts.'
                })
        
        # POST-PROCESSING FIX: Ensure all table references have the full database.schema prefix
        import re
        
        # Get list of table names from metadata
        table_names = []
        if metadata.get('tables'):
            table_names = [t['name'].upper() for t in metadata['tables']]
        
        # Pattern 1: Fix "FROM schema.table" -> "FROM database.schema.table"
        # This handles: FROM NN.DIM_CUSTOMER
        pattern1 = r'\b(FROM|JOIN|INTO|UPDATE)\s+' + re.escape(schema) + r'\.(\w+)'
        replacement1 = r'\1 ' + full_schema + r'.\2'
        generated_sql = re.sub(pattern1, replacement1, generated_sql, flags=re.IGNORECASE)
        
        # Pattern 2: Fix bare table names that are not already qualified
        # This handles: FROM DIM_CUSTOMER (without any schema)
        for table_name in table_names:
            # Match table name that is NOT preceded by a dot (meaning it's not qualified)
            # and IS preceded by FROM/JOIN/INTO/UPDATE
            pattern2 = r'\b(FROM|JOIN|INTO|UPDATE)\s+(?!' + re.escape(database) + r'\.)(' + re.escape(table_name) + r')\b'
            replacement2 = r'\1 ' + full_schema + r'.' + table_name
            generated_sql = re.sub(pattern2, replacement2, generated_sql, flags=re.IGNORECASE)
        
        # Pattern 3: Final safety check - if schema name appears without database prefix
        # This catches any remaining "schema.table" patterns
        pattern3 = r'(?<!\w)(?<!\.)' + re.escape(schema) + r'\.(\w+)'
        # Only replace if it's not already prefixed with the database
        if database.upper() + '.' + schema.upper() not in generated_sql.upper():
            # There might be schema.table without database prefix
            generated_sql = re.sub(
                r'\b' + re.escape(schema) + r'\.(\w+)',
                full_schema + r'.\1',
                generated_sql,
                flags=re.IGNORECASE
            )
        
        print(f"DEBUG - Database: {database}, Schema: {schema}")
        print(f"DEBUG - Generated SQL (after post-processing): {generated_sql}")
        
        # Execute the generated SQL
        query_result = execute_query(generated_sql)
        
        return jsonify({
            "query": generated_sql,
            "results": query_result
        })
        
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/api/generate-report', methods=['POST'])
def generate_report():
    """
    Generate a PDF data quality report for a given object and its test results.
    Accepts JSON body:
      - database, schema, object_name, object_type
      - test_cases  : list of test case dicts (each may include a 'status' key)
      - metadata    : object metadata dict
      - save_path   : (optional) directory path to save the report on the server
      - download    : (optional) bool, default True — return file for browser download
    """
    try:
        data         = request.get_json()
        database     = data.get('database', '')
        schema       = data.get('schema', '')
        object_name  = data.get('object_name', '')
        object_type  = data.get('object_type', 'TABLE')
        test_cases   = data.get('test_cases', [])
        metadata     = data.get('metadata', {})
        save_path    = data.get('save_path', '').strip()
        download     = data.get('download', True)

        if not all([database, schema, object_name]):
            return jsonify({'error': 'database, schema and object_name are required'}), 400

        # Determine output directory
        if save_path:
            report_dir = save_path
        else:
            # Default to the signed-in user's Downloads folder.
            report_dir = os.path.join(os.path.expanduser('~'), 'Downloads')

        if not os.path.isdir(report_dir):
            os.makedirs(report_dir, exist_ok=True)

        from datetime import datetime
        safe_name   = re.sub(r'[^A-Za-z0-9_\-]', '_', object_name)
        timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename    = f'DQ_Report_{safe_name}_{timestamp}.pdf'
        output_path = os.path.join(report_dir, filename)

        generate_pdf_report(
            object_name  = object_name,
            object_type  = object_type,
            database     = database,
            schema       = schema,
            test_cases   = test_cases,
            metadata     = metadata,
            output_path  = output_path,
        )

        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            return jsonify({'error': 'Report generation failed: output PDF was not created correctly'}), 500

        print(f'✅ Report saved: {output_path}')

        if download:
            return send_file(
                output_path,
                as_attachment=True,
                download_name=filename,
                mimetype='application/pdf',
            )
        else:
            return jsonify({'success': True, 'path': output_path, 'filename': filename})

    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Initialize connection when starting the app
    print("\n" + "=" * 50)
    print("🚀 Starting Snowflake Query Tool")
    print("=" * 50)
    
    # Don't create connection at startup - let user sign in via SSO button
    print("\n" + "=" * 50)
    print("🔍 Data Quality Tool")
    print("=" * 50)
    print("\n✅ Server starting...")
    print(f"🌐 Open your browser and go to: http://localhost:5000")
    print("\n📝 Sign in with SSO from the home page to connect to Snowflake")
    print("\nPress CTRL+C to stop the server\n")
    print("=" * 50 + "\n")
    
    app.run(debug=True, port=5000, use_reloader=False)
