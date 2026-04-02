# Snowflake Data Quality Tool

A comprehensive web-based tool for Snowflake with two main features:
1. **SQL Query Tool** - Execute SQL queries with a user-friendly interface
2. **AI Unit Test Generator** - Automatically generate and run unit tests using Snowflake Cortex AI

## Features

### 1. SQL Query Tool
- ✅ Execute any SQL query on Snowflake
- ✅ Results displayed in table format (like Snowflake console)
- ✅ Row count display
- ✅ Sample queries for quick testing
- ✅ Keyboard shortcut: Ctrl+Enter to execute query
- ✅ SSO authentication with Snowflake

### 2. AI Unit Test Generator 🤖
- ✅ Select schema, object type, and specific database objects
- ✅ Supports Tables, Views, Stored Procedures, and Functions
- ✅ Automatically fetches object metadata (columns, definitions, arguments, etc.)
- ✅ Uses Snowflake Cortex AI (Mistral-Large2) to generate intelligent test cases
- ✅ Generates 5 comprehensive test cases per object
- ✅ Each test case includes:
  - Test name and description
  - SQL query to execute
  - Expected results
  - Run button to execute the test
- ✅ Real-time test execution with pass/fail status
- ✅ Visual feedback (green for passed, red for failed)
- ✅ Compare expected vs actual results
- ✅ View detailed test outputs in table format

## Setup Instructions

1. **Install Dependencies:**
   ```bash
   pip install flask snowflake-connector-python python-dotenv
   ```

2. **Configure Environment:**
   - Make sure your `.env` file has all the required Snowflake credentials
   - Required variables: 
     - SNOWFLAKE_ACCOUNT
     - SNOWFLAKE_USER
     - SNOWFLAKE_WAREHOUSE
     - SNOWFLAKE_DATABASE
     - SNOWFLAKE_SCHEMA
     - SNOWFLAKE_ROLE

3. **Run the Application:**
   ```bash
   python app.py
   ```
   - The app will prompt for SSO authentication once when starting
   - The connection persists for the lifetime of the application

4. **Access the Web Interface:**
   - **Query Tool**: http://localhost:5000
   - **AI Test Generator**: http://localhost:5000/unit-test-generator

## How to Use AI Unit Test Generator

1. **Select Schema**: Choose the schema containing your database objects
2. **Select Object Type**: Choose between Table, View, Stored Procedure, or Function
3. **Select Object**: Pick the specific object you want to test
4. **View Metadata**: Review the object's structure and definition
5. **Generate Tests**: Click "Generate Unit Tests with AI"
   - AI will analyze the object metadata
   - Generates 5 comprehensive test cases automatically
6. **Run Tests**: Click the "Run Test" button on any test case
   - View expected vs actual results
   - See pass/fail status with visual indicators
7. **Review Results**: Analyze the test outputs in table format

## Test Case Types Generated

The AI generates various test types including:
- **Data Integrity Tests**: Verify data consistency and correctness
- **Edge Case Tests**: Test boundary conditions and unusual inputs
- **Business Logic Tests**: Validate business rules and calculations
- **Data Type Tests**: Ensure correct data types and formats
- **Constraint Tests**: Verify primary keys, foreign keys, and other constraints
- **Performance Tests**: Check query performance (where applicable)

## Technology Stack

- **Backend**: Flask (Python)
- **Database**: Snowflake
- **AI**: Snowflake Cortex AI (Mistral-Large2 model)
- **Frontend**: HTML, CSS, JavaScript
- **Authentication**: Snowflake SSO (External Browser)

## Files Structure

```
Data Quality Tool/
├── app.py                          # Flask backend application
├── main.py                         # Original Python script
├── .env                            # Environment variables (not in git)
├── .env.example                    # Example environment file
├── README.md                       # This file
└── templates/
    ├── index.html                  # Query Tool UI
    └── unit_test_generator.html    # AI Test Generator UI
```

## API Endpoints

- `GET /` - Query Tool page
- `GET /unit-test-generator` - Test Generator page
- `POST /execute` - Execute SQL query
- `GET /api/schemas` - Get list of schemas
- `POST /api/objects` - Get list of objects by type
- `POST /api/object-metadata` - Get object metadata
- `POST /api/generate-tests` - Generate test cases with AI
- `POST /api/run-test` - Execute a specific test case

## Tips

- The SSO authentication happens once when you start the app
- The connection persists until you stop the application
- Generated tests are dynamic and tailored to each object's structure
- You can run tests multiple times to verify consistency
- Press Ctrl+C in terminal to stop the application

## Requirements

- Python 3.7+
- Snowflake account with:
  - SSO authentication enabled
  - Access to Snowflake Cortex AI
  - Appropriate permissions to read object metadata
  - Access to the selected schemas and objects
