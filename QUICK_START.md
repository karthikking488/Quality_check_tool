# AI Unit Test Generator - Quick Start Guide

## What It Does
Automatically generates comprehensive unit test cases for your Snowflake database objects using AI (Cortex).

## Supported Objects
- ✅ **Tables** - Tests data integrity, column types, constraints
- ✅ **Views** - Tests view logic and output correctness  
- ✅ **Stored Procedures** - Tests procedure logic with various inputs
- ✅ **Functions** - Tests function outputs and edge cases

## How It Works

### Step 1: Select Your Object
1. Choose a **Schema** (e.g., NN)
2. Select **Object Type** (Table, View, Procedure, or Function)
3. Pick the specific **Object Name** from the dropdown

### Step 2: Review Metadata
- The tool automatically fetches:
  - Column names and types (for tables/views)
  - Procedure/function definitions
  - Arguments and parameters
  - Sample data (for tables)

### Step 3: Generate Tests
- Click "Generate Unit Tests with AI"
- Cortex AI analyzes the metadata
- Creates 5 intelligent test cases including:
  - **Test Name**: Descriptive identifier
  - **Description**: What the test validates
  - **SQL Query**: The actual test to run
  - **Expected Result**: What should happen

### Step 4: Run & Validate
- Click "Run Test" on any test case
- See real-time execution
- Compare expected vs actual results
- Get pass/fail status with visual indicators

## Example Test Cases Generated

### For a Table (CUSTOMERS):
1. **Data Integrity Check** - Verify no NULL values in required columns
2. **Data Type Validation** - Ensure correct data types
3. **Unique Constraint Test** - Check primary key uniqueness
4. **Row Count Validation** - Verify table has data
5. **Edge Case Test** - Find duplicate or anomalous records

### For a Stored Procedure:
1. **Valid Input Test** - Call with correct parameters
2. **Null Input Test** - Test handling of NULL values
3. **Edge Case Test** - Test boundary conditions
4. **Error Handling Test** - Test invalid inputs
5. **Output Validation** - Verify expected return values

## Visual Indicators
- 🔵 **NOT RUN** - Test hasn't been executed yet
- 🟠 **RUNNING** - Test is currently executing
- 🟢 **PASSED** - Test executed successfully
- 🔴 **FAILED** - Test failed (see details)

## Tips for Best Results
✅ Select objects with clear, well-documented structures
✅ Run all tests to get comprehensive coverage
✅ Review failed tests to identify potential issues
✅ Use the metadata view to understand what's being tested
✅ Re-run tests after making object changes

## Access the Tool
Navigate to: http://localhost:5000/unit-test-generator

Happy Testing! 🚀
