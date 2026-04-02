---
description: "Use when writing SQL queries, analysing data, building reports, working with Snowflake, designing data pipelines, fetching or exploring table/view/procedure metadata, generating data quality tests, integrating AI/LLM with data, or doing anything data engineering or data analysis related."
---

# Data Analyst & Data Engineer — Expert Behaviour

## Core Principles

- Always think about **data quality first**: null checks, row counts, deduplication, referential integrity.
- Use **fully qualified object names** (database.schema.table) in all SQL.
- Prefer **non-destructive** operations; never DROP, TRUNCATE, or DELETE without explicit confirmation.
- When exploring data, always start with: row counts, column types, nullability, and sample rows.
- Write **readable, maintainable SQL** — use CTEs over nested subqueries.
- Always use **parameterised queries** in Python to prevent SQL injection when values come from user input.

---

## Snowflake-Specific Rules

- Use `SNOWFLAKE.CORTEX.COMPLETE(...)` for AI generation when Cortex is available; fall back to external LLM APIs (OpenAI, Azure OpenAI) otherwise.
- Prefer `LISTAGG` over `STRING_AGG` for string aggregation.
- Use `DATEADD`, `DATEDIFF`, `DATE_TRUNC`, `YEAR()`, `MONTH()` for date operations.
- Never compare DATE, TIMESTAMP, or NUMBER columns to empty string (`''`) — use `IS NULL` / `IS NOT NULL`.
- For window functions use: `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)`.
- Use `IFF(condition, true_val, false_val)` for simple conditionals instead of CASE WHEN.
- Always use `USE DATABASE` / `USE SCHEMA` / `USE WAREHOUSE` before running queries that depend on context.
- When objects have overloads (procedures/functions), always include argument types in `GET_DDL` and `DESCRIBE` calls.
- Use `SHOW TABLES / VIEWS / PROCEDURES / FUNCTIONS IN SCHEMA` to discover objects.

---

## SQL Style Guidelines

- Use `WITH` (CTE) blocks for any query with more than 2 levels of aggregation.
- Alias all columns in SELECT when the origin is ambiguous.
- Always include `LIMIT` in exploratory/sample queries.
- Use `COUNT(DISTINCT col)` to check cardinality before assuming uniqueness.
- For aggregate comparisons (e.g., "above average"), always break into CTEs — never nest `AVG` inside `WHERE`.
- Reference every CTE used in a WHERE clause also in the FROM clause.

Example — comparing to average (correct Snowflake pattern):
```sql
WITH totals AS (
    SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id
),
avg_calc AS (
    SELECT AVG(total) AS avg_total FROM totals
)
SELECT t.customer_id, t.total
FROM totals t, avg_calc
WHERE t.total > avg_calc.avg_total;
```

---

## Data Quality & Testing

- For every data object (table, view, procedure), always check:
  1. Row count (volume test)
  2. Null checks on key/required columns
  3. Duplicate key checks
  4. Value range / domain checks (min, max, allowed values)
  5. Referential integrity (FK relationships)
- Use `expected_type` categories: `VALUE_EQUALS:N`, `VALUE_GREATER_THAN:N`, `HAS_ROWS`, `NO_ROWS`, `ROW_COUNT:N`.
- For COUNT(*) queries, always use `VALUE_EQUALS:N`, not `ROW_COUNT:N`.
- For procedure/function test cases, always use actual sample data values from metadata — never placeholder strings like `'test_value'`.

---

## Metadata Analysis

When exploring a data object, always collect:
- Column names and data types (`DESCRIBE TABLE / VIEW`)
- Row count (`SELECT COUNT(*)`)
- Sample rows (`SELECT * FROM ... LIMIT 5`)
- Statistics for numeric columns: `MIN`, `MAX`, `AVG`
- Distinct values for categorical (string) columns with ≤ 50 unique values
- View or procedure definition (`GET_DDL(...)`)

When generating test cases or reports, use this collected metadata as grounding — do not guess or fabricate column names or values.

---

## Python Data Engineering Patterns

- Use `snowflake-connector-python` with `DictCursor` for dictionary-style row access.
- Persist database connections across requests using a global connection object; always check if the connection is alive before use.
- Register cleanup functions with `atexit.register(...)` to close connections on shutdown.
- Centralise all query execution through a single `execute_query(query)` helper — handle `cursor.description is None` for DDL/context commands.
- Use `python-dotenv` and `.env` files for all credentials and config; never hardcode secrets.
- Always wrap individual metadata fetch calls in `try/except` so one failure doesn't break the entire metadata response.
- Return consistent JSON shapes from Flask endpoints: `{"success": True, "data": [...]}` or `{"error": "message"}`.

---

## Flask API Patterns

- Validate all required request parameters at the top of each endpoint; return `{"error": "..."}` immediately if missing.
- Use `host='0.0.0.0'` when the app needs to be accessible by other users on the network.
- Use environment variables `APP_HOST` and `APP_PORT` to make host and port configurable without code changes.
- Return HTTP-appropriate JSON error messages — never let unhandled exceptions reach the client.

---

## Reporting & Analysis Output

- When presenting data analysis results, always include:
  - Summary statistics (row counts, nulls, unique values)
  - Key insights and anomalies
  - Actionable recommendations
- Structure reports as: **Executive Summary → Findings → Data Details → Recommendations**.
- Use clear section headings and bullet points for scan-ability.
- When displaying SQL results, show column headers and a row count.

---

## AI / LLM Integration with Data

- Always pass fully qualified table names in prompts to avoid ambiguity.
- Include actual column names, data types, statistics, and sample rows in the prompt context.
- Include valid categorical values from the data so the model doesn't fabricate values.
- Escape all single quotes in prompts before embedding in SQL strings (`.replace("'", "''")`).
- Truncate large metadata payloads to stay within token limits while preserving the most important context (column names, sample rows, statistics).
- Always parse LLM responses defensively — use regex to extract JSON, handle malformed output gracefully.
- For Cortex fallback: if `SNOWFLAKE.CORTEX.COMPLETE` raises a permission error, route to an external LLM API (OpenAI, Azure OpenAI, etc.) using the same prompt.
