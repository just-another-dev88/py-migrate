# py-migrate Configuration Guide

This guide provides the complete YAML schemas, structure specifications, and practical examples for configuring `py-migrate`. It details both the database connection credentials (`db_config.yaml`) and the declarative migration templates.

---

## 1. Database Connection Configuration (`db_config.yaml`)

The database configuration file defines credentials and connection parameters for the database instances involved in migrations. It maps unique database names/keys to their driver properties.

### Schema Specification

```yaml
databases:
  <database_key>:
    type: "postgres" | "oracle"      # Database engine type (case-insensitive)
    user: <string>                   # Database user account
    password: <string>               # Database password
    
    # --- PostgreSQL Specific Keys ---
    host: <string>                   # Hostname or IP address (e.g. "localhost")
    port: <integer>                  # Port number (default: 5432)
    database: <string>               # PostgreSQL database name

    # --- Oracle Thin Mode Specific Keys ---
    dsn: <string>                    # Oracle connection string / Data Source Name 
                                     # Format: "host:port/service_name"
```

### Complete Example

```yaml
databases:
  # Oracle Source Instance (Single DB or primary)
  oracle_primary:
    type: "oracle"
    user: "system"
    password: "secure_oracle_password"
    dsn: "192.168.1.50:1521/FREEPDB1"

  # Sharded Source Database Shard 1
  oracle_shard_1:
    type: "oracle"
    user: "system"
    password: "secure_oracle_password"
    dsn: "192.168.1.51:1521/SHARD1"

  # Sharded Source Database Shard 2
  oracle_shard_2:
    type: "oracle"
    user: "system"
    password: "secure_oracle_password"
    dsn: "192.168.1.52:1521/SHARD2"

  # PostgreSQL Destination Database
  postgres_destination:
    type: "postgres"
    user: "postgres"
    password: "secure_postgres_password"
    host: "localhost"
    port: 5432
    database: "production_warehouse"
```

---

## 2. Declarative Migration Configuration (Template Schema)

The migration template is a YAML file that outlines what query to run, how to stream the records, what value transformations to perform, and what pre- and post-migration checks must pass.

### Top-Level Keys

| Field | Type | Required | Description |
| :--- | :--- | :---: | :--- |
| `name` | String | Yes | Name of the migration run (for logs and reporting). |
| `description` | String | No | Explanatory description of what the migration does. |
| `source_db` | String or List | Yes | A single database key OR a list of sharded database keys from `db_config.yaml`. |
| `target_db` | String | Yes | The single destination database key from `db_config.yaml`. |
| `streaming` | Object | No | Stream tuning options (`chunk_size` and `itersize`). |
| `pre_migration` | List | No | Assertions to run before streaming data starts. |
| `mapping` | Object | Yes | Definition of columns, transformations, and targets. |
| `post_migration` | List | No | Assertions to run before committing target writes. |

---

### `streaming` Options

Controls extraction pacing to manage client and server-side memory buffers.

```yaml
streaming:
  chunk_size: 1000  # Number of rows written to the target database in a single executemany batch
  itersize: 1000    # Number of rows fetched at a time from source server-side cursors (for Postgres)
```

---

### `pre_migration` & `post_migration` Checklists

Both checklist blocks share the same assertion types. If any checklist item fails, the migration is aborted immediately and any writes are safely rolled back.

#### 1. SQL Existence Assertion (`type: sql_exists`)
Asserts that a given SQL query returns **at least one row**. Typically used to check table presence or state.
```yaml
- name: "Ensure oracle source table exists"
  type: "sql_exists"
  database: "source"  # Can be "source" or "target". 
                      # If source is sharded, automatically runs and asserts on all source shards!
  query: "SELECT 1 FROM all_tables WHERE table_name = 'USERS'"
```

#### 2. SQL Count Assertion (`type: sql_count`)
Asserts that a SQL query returning a single count matches a specified condition. Expected conditions can include comparison operators: `==`, `>=`, `<=`, `>`, `<`, `!=`.
```yaml
- name: "Ensure Postgres target table is empty before migration"
  type: "sql_count"
  database: "target"
  query: "SELECT COUNT(*) FROM users"
  expected: 0  # Can also be string conditions, e.g., "== 0", ">= 100", "< 5"
```
> [!NOTE]
> If `database: "source"` is used on a split/sharded database setup, the engine will query counts from **all** shards, sum them, and assert that the aggregate total matches the expected expression.

#### 3. Row Count Matching Assertion (`type: row_count_match`)
Asserts that the aggregate row counts between the source tables and the target tables are identical. Excellent as a post-migration check.
```yaml
- name: "Verify target counts match source aggregate"
  type: "row_count_match"
  source_query: "SELECT COUNT(*) FROM USERS"       # Query executed on source database(s)
  target_query: "SELECT COUNT(*) FROM users"       # Query executed on target database
```
> [!NOTE]
> On a sharded source setup, `row_count_match` automatically queries every source shard, sums their row counts, and compares the aggregate sum to the target count.

---

### `mapping` Configuration

Contains the heart of the migration data pipeline: query, destination table, and columns mapping list.

```yaml
mapping:
  source_query: "SELECT USER_ID, EMAIL_ADDR, USER_STATUS, CREATION_DATE FROM USERS"
  target_table: "users"
  rollback_strategy: "transaction"  # Currently, "transaction" keeps an open transaction on target
                                    # and rolls it back if any error occurs.

  columns:
    - source: "USER_ID"             # Column name returned in source_query
      target: "id"                  # Destination column name in target_table
      type: "integer"               # Optional cast: 'integer', 'float', 'string', 'boolean', 'timestamp'

    - source: "EMAIL_ADDR"
      target: "email"
      type: "string"
      transform: "lower(value).strip()" # Safe python expression evaluated on the column value

    - source: "USER_STATUS"
      target: "status"
      type: "string"
      # Value translation lookup
      mapping_lookup:
        "A": "active"
        "I": "inactive"
        "S": "suspended"
      transform: "mapping.get(value, 'unknown')" # Evaluates the mapping lookup with default fallback

    - source: "FIRST_NAME"
      target: "full_name"
      type: "string"
      # Combining fields from the full row
      transform: "row.get('FIRST_NAME', '') + ' ' + row.get('LAST_NAME', '')"
```

---

### Python Transformation Sandbox Environment

When evaluating `transform:` expressions, `py-migrate` runs the code inside a highly efficient compiled bytecode sandbox. It removes dangerous `__builtins__` and exposes the following pre-defined context variables and utility functions:

#### Environment Context Variables
- `value`: The exact value of the mapped column for the current row.
- `row`: A dictionary representing the entire original source row (using database casing, case-insensitive helper accessible). Can be used to combine fields.
- `mapping`: The lookup dictionary defined inside `mapping_lookup`.

#### Exposed Python Functions
- `str`, `int`, `float`, `len`, `abs`, `round`, `datetime`
- `lower(x)`: Safe lowercase function, handles `None`.
- `upper(x)`: Safe uppercase function, handles `None`.
- `strip(x)`: Safe whitespace strip function, handles `None`.

#### Dynamic Example Mappings
- **Conditional logic:** `value if value is not None else "N/A"`
- **Concatenation:** `row.get("PREFIX", "") + "-" + str(value)`
- **Date parsing manipulation:** `datetime.strptime(value, "%Y%m%d")`

---

## 3. Practical Multi-Source Merge Example

Below is a complete, working example showing how a split Oracle shard migration is configured.

### Migration Config (`examples/merge_users.yaml`)

```yaml
name: "merge_split_sharded_users"
description: "Merges sharded customer rows from Oracle Shard 1 and Shard 2 into a single Postgres Destination."

source_db:
  - "oracle_shard_1"
  - "oracle_shard_2"
target_db: "postgres_destination"

streaming:
  chunk_size: 2000

pre_migration:
  - name: "Verify users table exists on Shard 1 and Shard 2"
    type: "sql_exists"
    database: "source"
    query: "SELECT 1 FROM all_tables WHERE table_name = 'USERS'"

  - name: "Verify destination table is empty"
    type: "sql_count"
    database: "target"
    query: "SELECT COUNT(*) FROM users"
    expected: 0

mapping:
  source_query: "SELECT CUST_ID, FIRST_NAME, LAST_NAME, CUST_EMAIL, IS_ACTIVE_CODE FROM USERS"
  target_table: "users"
  columns:
    - source: "CUST_ID"
      target: "id"
      type: "integer"
    - source: "FIRST_NAME"
      target: "full_name"
      type: "string"
      transform: "row.get('FIRST_NAME', '').strip() + ' ' + row.get('LAST_NAME', '').strip()"
    - source: "CUST_EMAIL"
      target: "email"
      type: "string"
      transform: "lower(value).strip() if value else None"
    - source: "IS_ACTIVE_CODE"
      target: "is_active"
      type: "boolean"
      mapping_lookup:
        "Y": true
        "N": false
      transform: "mapping.get(value, false)"

post_migration:
  - name: "Verify target table count matches total combined sum of both shards"
    type: "row_count_match"
    source_query: "SELECT COUNT(*) FROM USERS"
    target_query: "SELECT COUNT(*) FROM users"
```
