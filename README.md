# py-migrate

A declarative data migration tool built in Python. This tool facilitates memory-efficient database migrations (e.g. from Oracle Thin to PostgreSQL) using declarative mapping templates defined in YAML.

## Features

- **Declarative Mappings:** Define mappings, lookup dictionaries, and value transformations in YAML.
- **Oracle & Postgres Support:** Seamless connection to PostgreSQL (using `psycopg2`) and Oracle Thin Mode (using `oracledb`).
- **Pre/Post-Migration Checklists:** Custom SQL assertions to verify schemas, counts, and conditions before and after migration.
- **Streaming Execution:** High-efficiency row streaming using batch fetches and database server-side cursors to maintain a small memory footprint.
- **Atomic Transactional Rollbacks:** Single-transaction writes on the target database with explicit rollback if any errors or assertions fail.

## Installation

```bash
pip install -e .
```

## Quick Start

1. Define database connections in `config.yaml`:
   ```yaml
   databases:
     oracle_source:
       type: "oracle"
       user: "system"
       password: "password"
       dsn: "localhost:1521/XEPDB1"
     postgres_target:
       type: "postgres"
       user: "postgres"
       password: "password"
       host: "localhost"
       port: 5432
       database: "postgres"
   ```

2. Run a migration template:
   ```bash
   pymigrate run examples/user_migration.yaml --config examples/db_config.yaml
   ```
