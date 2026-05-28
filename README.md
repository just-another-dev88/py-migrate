# py-migrate

A declarative data migration tool built in Python. This tool facilitates memory-efficient database migrations (e.g. from Oracle Thin to PostgreSQL) using declarative mapping templates defined in YAML.

## Features

- **Declarative Mappings:** Define mappings, lookup dictionaries, and value transformations in YAML.
- **Oracle & Postgres Support:** Seamless connection to PostgreSQL (using `psycopg2`) and Oracle Thin Mode (using `oracledb`).
- **Flexible Database Topologies:** Supports Single-to-Single, Multi-Source (aggregation from shards), and Multi-Target (replicated parallel mirrors) configurations using `source_dbs` and `target_dbs` YAML lists.
- **Pre/Post-Migration Checklists:** Custom SQL assertions to verify schemas, counts, and conditions before and after migration across all configured source and target databases.
- **Streaming Execution:** High-efficiency row streaming using batch fetches and database server-side cursors to maintain a small memory footprint.
- **Atomic Transactional Rollbacks:** Multi-transaction synchronization across all targets with explicit rollback on all instances if any errors or assertions fail.

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

## Database Topology Examples

The `examples/` directory contains templates demonstrating various migration topologies:
- **[single_to_single.yaml](file:///d:/Coding/py-migrate/examples/single_to_single.yaml)**: Replicates one source database to one target database.
- **[multi_to_single.yaml](file:///d:/Coding/py-migrate/examples/multi_to_single.yaml)**: Aggregates and streams data from multiple database shards into a single destination database.
- **[single_to_multi.yaml](file:///d:/Coding/py-migrate/examples/single_to_multi.yaml)**: Mirror and write data from a single source database across multiple replica targets simultaneously.
- **[multi_to_multi.yaml](file:///d:/Coding/py-migrate/examples/multi_to_multi.yaml)**: Full multi-source shard aggregation replicated dynamically to multiple target database mirrors.

