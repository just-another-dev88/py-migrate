import logging
from typing import Dict, Any, List, Tuple
from pymigrate.database import get_adapter, BaseDatabaseAdapter
from pymigrate.templates import MigrationTemplate
from pymigrate.checklist import ChecklistRunner, CheckResult

logger = logging.getLogger("pymigrate.engine")

class MigrationError(Exception):
    """Exception raised when a migration fails."""
    pass


class MigrationEngine:
    """Orchestrates the lifecycle of a declarative data migration."""

    def __init__(self, template: MigrationTemplate, db_configs: Dict[str, Any]):
        self.template = template
        self.db_configs = db_configs
        self.source_db: BaseDatabaseAdapter = None
        self.target_db: BaseDatabaseAdapter = None

    def _init_databases(self) -> None:
        """Initialize the database adapters based on the template config."""
        if self.source_db is not None and self.target_db is not None:
            logger.debug("Databases already initialized (or injected). Skipping setup.")
            return

        src_name = self.template.source_db
        tgt_name = self.template.target_db

        if src_name not in self.db_configs:
            raise MigrationError(f"Source database configuration '{src_name}' not found in db_config.")
        if tgt_name not in self.db_configs:
            raise MigrationError(f"Target database configuration '{tgt_name}' not found in db_config.")

        try:
            self.source_db = get_adapter(self.db_configs[src_name])
            self.target_db = get_adapter(self.db_configs[tgt_name])
        except Exception as e:
            raise MigrationError(f"Failed to initialize database adapters: {e}")

    def _close_databases(self) -> None:
        """Gracefully close all database connections."""
        if self.source_db:
            try:
                self.source_db.close()
            except Exception as e:
                logger.warning(f"Error closing source database: {e}")
        if self.target_db:
            try:
                self.target_db.close()
            except Exception as e:
                logger.warning(f"Error closing target database: {e}")

    def run(self) -> Dict[str, Any]:
        """Execute the full data migration workflow: pre-checks, stream + transform + batch write, post-checks, commit."""
        self._init_databases()
        
        pre_results: List[CheckResult] = []
        post_results: List[CheckResult] = []
        total_rows_migrated = 0
        
        try:
            # 1. Run Pre-Migration Checklist
            logger.info("Executing Pre-Migration Checklist...")
            runner = ChecklistRunner(self.source_db, self.target_db)
            pre_passed, pre_results = runner.run_checklist(self.template.pre_migration)
            
            for res in pre_results:
                logger.info(str(res))
                
            if not pre_passed:
                raise MigrationError("Pre-migration checklist failed. Migration aborted.")
            
            # 2. Start Migration Transaction on Target
            logger.info("Initiating target database transaction...")
            self.target_db.begin_transaction()
            
            # 3. Stream & Map & Write
            logger.info(f"Streaming data from source query. Chunk size: {self.template.chunk_size}")
            target_table = self.template.target_table
            target_columns = [col.target for col in self.template.columns]
            
            stream = self.source_db.fetch_stream(
                self.template.source_query, 
                chunk_size=self.template.chunk_size
            )
            
            chunk_count = 0
            for chunk in stream:
                chunk_count += 1
                logger.debug(f"Processing chunk {chunk_count} ({len(chunk)} rows)...")
                
                rows_to_insert: List[Tuple] = []
                for row_idx, row in enumerate(chunk):
                    try:
                        # Extract and transform columns
                        mapped_values = []
                        for col in self.template.columns:
                            mapped_values.append(col.map_row(row))
                        rows_to_insert.append(tuple(mapped_values))
                    except Exception as e:
                        raise MigrationError(
                            f"Transformation failed at chunk {chunk_count}, row index {row_idx}: {e}"
                        )
                
                # Write batch to target
                try:
                    self.target_db.write_batch(target_table, target_columns, rows_to_insert)
                    total_rows_migrated += len(rows_to_insert)
                except Exception as e:
                    raise MigrationError(f"Database write failed at chunk {chunk_count}: {e}")

            logger.info(f"Stream migration complete. Total rows processed and written: {total_rows_migrated}")
            
            # 4. Run Post-Migration Checklist
            logger.info("Executing Post-Migration Checklist...")
            # Use the same open connections to run post checks so that uncommitted data is visible (essential for transactional validation!)
            post_passed, post_results = runner.run_checklist(self.template.post_migration)
            
            for res in post_results:
                logger.info(str(res))
                
            if not post_passed:
                raise MigrationError("Post-migration checklist failed. Discarding migrated data.")
            
            # 5. Commit Transaction
            logger.info("All checks passed. Committing transaction...")
            self.target_db.commit()
            logger.info("Migration completed successfully!")
            
            return {
                "success": True,
                "rows_migrated": total_rows_migrated,
                "pre_checklist": pre_results,
                "post_checklist": post_results
            }

        except Exception as e:
            logger.error(f"Migration aborted due to error: {e}")
            logger.info("Rolling back target transaction to restore database state...")
            try:
                if self.target_db:
                    self.target_db.rollback()
            except Exception as rollback_err:
                logger.critical(f"Target rollback failed: {rollback_err}")
            
            # Re-raise as MigrationError
            if isinstance(e, MigrationError):
                raise e
            raise MigrationError(f"Unexpected error during migration execution: {e}")
            
        finally:
            self._close_databases()

    def validate_only(self) -> Dict[str, Any]:
        """Validate template and dry-run connection/pre-check setup without writing any data."""
        self._init_databases()
        try:
            logger.info("Validating database connections...")
            self.source_db.connect()
            self.target_db.connect()
            logger.info("Connections validated successfully.")
            
            logger.info("Running Pre-Migration Checks (Dry-run)...")
            runner = ChecklistRunner(self.source_db, self.target_db)
            pre_passed, pre_results = runner.run_checklist(self.template.pre_migration)
            
            return {
                "success": pre_passed,
                "pre_checklist": pre_results
            }
        except Exception as e:
            raise MigrationError(f"Validation dry-run failed: {e}")
        finally:
            self._close_databases()
