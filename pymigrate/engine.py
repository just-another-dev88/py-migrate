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
        self.source_dbs: List[BaseDatabaseAdapter] = []
        self.source_db: BaseDatabaseAdapter = None
        self.target_dbs: List[BaseDatabaseAdapter] = []
        self.target_db: BaseDatabaseAdapter = None

    def _init_databases(self) -> None:
        """Initialize the database adapters based on the template config."""
        # Sync list and single reference if mock instances were injected during testing
        if self.source_db is not None and (not self.source_dbs or self.source_dbs[0] is not self.source_db):
            self.source_dbs = [self.source_db]
        elif self.source_dbs and self.source_db is None:
            self.source_db = self.source_dbs[0]

        if self.target_db is not None and (not self.target_dbs or self.target_dbs[0] is not self.target_db):
            self.target_dbs = [self.target_db]
        elif self.target_dbs and self.target_db is None:
            self.target_db = self.target_dbs[0]

        if self.source_dbs and self.target_dbs:
            logger.debug("Databases already initialized (or injected). Skipping setup.")
            return

        src_names = self.template.source_dbs
        tgt_names = self.template.target_dbs

        for name in src_names:
            if name not in self.db_configs:
                raise MigrationError(f"Source database configuration '{name}' not found in db_config.")
        for name in tgt_names:
            if name not in self.db_configs:
                raise MigrationError(f"Target database configuration '{name}' not found in db_config.")

        try:
            self.source_dbs = [get_adapter(self.db_configs[name]) for name in src_names]
            self.source_db = self.source_dbs[0] if self.source_dbs else None
            self.target_dbs = [get_adapter(self.db_configs[name]) for name in tgt_names]
            self.target_db = self.target_dbs[0] if self.target_dbs else None
        except Exception as e:
            raise MigrationError(f"Failed to initialize database adapters: {e}")

    def _close_databases(self) -> None:
        """Gracefully close all database connections."""
        if self.source_dbs:
            for db in self.source_dbs:
                try:
                    db.close()
                except Exception as e:
                    logger.warning(f"Error closing source database: {e}")
        if self.target_dbs:
            for db in self.target_dbs:
                try:
                    db.close()
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
            runner = ChecklistRunner(self.source_dbs, self.target_dbs)
            pre_passed, pre_results = runner.run_checklist(self.template.pre_migration)
            
            for res in pre_results:
                logger.info(str(res))
                
            if not pre_passed:
                raise MigrationError("Pre-migration checklist failed. Migration aborted.")
            
            # 2. Start Migration Transaction on Target
            logger.info("Initiating target database transactions...")
            for tgt in self.target_dbs:
                tgt.begin_transaction()
            
            # 3. Stream & Map & Write
            target_table = self.template.target_table
            target_columns = [col.target for col in self.template.columns]
            
            chunk_count = 0
            for src_idx, src_db in enumerate(self.source_dbs):
                logger.info(f"Streaming data from source [{src_idx}]... Chunk size: {self.template.chunk_size}")
                stream = src_db.fetch_stream(
                    self.template.source_query, 
                    chunk_size=self.template.chunk_size
                )
                
                for chunk in stream:
                    chunk_count += 1
                    logger.debug(f"Processing chunk {chunk_count} from source [{src_idx}] ({len(chunk)} rows)...")
                    
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
                                f"Transformation failed at source [{src_idx}], chunk {chunk_count}, row index {row_idx}: {e}"
                            )
                    
                    # Write batch to target
                    try:
                        for tgt in self.target_dbs:
                            tgt.write_batch(target_table, target_columns, rows_to_insert)
                        total_rows_migrated += len(rows_to_insert)
                    except Exception as e:
                        raise MigrationError(f"Database write failed at source [{src_idx}], chunk {chunk_count}: {e}")

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
            logger.info("All checks passed. Committing transactions...")
            for tgt in self.target_dbs:
                tgt.commit()
            logger.info("Migration completed successfully!")
            
            return {
                "success": True,
                "rows_migrated": total_rows_migrated,
                "pre_checklist": pre_results,
                "post_checklist": post_results
            }

        except Exception as e:
            logger.error(f"Migration aborted due to error: {e}")
            logger.info("Rolling back target transactions to restore database state...")
            if self.target_dbs:
                for tgt in self.target_dbs:
                    try:
                        tgt.rollback()
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
            for idx, db in enumerate(self.source_dbs):
                db.connect()
                logger.info(f"Source [{idx}] connection validated.")
            for idx, db in enumerate(self.target_dbs):
                db.connect()
                logger.info(f"Target [{idx}] connection validated successfully.")
            
            logger.info("Running Pre-Migration Checks (Dry-run)...")
            runner = ChecklistRunner(self.source_dbs, self.target_dbs)
            pre_passed, pre_results = runner.run_checklist(self.template.pre_migration)
            
            return {
                "success": pre_passed,
                "pre_checklist": pre_results
            }
        except Exception as e:
            raise MigrationError(f"Validation dry-run failed: {e}")
        finally:
            self._close_databases()
