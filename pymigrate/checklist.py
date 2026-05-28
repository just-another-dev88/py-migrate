import logging
from typing import Dict, Any, List, Tuple
from pymigrate.database import BaseDatabaseAdapter

logger = logging.getLogger("pymigrate.checklist")

class CheckResult:
    """Represents the outcome of a single checklist validation."""

    def __init__(self, name: str, check_type: str, passed: bool, message: str = ""):
        self.name = name
        self.check_type = check_type
        self.passed = passed
        self.message = message

    def __str__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        msg = f" - {self.message}" if self.message else ""
        return f"[{status}] {self.name} ({self.check_type}){msg}"


class ChecklistRunner:
    """Executes pre-migration and post-migration assertions against database adapters."""

    def __init__(self, source_dbs: Any, target_dbs: Any):
        if isinstance(source_dbs, list):
            self.source_dbs = source_dbs
        else:
            self.source_dbs = [source_dbs]
        # Keep a self.source_db pointing to the first source for backward compatibility
        self.source_db = self.source_dbs[0] if self.source_dbs else None

        if isinstance(target_dbs, list):
            self.target_dbs = target_dbs
        else:
            self.target_dbs = [target_dbs] if target_dbs else []
        # Keep a self.target_db pointing to the first target for backward compatibility
        self.target_db = self.target_dbs[0] if self.target_dbs else None

    def _get_db(self, db_ref: str) -> BaseDatabaseAdapter:
        if not db_ref:
            raise ValueError("Check configuration is missing 'database' parameter.")
        ref = db_ref.lower()
        if ref == "source":
            return self.source_db
        elif ref == "target":
            return self.target_db
        else:
            raise ValueError(f"Unknown database reference in checklist: {db_ref}. Must be 'source' or 'target'.")

    def run_check(self, config: Dict[str, Any]) -> CheckResult:
        name = config.get("name", "Unnamed Check")
        check_type = config.get("type")
        
        if not check_type:
            return CheckResult(name, "unknown", False, "Missing check 'type' parameter.")

        check_type = check_type.lower()
        try:
            if check_type == "sql_exists":
                return self._run_sql_exists(name, config)
            elif check_type == "sql_count":
                return self._run_sql_count(name, config)
            elif check_type == "row_count_match":
                return self._run_row_count_match(name, config)
            else:
                return CheckResult(name, check_type, False, f"Unsupported check type: {check_type}")
        except Exception as e:
            logger.error(f"Error running check '{name}': {e}", exc_info=True)
            return CheckResult(name, check_type, False, f"Execution failed: {str(e)}")

    def run_checklist(self, checklist_configs: List[Dict[str, Any]]) -> Tuple[bool, List[CheckResult]]:
        """Run all check configurations. Returns (all_passed, list_of_results)."""
        results = []
        all_passed = True
        
        for config in checklist_configs:
            res = self.run_check(config)
            results.append(res)
            if not res.passed:
                all_passed = False
                
        return all_passed, results

    def _run_sql_exists(self, name: str, config: Dict[str, Any]) -> CheckResult:
        db_ref = config.get("database")
        query = config.get("query")
        
        if not query:
            return CheckResult(name, "sql_exists", False, "Missing SQL 'query' in config.")
            
        if db_ref and db_ref.lower() == "source":
            failures = []
            for idx, db in enumerate(self.source_dbs):
                try:
                    results = db.fetch_all(query)
                    if len(results) == 0:
                        failures.append(f"source[{idx}] (0 rows)")
                except Exception as e:
                    failures.append(f"source[{idx}] failed: {e}")
            passed = len(failures) == 0
            if passed:
                message = ""
            else:
                if len(self.source_dbs) == 1:
                    message = "Query returned 0 rows (expected >= 1 row)."
                    if "failed" in failures[0]:
                        message = failures[0]
                else:
                    message = f"SQL existence check failed on: {', '.join(failures)}"
            return CheckResult(name, "sql_exists", passed, message)
        elif db_ref and db_ref.lower() == "target":
            failures = []
            for idx, db in enumerate(self.target_dbs):
                try:
                    results = db.fetch_all(query)
                    if len(results) == 0:
                        failures.append(f"target[{idx}] (0 rows)")
                except Exception as e:
                    failures.append(f"target[{idx}] failed: {e}")
            passed = len(failures) == 0
            if passed:
                message = ""
            else:
                if len(self.target_dbs) == 1:
                    message = "Query returned 0 rows (expected >= 1 row)."
                    if "failed" in failures[0]:
                        message = failures[0]
                else:
                    message = f"SQL existence check failed on: {', '.join(failures)}"
            return CheckResult(name, "sql_exists", passed, message)
        else:
            db = self._get_db(db_ref)
            results = db.fetch_all(query)
            passed = len(results) > 0
            message = "" if passed else "Query returned 0 rows (expected >= 1 row)."
            return CheckResult(name, "sql_exists", passed, message)

    def _run_sql_count(self, name: str, config: Dict[str, Any]) -> CheckResult:
        db_ref = config.get("database")
        query = config.get("query")
        expected_val = config.get("expected")
        
        if not query:
            return CheckResult(name, "sql_count", False, "Missing SQL 'query' in config.")
        if expected_val is None:
            return CheckResult(name, "sql_count", False, "Missing 'expected' value in config.")

        actual_val = 0
        if db_ref and db_ref.lower() == "source":
            for idx, db in enumerate(self.source_dbs):
                results = db.fetch_all(query)
                if not results:
                    return CheckResult(name, "sql_count", False, f"Query returned no rows on source[{idx}].")
                first_row = results[0]
                val = list(first_row.values())[0]
                try:
                    actual_val += int(val)
                except (ValueError, TypeError):
                    return CheckResult(name, "sql_count", False, f"Returned count is not an integer on source[{idx}]: {val}")
        elif db_ref and db_ref.lower() == "target":
            for idx, db in enumerate(self.target_dbs):
                results = db.fetch_all(query)
                if not results:
                    return CheckResult(name, "sql_count", False, f"Query returned no rows on target[{idx}].")
                first_row = results[0]
                val = list(first_row.values())[0]
                try:
                    actual_val += int(val)
                except (ValueError, TypeError):
                    return CheckResult(name, "sql_count", False, f"Returned count is not an integer on target[{idx}]: {val}")
        else:
            db = self._get_db(db_ref)
            results = db.fetch_all(query)
            if not results:
                return CheckResult(name, "sql_count", False, "Query returned no rows to count.")
            first_row = results[0]
            val = list(first_row.values())[0]
            try:
                actual_val = int(val)
            except (ValueError, TypeError):
                return CheckResult(name, "sql_count", False, f"Returned count is not an integer: {val}")
            
        # Parse comparison expression (e.g. expected: ">= 5" or just integer expected: 0)
        passed = False
        op = "=="
        expected_parsed = None
        
        if isinstance(expected_val, str):
            val_str = expected_val.strip()
            for possible_op in (">=", "<=", "==", "!=", ">", "<", "="):
                if val_str.startswith(possible_op):
                    op = possible_op
                    val_part = val_str[len(possible_op):].strip()
                    try:
                        expected_parsed = int(val_part)
                    except ValueError:
                        return CheckResult(name, "sql_count", False, f"Invalid expected value integer: {val_part}")
                    break
            if expected_parsed is None:
                try:
                    expected_parsed = int(val_str)
                except ValueError:
                    return CheckResult(name, "sql_count", False, f"Invalid expected value: {expected_val}")
        else:
            try:
                expected_parsed = int(expected_val)
            except (ValueError, TypeError):
                return CheckResult(name, "sql_count", False, f"Invalid expected value: {expected_val}")

        # Perform comparison
        if op == "==" or op == "=":
            passed = actual_val == expected_parsed
        elif op == ">=":
            passed = actual_val >= expected_parsed
        elif op == "<=":
            passed = actual_val <= expected_parsed
        elif op == ">":
            passed = actual_val > expected_parsed
        elif op == "<":
            passed = actual_val < expected_parsed
        elif op == "!=":
            passed = actual_val != expected_parsed

        message = "" if passed else f"Expected: {op} {expected_parsed}, Got: {actual_val}"
        return CheckResult(name, "sql_count", passed, message)

    def _run_row_count_match(self, name: str, config: Dict[str, Any]) -> CheckResult:
        src_query = config.get("source_query")
        tgt_query = config.get("target_query")
        
        if not src_query:
            return CheckResult(name, "row_count_match", False, "Missing 'source_query' in config.")
        if not tgt_query:
            return CheckResult(name, "row_count_match", False, "Missing 'target_query' in config.")

        # Aggregate row counts across all sources
        src_count = 0
        for idx, db in enumerate(self.source_dbs):
            src_res = db.fetch_all(src_query)
            if src_res:
                try:
                    src_count += int(list(src_res[0].values())[0])
                except Exception as e:
                    return CheckResult(name, "row_count_match", False, f"Error getting count from source[{idx}]: {e}")

        # Get count from target databases and check that each matches src_count
        failures = []
        tgt_count = 0
        for idx, db in enumerate(self.target_dbs):
            try:
                tgt_res = db.fetch_all(tgt_query)
                tgt_count = 0
                if tgt_res:
                    tgt_count = int(list(tgt_res[0].values())[0])
                if src_count != tgt_count:
                    failures.append(f"target[{idx}] ({tgt_count} rows)")
            except Exception as e:
                failures.append(f"target[{idx}] failed: {e}")

        passed = len(failures) == 0
        if passed:
            message = ""
        else:
            if len(self.target_dbs) == 1:
                message = f"Aggregate Source rows: {src_count}, Target rows: {tgt_count}"
            else:
                message = f"Aggregate Source rows: {src_count}, Target mismatch details: {', '.join(failures)}"
        return CheckResult(name, "row_count_match", passed, message)
