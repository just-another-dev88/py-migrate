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

    def __init__(self, source_db: BaseDatabaseAdapter, target_db: BaseDatabaseAdapter):
        self.source_db = source_db
        self.target_db = target_db

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

        db = self._get_db(db_ref)
        results = db.fetch_all(query)
        
        if not results:
            return CheckResult(name, "sql_count", False, "Query returned no rows to count.")
            
        # Extract first value from the first row of result set
        first_row = results[0]
        actual_val = list(first_row.values())[0]
        
        try:
            actual_val = int(actual_val)
        except (ValueError, TypeError):
            return CheckResult(name, "sql_count", False, f"Returned count is not an integer: {actual_val}")
            
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

        src_res = self.source_db.fetch_all(src_query)
        tgt_res = self.target_db.fetch_all(tgt_query)
        
        if not src_res or not tgt_res:
            src_count = 0 if not src_res else int(list(src_res[0].values())[0])
            tgt_count = 0 if not tgt_res else int(list(tgt_res[0].values())[0])
        else:
            try:
                src_count = int(list(src_res[0].values())[0])
            except Exception:
                src_count = 0
            try:
                tgt_count = int(list(tgt_res[0].values())[0])
            except Exception:
                tgt_count = 0

        passed = src_count == tgt_count
        message = "" if passed else f"Source rows: {src_count}, Target rows: {tgt_count}"
        return CheckResult(name, "row_count_match", passed, message)
