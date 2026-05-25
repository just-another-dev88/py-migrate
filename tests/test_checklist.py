import pytest
from pymigrate.checklist import ChecklistRunner, CheckResult
from pymigrate.database import BaseDatabaseAdapter

class MockDatabaseAdapter(BaseDatabaseAdapter):
    def __init__(self):
        super().__init__({})
        self.mock_fetch_results = []
        self.connect_called = False
        self.close_called = False
        self.begin_called = False
        self.commit_called = False
        self.rollback_called = False

    def connect(self):
        self.connect_called = True

    def close(self):
        self.close_called = True

    def execute(self, query, params=None):
        pass

    def fetch_all(self, query, params=None):
        return self.mock_fetch_results

    def fetch_stream(self, query, params=None, chunk_size=1000):
        yield self.mock_fetch_results

    def write_batch(self, table_name, columns, rows):
        return len(rows)

    def begin_transaction(self):
        self.begin_called = True

    def commit(self):
        self.commit_called = True

    def rollback(self):
        self.rollback_called = True


def test_sql_exists():
    src_db = MockDatabaseAdapter()
    tgt_db = MockDatabaseAdapter()
    runner = ChecklistRunner(src_db, tgt_db)

    # Test exists true
    src_db.mock_fetch_results = [{"1": 1}]
    res = runner.run_check({
        "name": "Check source table",
        "type": "sql_exists",
        "database": "source",
        "query": "SELECT 1"
    })
    assert res.passed is True

    # Test exists false
    src_db.mock_fetch_results = []
    res = runner.run_check({
        "name": "Check source table",
        "type": "sql_exists",
        "database": "source",
        "query": "SELECT 1"
    })
    assert res.passed is False
    assert "returned 0 rows" in res.message


def test_sql_count():
    src_db = MockDatabaseAdapter()
    tgt_db = MockDatabaseAdapter()
    runner = ChecklistRunner(src_db, tgt_db)

    # Exact match integer
    src_db.mock_fetch_results = [{"COUNT": 5}]
    res = runner.run_check({
        "name": "Count check",
        "type": "sql_count",
        "database": "source",
        "query": "SELECT COUNT(*)",
        "expected": 5
    })
    assert res.passed is True

    # Logical operator >=
    src_db.mock_fetch_results = [{"COUNT": 10}]
    res = runner.run_check({
        "name": "Count check",
        "type": "sql_count",
        "database": "source",
        "query": "SELECT COUNT(*)",
        "expected": ">= 8"
    })
    assert res.passed is True

    # Logical operator fail
    res = runner.run_check({
        "name": "Count check",
        "type": "sql_count",
        "database": "source",
        "query": "SELECT COUNT(*)",
        "expected": "< 5"
    })
    assert res.passed is False
    assert "Expected: < 5, Got: 10" in res.message


def test_row_count_match():
    src_db = MockDatabaseAdapter()
    tgt_db = MockDatabaseAdapter()
    runner = ChecklistRunner(src_db, tgt_db)

    # Matching row counts
    src_db.mock_fetch_results = [{"COUNT": 42}]
    tgt_db.mock_fetch_results = [{"COUNT": 42}]
    res = runner.run_check({
        "name": "Match rows",
        "type": "row_count_match",
        "source_query": "SELECT COUNT(*) FROM src",
        "target_query": "SELECT COUNT(*) FROM tgt"
    })
    assert res.passed is True

    # Mis-matching row counts
    tgt_db.mock_fetch_results = [{"COUNT": 30}]
    res = runner.run_check({
        "name": "Match rows",
        "type": "row_count_match",
        "source_query": "SELECT COUNT(*) FROM src",
        "target_query": "SELECT COUNT(*) FROM tgt"
    })
    assert res.passed is False
    assert "Source rows: 42, Target rows: 30" in res.message
