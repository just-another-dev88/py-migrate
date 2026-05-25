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
    assert "Aggregate Source rows: 42, Target rows: 30" in res.message


def test_checklist_multi_source():
    # Setup two source shard adapters
    src1 = MockDatabaseAdapter()
    src2 = MockDatabaseAdapter()
    tgt = MockDatabaseAdapter()
    
    runner = ChecklistRunner([src1, src2], tgt)

    # 1. Test sql_exists on multi-source (passes only if both contain data)
    src1.mock_fetch_results = [{"1": 1}]
    src2.mock_fetch_results = [{"1": 1}]
    res = runner.run_check({
        "name": "Check shard tables",
        "type": "sql_exists",
        "database": "source",
        "query": "SELECT 1"
    })
    assert res.passed is True

    # Test sql_exists fail on one shard
    src2.mock_fetch_results = []
    res = runner.run_check({
        "name": "Check shard tables",
        "type": "sql_exists",
        "database": "source",
        "query": "SELECT 1"
    })
    assert res.passed is False
    assert "source[1] (0 rows)" in res.message

    # 2. Test sql_count aggregates (sums) counts across both shards
    src1.mock_fetch_results = [{"COUNT": 30}]
    src2.mock_fetch_results = [{"COUNT": 20}]
    res = runner.run_check({
        "name": "Aggregated count",
        "type": "sql_count",
        "database": "source",
        "query": "SELECT COUNT(*)",
        "expected": "== 50"
    })
    assert res.passed is True

    # Aggregated count comparison operator
    res = runner.run_check({
        "name": "Aggregated count",
        "type": "sql_count",
        "database": "source",
        "query": "SELECT COUNT(*)",
        "expected": ">= 45"
    })
    assert res.passed is True

    # Aggregated count failing comparison
    res = runner.run_check({
        "name": "Aggregated count",
        "type": "sql_count",
        "database": "source",
        "query": "SELECT COUNT(*)",
        "expected": "== 100"
    })
    assert res.passed is False
    assert "Expected: == 100, Got: 50" in res.message

    # 3. Test row_count_match sums all sources
    tgt.mock_fetch_results = [{"COUNT": 50}]
    res = runner.run_check({
        "name": "Match target with aggregate sources",
        "type": "row_count_match",
        "source_query": "SELECT COUNT(*) FROM src",
        "target_query": "SELECT COUNT(*) FROM tgt"
    })
    assert res.passed is True

