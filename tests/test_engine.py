import pytest
import os
import tempfile
from pymigrate.engine import MigrationEngine, MigrationError
from pymigrate.templates import MigrationTemplate
from tests.test_checklist import MockDatabaseAdapter

class TrackingDatabaseAdapter(MockDatabaseAdapter):
    """Database adapter that tracks execution methods and written rows."""
    def __init__(self):
        super().__init__()
        self.written_batches = []
        self.stream_data = []

    def fetch_stream(self, query, params=None, chunk_size=1000):
        self.connect()
        for batch in self.stream_data:
            yield batch

    def write_batch(self, table_name, columns, rows):
        self.written_batches.append((table_name, columns, rows))
        return len(rows)


def get_test_template_path(pre_checks=None, post_checks=None, multi_source=False, multi_target=False):
    pre_yaml = ""
    if pre_checks:
        pre_yaml = "pre_migration:\n" + "\n".join([f"  - name: {c['name']}\n    type: {c['type']}\n    database: {c['database']}\n    query: '{c['query']}'" for c in pre_checks])
        
    post_yaml = ""
    if post_checks:
        post_yaml = "post_migration:\n" + "\n".join([f"  - name: {c['name']}\n    type: {c['type']}\n    database: {c['database']}\n    query: '{c['query']}'\n    expected: {c['expected']}" for c in post_checks])

    source_db_block = 'source_db:\n  - "src1"\n  - "src2"' if multi_source else 'source_db: "src"'
    target_db_block = 'target_db:\n  - "tgt1"\n  - "tgt2"' if multi_target else 'target_db: "tgt"'

    yaml_content = f"""
name: "test_migration"
{source_db_block}
{target_db_block}
streaming:
  chunk_size: 2
{pre_yaml}
{post_yaml}
mapping:
  source_query: "SELECT ID, NAME FROM src_table"
  target_table: "users"
  columns:
    - source: "ID"
      target: "id"
      type: "integer"
    - source: "NAME"
      target: "name"
      transform: "lower(value)"
"""
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
    f.write(yaml_content)
    f.close()
    return f.name


def test_migration_engine_success():
    template_path = get_test_template_path()
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src": {"type": "postgres"}, "tgt": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        # Inject tracking adapters
        src_db = TrackingDatabaseAdapter()
        tgt_db = TrackingDatabaseAdapter()
        engine.source_db = src_db
        engine.target_db = tgt_db

        # Set up source data
        src_db.stream_data = [
            [{"ID": 1, "NAME": "ALICE"}, {"ID": 2, "NAME": "BOB"}],
            [{"ID": 3, "NAME": "CHARLIE"}]
        ]
        
        # Execute migration
        res = engine.run()
        
        assert res["success"] is True
        assert res["rows_migrated"] == 3
        
        # Verify transaction boundaries
        assert tgt_db.begin_called is True
        assert tgt_db.commit_called is True
        assert tgt_db.rollback_called is False
        
        # Verify written data mapping
        assert len(tgt_db.written_batches) == 2
        batch1 = tgt_db.written_batches[0]
        assert batch1[0] == "users"
        assert batch1[1] == ["id", "name"]
        assert batch1[2] == [(1, "alice"), (2, "bob")]

        batch2 = tgt_db.written_batches[1]
        assert batch2[2] == [(3, "charlie")]
        
    finally:
        os.remove(template_path)


def test_migration_engine_rollback_on_mapping_error():
    template_path = get_test_template_path()
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src": {"type": "postgres"}, "tgt": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        src_db = TrackingDatabaseAdapter()
        tgt_db = TrackingDatabaseAdapter()
        engine.source_db = src_db
        engine.target_db = tgt_db

        # Let's mock data containing an invalid type for INT transformation, triggering error
        src_db.stream_data = [
            [{"ID": 1, "NAME": "ALICE"}],
            [{"ID": "NOT_AN_INT", "NAME": "BOB"}]
        ]
        
        # Execute migration and assert failure
        with pytest.raises(MigrationError) as exc_info:
            engine.run()
            
        assert "Transformation failed" in str(exc_info.value)
        
        # Verify transaction rolled back
        assert tgt_db.begin_called is True
        assert tgt_db.commit_called is False
        assert tgt_db.rollback_called is True
        
    finally:
        os.remove(template_path)


def test_migration_engine_rollback_on_post_check_failure():
    # Let's specify a post-migration check that expects 100 rows, but we only migrate 2
    post_checks = [{
        "name": "Check row count",
        "type": "sql_count",
        "database": "target",
        "query": "SELECT COUNT(*) FROM users",
        "expected": 100
    }]
    template_path = get_test_template_path(post_checks=post_checks)
    
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src": {"type": "postgres"}, "tgt": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        src_db = TrackingDatabaseAdapter()
        tgt_db = TrackingDatabaseAdapter()
        engine.source_db = src_db
        engine.target_db = tgt_db

        # We will return 2 records when post_check queries the target db for row count
        tgt_db.mock_fetch_results = [{"COUNT": 2}]
        src_db.stream_data = [
            [{"ID": 1, "NAME": "ALICE"}, {"ID": 2, "NAME": "BOB"}]
        ]
        
        # Execute migration and expect failure from post checks
        with pytest.raises(MigrationError) as exc_info:
            engine.run()
            
        assert "Post-migration checklist failed" in str(exc_info.value)
        
        # Verify transaction rolled back even though streaming/writing succeeded
        assert tgt_db.begin_called is True
        assert tgt_db.commit_called is False
        assert tgt_db.rollback_called is True
        
    finally:
        os.remove(template_path)


def test_migration_engine_multi_source_success():
    template_path = get_test_template_path(multi_source=True)
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src1": {"type": "postgres"}, "src2": {"type": "postgres"}, "tgt": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        src_db1 = TrackingDatabaseAdapter()
        src_db2 = TrackingDatabaseAdapter()
        tgt_db = TrackingDatabaseAdapter()
        
        engine.source_dbs = [src_db1, src_db2]
        engine.source_db = src_db1
        engine.target_db = tgt_db

        # Source 1 data
        src_db1.stream_data = [
            [{"ID": 1, "NAME": "ALICE"}, {"ID": 2, "NAME": "BOB"}]
        ]
        # Source 2 data
        src_db2.stream_data = [
            [{"ID": 3, "NAME": "CHARLIE"}]
        ]
        
        res = engine.run()
        
        assert res["success"] is True
        assert res["rows_migrated"] == 3
        
        # Verify transaction boundary on single target
        assert tgt_db.begin_called is True
        assert tgt_db.commit_called is True
        assert tgt_db.rollback_called is False
        
        # Verify written data has batches from both databases sequentially
        assert len(tgt_db.written_batches) == 2
        
        batch1 = tgt_db.written_batches[0]
        assert batch1[2] == [(1, "alice"), (2, "bob")]
        
        batch2 = tgt_db.written_batches[1]
        assert batch2[2] == [(3, "charlie")]
        
    finally:
        os.remove(template_path)


def test_migration_engine_multi_source_failure_rollback():
    template_path = get_test_template_path(multi_source=True)
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src1": {"type": "postgres"}, "src2": {"type": "postgres"}, "tgt": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        src_db1 = TrackingDatabaseAdapter()
        src_db2 = TrackingDatabaseAdapter()
        tgt_db = TrackingDatabaseAdapter()
        
        engine.source_dbs = [src_db1, src_db2]
        engine.source_db = src_db1
        engine.target_db = tgt_db

        # Shard 1 succeeded
        src_db1.stream_data = [
            [{"ID": 1, "NAME": "ALICE"}]
        ]
        # Shard 2 fails due to mapping transform value type error
        src_db2.stream_data = [
            [{"ID": "NOT_AN_INT", "NAME": "BOB"}]
        ]
        
        with pytest.raises(MigrationError) as exc_info:
            engine.run()
            
        assert "Transformation failed" in str(exc_info.value)
        
        # Verify single target rolled back entirely, throwing away Shard 1 and Shard 2 writes
        assert tgt_db.begin_called is True
        assert tgt_db.commit_called is False
        assert tgt_db.rollback_called is True
        
    finally:
        os.remove(template_path)


def test_migration_engine_multi_target_success():
    template_path = get_test_template_path(multi_target=True)
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src": {"type": "postgres"}, "tgt1": {"type": "postgres"}, "tgt2": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        src_db = TrackingDatabaseAdapter()
        tgt_db1 = TrackingDatabaseAdapter()
        tgt_db2 = TrackingDatabaseAdapter()
        
        engine.source_db = src_db
        engine.source_dbs = [src_db]
        engine.target_dbs = [tgt_db1, tgt_db2]
        engine.target_db = tgt_db1

        src_db.stream_data = [
            [{"ID": 1, "NAME": "ALICE"}, {"ID": 2, "NAME": "BOB"}]
        ]
        
        res = engine.run()
        
        assert res["success"] is True
        assert res["rows_migrated"] == 2
        
        # Both targets began and committed transactions successfully
        for tgt_db in [tgt_db1, tgt_db2]:
            assert tgt_db.begin_called is True
            assert tgt_db.commit_called is True
            assert tgt_db.rollback_called is False
            
            assert len(tgt_db.written_batches) == 1
            batch = tgt_db.written_batches[0]
            assert batch[0] == "users"
            assert batch[1] == ["id", "name"]
            assert batch[2] == [(1, "alice"), (2, "bob")]
            
    finally:
        os.remove(template_path)


def test_migration_engine_multi_target_failure_rollback():
    template_path = get_test_template_path(multi_target=True)
    try:
        template = MigrationTemplate(template_path)
        db_configs = {"src": {"type": "postgres"}, "tgt1": {"type": "postgres"}, "tgt2": {"type": "postgres"}}
        
        engine = MigrationEngine(template, db_configs)
        engine._init_databases()
        
        src_db = TrackingDatabaseAdapter()
        tgt_db1 = TrackingDatabaseAdapter()
        tgt_db2 = TrackingDatabaseAdapter()
        
        engine.source_db = src_db
        engine.source_dbs = [src_db]
        engine.target_dbs = [tgt_db1, tgt_db2]
        engine.target_db = tgt_db1

        # Shard fails due to transform mapping error
        src_db.stream_data = [
            [{"ID": "INVALID_INT", "NAME": "ALICE"}]
        ]
        
        with pytest.raises(MigrationError) as exc_info:
            engine.run()
            
        assert "Transformation failed" in str(exc_info.value)
        
        # Both targets are rolled back
        for tgt_db in [tgt_db1, tgt_db2]:
            assert tgt_db.begin_called is True
            assert tgt_db.commit_called is False
            assert tgt_db.rollback_called is True
            
    finally:
        os.remove(template_path)
