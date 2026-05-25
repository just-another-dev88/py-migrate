import pytest
import os
import tempfile
from pymigrate.templates import MigrationTemplate, ColumnMapping, FieldTransformer, ValidationError

def test_field_transformer():
    # Test simple lower and strip
    tf = FieldTransformer("lower(value).strip()")
    assert tf.evaluate("  HELLO WORLD  ", {}) == "hello world"

    # Test mapping lookup
    tf = FieldTransformer("mapping.get(value, 'default')", {"A": "active"})
    assert tf.evaluate("A", {}) == "active"
    assert tf.evaluate("B", {}) == "default"

    # Test full row access
    tf = FieldTransformer("row['first'] + ' ' + row['last']")
    assert tf.evaluate("", {"first": "Alice", "last": "Smith"}) == "Alice Smith"

    # Test invalid syntax
    with pytest.raises(ValidationError):
        FieldTransformer("value + @invalid_syntax")


def test_column_mapping_coercion():
    # Int coercion
    col = ColumnMapping({"source": "SRC", "target": "TGT", "type": "integer"})
    assert col.convert_type("123") == 123
    assert col.convert_type(12.5) == 12
    assert col.convert_type(None) is None

    # Float coercion
    col = ColumnMapping({"source": "SRC", "target": "TGT", "type": "float"})
    assert col.convert_type("123.45") == 123.45

    # Bool coercion
    col = ColumnMapping({"source": "SRC", "target": "TGT", "type": "bool"})
    assert col.convert_type("True") is True
    assert col.convert_type("yes") is True
    assert col.convert_type("0") is False

    # Timestamp coercion
    col = ColumnMapping({"source": "SRC", "target": "TGT", "type": "timestamp"})
    from datetime import datetime
    dt = col.convert_type("2026-05-25 08:00:00")
    assert isinstance(dt, datetime)
    assert dt.year == 2026
    assert dt.month == 5
    assert dt.day == 25


def test_column_mapping_row_extraction():
    col = ColumnMapping({
        "source": "FIRST_NAME", 
        "target": "first_name", 
        "transform": "lower(value)"
    })
    
    # Exact match key
    assert col.map_row({"FIRST_NAME": "Alice"}) == "alice"
    
    # Case-insensitive match key (Oracle UPPER vs target typical styles)
    assert col.map_row({"first_name": "BOB"}) == "bob"

    # Missing key error
    with pytest.raises(KeyError):
        col.map_row({"last_name": "Smith"})


def test_migration_template_parser():
    yaml_content = """
name: "test_migration"
source_db: "src"
target_db: "tgt"
streaming:
  chunk_size: 500
mapping:
  source_query: "SELECT * FROM src_table"
  target_table: "tgt_table"
  columns:
    - source: "ID"
      target: "id"
      type: "integer"
    - source: "NAME"
      target: "name"
      transform: "upper(value)"
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        temp_path = f.name

    try:
        template = MigrationTemplate(temp_path)
        assert template.name == "test_migration"
        assert template.source_db == "src"
        assert template.target_db == "tgt"
        assert template.chunk_size == 500
        assert template.source_query == "SELECT * FROM src_table"
        assert template.target_table == "tgt_table"
        assert len(template.columns) == 2
        
        assert template.columns[0].source == "ID"
        assert template.columns[0].target == "id"
        assert template.columns[0].type == "integer"
        
        assert template.columns[1].source == "NAME"
        assert template.columns[1].target == "name"
        assert template.columns[1].transformer is not None
    finally:
        os.remove(temp_path)
