import yaml
import logging
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger("pymigrate.templates")

class ValidationError(Exception):
    """Exception raised when a migration template is invalid."""
    pass


class FieldTransformer:
    """Precompiles and safely evaluates a python expression on a row field."""

    def __init__(self, expr: str, mapping_lookup: Dict[str, Any] = None):
        self.expr = expr
        self.lookup = mapping_lookup or {}
        try:
            self.compiled_code = compile(self.expr, "<transform>", "eval")
        except Exception as e:
            raise ValidationError(f"Syntax error compiling transformation expression '{self.expr}': {e}")

    def evaluate(self, value: Any, row: Dict[str, Any]) -> Any:
        # Safe execution environment
        context = {
            "value": value,
            "row": row,
            "mapping": self.lookup,
            "str": str,
            "int": int,
            "float": float,
            "len": len,
            "abs": abs,
            "round": round,
            "datetime": datetime,
            "lower": lambda x: str(x).lower() if x is not None else None,
            "upper": lambda x: str(x).upper() if x is not None else None,
            "strip": lambda x: str(x).strip() if x is not None else None,
        }
        try:
            return eval(self.compiled_code, {"__builtins__": {}}, context)
        except Exception as e:
            raise RuntimeError(f"Runtime error evaluating '{self.expr}' on value '{value}' in row {list(row.keys())}: {e}")


class ColumnMapping:
    """Represents the mapping of a single column."""

    def __init__(self, config: Dict[str, Any]):
        self.source = config.get("source")
        self.target = config.get("target")
        self.type = config.get("type")
        
        transform_expr = config.get("transform")
        mapping_lookup = config.get("mapping_lookup")
        
        if not self.source:
            raise ValidationError("Column mapping is missing required 'source' field.")
        if not self.target:
            raise ValidationError("Column mapping is missing required 'target' field.")

        self.transformer = None
        if transform_expr:
            self.transformer = FieldTransformer(transform_expr, mapping_lookup)
        elif mapping_lookup:
            # If mapping_lookup is specified without a transform, default to a lookup matching direct value
            self.transformer = FieldTransformer("mapping.get(value, value)", mapping_lookup)

    def convert_type(self, val: Any) -> Any:
        """Coerce values into desired types if specified."""
        if val is None:
            return None
        
        if not self.type:
            return val
            
        t = self.type.lower()
        try:
            if t == "integer" or t == "int":
                return int(val)
            elif t == "float" or t == "number":
                return float(val)
            elif t == "string" or t == "str":
                return str(val)
            elif t == "boolean" or t == "bool":
                if isinstance(val, str):
                    return val.strip().lower() in ("true", "1", "yes", "t", "y")
                return bool(val)
            elif t == "timestamp" or t == "date" or t == "datetime":
                if isinstance(val, datetime):
                    return val
                if isinstance(val, str):
                    # Attempt common ISO formatting parses
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f"):
                        try:
                            return datetime.strptime(val.strip(), fmt)
                        except ValueError:
                            continue
                raise ValueError(f"Unable to parse timestamp from value '{val}'")
            else:
                raise ValidationError(f"Unsupported type cast: {self.type}")
        except Exception as e:
            raise ValueError(f"Type conversion failure for column '{self.target}' to '{self.type}' with value '{val}': {e}")

    def map_row(self, row: Dict[str, Any]) -> Any:
        """Extract column value from source row, apply transform, and convert type."""
        # Source keys can sometimes have different casings, let's support case-insensitive key lookup
        # to make Oracle (typically UPPERCASE columns) and Postgres (lowercase columns) seamless.
        source_key = self.source
        if source_key not in row:
            # Try case-insensitive lookup
            found_key = None
            for k in row.keys():
                if k.upper() == source_key.upper():
                    found_key = k
                    break
            if found_key:
                source_key = found_key
            else:
                raise KeyError(f"Source column '{self.source}' not found in source row query results. Available: {list(row.keys())}")

        val = row[source_key]
        
        if self.transformer:
            val = self.transformer.evaluate(val, row)
            
        return self.convert_type(val)


class MigrationTemplate:
    """Parses, validates, and holds the migration template config."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.config = self._load_yaml(filepath)
        self.name = self.config.get("name")
        self.description = self.config.get("description", "")
        source_db_val = self.config.get("source_db")
        if isinstance(source_db_val, list):
            self.source_dbs = [str(db) for db in source_db_val]
        elif isinstance(source_db_val, str):
            self.source_dbs = [source_db_val]
        else:
            self.source_dbs = []
        self.source_db = self.source_dbs[0] if self.source_dbs else None

        self.target_db = self.config.get("target_db")
        
        streaming_config = self.config.get("streaming", {})
        self.chunk_size = streaming_config.get("chunk_size", 1000)
        self.itersize = streaming_config.get("itersize", 1000)
        
        self.pre_migration = self.config.get("pre_migration", [])
        self.post_migration = self.config.get("post_migration", [])
        
        self._validate_root_fields()
        self._parse_mappings()

    def _load_yaml(self, filepath: str) -> Dict[str, Any]:
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                if not isinstance(data, dict):
                    raise ValidationError("Template YAML top level must be a dictionary.")
                return data
        except yaml.YAMLError as e:
            raise ValidationError(f"Error parsing YAML file: {e}")
        except FileNotFoundError:
            raise ValidationError(f"Template file not found at: {filepath}")

    def _validate_root_fields(self) -> None:
        if not self.name:
            raise ValidationError("Template is missing required 'name' field.")
        if not self.source_dbs:
            raise ValidationError("Template is missing required 'source_db' reference.")
        if not self.target_db:
            raise ValidationError("Template is missing required 'target_db' reference.")
        if "mapping" not in self.config:
            raise ValidationError("Template is missing required 'mapping' section.")

    def _parse_mappings(self) -> None:
        mapping_block = self.config["mapping"]
        if not isinstance(mapping_block, dict):
            raise ValidationError("'mapping' section must be a dictionary.")

        self.source_query = mapping_block.get("source_query")
        self.target_table = mapping_block.get("target_table")
        self.rollback_strategy = mapping_block.get("rollback_strategy", "transaction")

        if not self.source_query:
            raise ValidationError("'mapping' section is missing required 'source_query' field.")
        if not self.target_table:
            raise ValidationError("'mapping' section is missing required 'target_table' field.")

        columns_list = mapping_block.get("columns")
        if not columns_list or not isinstance(columns_list, list):
            raise ValidationError("'mapping.columns' must be a non-empty list of column configurations.")

        self.columns: List[ColumnMapping] = []
        for i, col_config in enumerate(columns_list):
            if not isinstance(col_config, dict):
                raise ValidationError(f"Column configuration at index {i} must be a dictionary.")
            try:
                self.columns.append(ColumnMapping(col_config))
            except ValidationError as e:
                raise ValidationError(f"Validation error in column index {i}: {e}")
