"""Shared value types, conversions and shaping helpers for sync and async clients."""

import base64
import binascii
import json as _json
from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .errors import JdbcProxyError

_UNSET = object()  # sentinel: "not specified" (distinct from None)

# java.sql.Types constants that map to Python native values
_INT_TYPES = {-6, 5, 4, -5}            # TINYINT, SMALLINT, INTEGER, BIGINT
_DEC_TYPES = {2, 3}                    # NUMERIC, DECIMAL  (server sends exact strings)
_FLOAT_TYPES = {6, 7, 8}               # FLOAT, REAL, DOUBLE
_BOOL_TYPES = {16, -7}                 # BOOLEAN, BIT
_DATE_TYPE = 91                        # DATE
_TIME_TYPE = 92                        # TIME
_TIMESTAMP_TYPES = {93, 2014, -101, -102}   # TIMESTAMP, TIMESTAMP WITH TZ, ojdbc private codes
_BIN_TYPES = {-2, -3, -4, 2004}        # BINARY, VARBINARY, LONGVARBINARY, BLOB


@dataclass(frozen=True)
class Column:
    """Metadata for one result column, from JDBC ResultSetMetaData."""

    label: str
    name: str
    sql_type: int
    type_name: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: Optional[int] = None          # 0 = NO, 1 = YES, 2 = UNKNOWN
    auto_increment: bool = False
    display_size: Optional[int] = None
    signed: bool = False
    catalog: Optional[str] = None
    schema: Optional[str] = None
    table: Optional[str] = None

    @classmethod
    def from_json(cls, d: Dict[str, Any]) -> "Column":
        return cls(
            label=d.get("label") or "",
            name=d.get("name") or "",
            sql_type=d.get("sqlType", 0),
            type_name=d.get("typeName"),
            precision=d.get("precision"),
            scale=d.get("scale"),
            nullable=d.get("nullable"),
            auto_increment=bool(d.get("autoIncrement")),
            display_size=d.get("displaySize"),
            signed=bool(d.get("signed")),
            catalog=d.get("catalog"),
            schema=d.get("schema"),
            table=d.get("table"),
        )


class Typed:
    """Explicit parameter type hint for the server.

    Needed when a strict dialect (PostgreSQL) must be told the intent of a
    string parameter, e.g. ``Typed("json", '{"a": 1}')`` for a JSONB column.

    type: decimal | int | float | bool | date | time | timestamp | timestamptz |
          json | bytes | text
    """

    __slots__ = ("type", "value")

    def __init__(self, type: str, value: Any):
        self.type = type
        self.value = value

    def to_json(self) -> Dict[str, Any]:
        value = self.value
        if isinstance(value, (bytes, bytearray, memoryview)):
            value = base64.b64encode(bytes(value)).decode("ascii")
        elif isinstance(value, (dict, list)):
            value = _json.dumps(value, default=str)
        return {"type": self.type, "value": value}

    def __repr__(self) -> str:
        return f"Typed({self.type!r}, {self.value!r})"


def _convert(value: Any, col: Column) -> Any:
    """Convert one raw JSON value to a Python value based on the column type."""
    if value is None:
        return None
    t = col.sql_type
    try:
        if t in _INT_TYPES:
            return int(value)
        if t in _DEC_TYPES:
            d = Decimal(str(value))
            # integer-typed decimals (scale 0, or ojdbc's -127 for plain NUMBER)
            # come back as int; true decimals keep exact scale
            if col.scale is not None and col.scale <= 0:
                return int(d)
            return d
        if t in _FLOAT_TYPES:
            return float(value)
        if t in _BOOL_TYPES:
            # BIT(1)/BOOLEAN come as bool; BIT(n>1) comes as base64 bytes
            if isinstance(value, bool):
                return value
            if col.precision is not None and col.precision > 1:
                return base64.b64decode(value)
            return bool(value)
        if t == _DATE_TYPE:
            return date.fromisoformat(str(value)[:10])
        if t == _TIME_TYPE:
            return time.fromisoformat(str(value))
        if t in _TIMESTAMP_TYPES:
            return datetime.fromisoformat(str(value))
        if t in _BIN_TYPES:
            return base64.b64decode(value)
    except (ValueError, TypeError, binascii.Error):
        return value  # keep raw when the declared type doesn't match reality
    return value


def _param_to_json(v: Any) -> Any:
    """Map Python native types onto the wire protocol (plain value or typed hint)."""
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, Decimal):
        return {"type": "decimal", "value": str(v)}
    if isinstance(v, datetime):
        if v.tzinfo is not None:  # time-zone aware → keep the instant (PG timestamptz etc.)
            return {"type": "timestamptz", "value": v.isoformat()}
        return {"type": "timestamp", "value": v.isoformat(sep="T")}
    if isinstance(v, date):
        return {"type": "date", "value": v.isoformat()}
    if isinstance(v, time):
        return {"type": "time", "value": v.isoformat()}
    if isinstance(v, (bytes, bytearray, memoryview)):
        return {"type": "bytes", "value": base64.b64encode(bytes(v)).decode("ascii")}
    if isinstance(v, Typed):
        return v.to_json()
    if isinstance(v, dict) and set(v.keys()) == {"type", "value"} and isinstance(v["type"], str):
        return Typed(v["type"], v["value"]).to_json()
    if isinstance(v, (list, dict)):
        return {"type": "json", "value": _json.dumps(v, default=str)}
    return str(v)


def normalize_params(params: Optional[Sequence[Any]]) -> Optional[List[Any]]:
    if params is None:
        return None
    return [_param_to_json(v) for v in params]


def build_result(payload: dict, *, convert: bool = True) -> QueryResult:
    """Shape an /execute (or generatedKeys) payload into a QueryResult."""
    columns = [Column.from_json(c) for c in (payload.get("columns") or [])]
    keys = payload.get("generatedKeys")
    generated = None
    if keys:
        gen_cols = [Column.from_json(c) for c in (keys.get("columns") or [])]
        generated = QueryResult(gen_cols, keys.get("rows") or [],
                                "query", None, None, False, convert=convert)
    return QueryResult(
        columns=columns,
        raw_rows=payload.get("rows") or [],
        statement_type=payload.get("statementType", "update"),
        update_count=payload.get("updateCount"),
        generated_keys=generated,
        truncated=bool(payload.get("truncated")),
        convert=convert,
    )


def rows_to_dicts(resp: dict) -> List[Dict[str, Any]]:
    """Metadata responses: rows as dicts keyed by column label."""
    names = [c.get("label") for c in (resp.get("columns") or [])]
    return [dict(zip(names, row)) for row in (resp.get("rows") or [])]


class QueryResult:
    """Result of one statement execution.

    Attributes:
        columns:        list of Column metadata.
        rows:           list of tuples of (converted) values.
        statement_type: "query" | "update" | "batch".
        update_count:   affected row count for DML, None for queries.
        generated_keys: QueryResult with auto-generated keys, or None.
        truncated:      True when the server hit maxRows and more rows existed.
    """

    def __init__(self, columns: List[Column], raw_rows: List[List[Any]],
                 statement_type: str, update_count: Optional[int] = None,
                 generated_keys: Optional["QueryResult"] = None,
                 truncated: bool = False, convert: bool = True):
        self.columns = columns
        self.statement_type = statement_type
        self.update_count = update_count
        self.generated_keys = generated_keys
        self.truncated = truncated
        if convert:
            self.rows = [
                tuple(_convert(v, col) for v, col in zip(raw, columns))
                for raw in raw_rows
            ]
        else:
            self.rows = [tuple(raw) for raw in raw_rows]
        self._raw_rows = raw_rows

    @property
    def column_names(self) -> List[str]:
        return [c.label for c in self.columns]

    @property
    def raw_rows(self) -> List[Tuple[Any, ...]]:
        """Rows with raw JSON values (no client-side type conversion)."""
        return [tuple(r) for r in self._raw_rows]

    def dict_rows(self) -> List[Dict[str, Any]]:
        """Rows as dicts keyed by column label."""
        names = self.column_names
        return [dict(zip(names, row)) for row in self.rows]

    def raw_dict_rows(self) -> List[Dict[str, Any]]:
        """Rows as dicts keyed by column label, with raw values."""
        names = self.column_names
        return [dict(zip(names, row)) for row in self.raw_rows]

    def first_row(self) -> Optional[Tuple[Any, ...]]:
        return self.rows[0] if self.rows else None

    def scalar(self) -> Any:
        """First cell of the first row; raises if the result is empty."""
        if not self.rows:
            raise JdbcProxyError("expected a row but the result is empty")
        return self.rows[0][0]

    def __len__(self) -> int:
        return len(self.rows)

    def __repr__(self) -> str:
        kind = self.statement_type
        if kind == "update":
            return f"<QueryResult update update_count={self.update_count}>"
        return f"<QueryResult {kind} rows={len(self.rows)} cols={len(self.columns)}>"


@dataclass
class BatchResult:
    """Result of a batch execution."""

    update_counts: List[int] = field(default_factory=list)
    generated_keys: Optional[QueryResult] = None

    def __repr__(self) -> str:
        return f"<BatchResult update_counts={self.update_counts}>"


# ---------------------------------------------------------------- dialect helpers

def jdbc_db_name(url: str) -> Optional[str]:
    """Database name from jdbc:mysql://host[:port]/db?... (also oceanbase/mariadb)."""
    marker = "://"
    i = url.find(marker)
    if i < 0:
        return None
    rest = url[i + len(marker):]
    slash = rest.find("/")
    if slash < 0:
        return None
    db = rest[slash + 1:]
    for sep in ("?", "/"):
        j = db.find(sep)
        if j >= 0:
            db = db[:j]
    return db or None


def pg_schema_name(url: str) -> Optional[str]:
    """currentSchema=/schema= parameter from a PostgreSQL JDBC URL query string."""
    q = url.find("?")
    if q < 0:
        return None
    for pair in url[q + 1:].split("&"):
        k, _, v = pair.partition("=")
        if k in ("currentSchema", "schema") and v:
            return v
    return None


def _infer_family(product: Optional[str]) -> Optional[str]:
    """Best-effort family from the driver product name (legacy fallback)."""
    p = (product or "").lower()
    if "oracle" in p:
        return "oracle"
    if "opengauss" in p or "gauss" in p or "postgres" in p:
        return "postgresql"
    if "mysql" in p or "mariadb" in p:
        return "mysql"
    return None


def dialect_defaults(dialect: dict) -> Tuple[Optional[str], Optional[str]]:
    """(default catalog, default schema) for metadata helpers.

    Prefers the server-resolved family (``dialect`` field, which honours the
    client's explicit declaration — essential for OceanBase/OpenGauss compat
    modes); falls back to product-name inference for older servers.
    """
    family = dialect.get("dialect") or _infer_family(dialect.get("productName"))
    url = dialect.get("jdbcUrl") or ""
    if family == "mysql":
        return jdbc_db_name(url), None
    if family == "postgresql":
        return None, pg_schema_name(url) or "public"
    if family == "oracle":
        return None, (dialect.get("userName") or "").upper() or None
    return None, None


# ---------------------------------------------------------------- metadata shapers

def shape_tables(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{
        "catalog": r.get("TABLE_CAT"),
        "schema": r.get("TABLE_SCHEM"),
        "name": r.get("TABLE_NAME"),
        "type": r.get("TABLE_TYPE"),
        "remarks": r.get("REMARKS"),
    } for r in rows]


def _as_int(v: Any) -> Any:
    """Driver metadata columns (COLUMN_SIZE etc.) are strings on some drivers (ojdbc)."""
    if isinstance(v, str):
        try:
            return int(v)
        except ValueError:
            return v
    return v


def shape_columns(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{
        "name": r.get("COLUMN_NAME"),
        "type": r.get("TYPE_NAME"),
        "sql_type": _as_int(r.get("DATA_TYPE")),
        "position": _as_int(r.get("ORDINAL_POSITION")),
        "precision": _as_int(r.get("COLUMN_SIZE")),
        "scale": _as_int(r.get("DECIMAL_DIGITS")),
        "nullable": r.get("NULLABLE") == "YES",
        "default": r.get("COLUMN_DEF"),
        "auto_increment": r.get("IS_AUTOINCREMENT") == "YES",
        "remarks": r.get("REMARKS"),
    } for r in rows]


def shape_primary_keys(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = sorted(rows, key=lambda r: r.get("KEY_SEQ") or 0)
    return [{"column": r.get("COLUMN_NAME"), "seq": r.get("KEY_SEQ"),
             "pk_name": r.get("PK_NAME")} for r in rows]


def shape_foreign_keys(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{
        "fk_table": r.get("FKTABLE_NAME"), "fk_column": r.get("FKCOLUMN_NAME"),
        "pk_table": r.get("PKTABLE_NAME"), "pk_column": r.get("PKCOLUMN_NAME"),
        "fk_name": r.get("FK_NAME"), "pk_name": r.get("PK_NAME"),
        "update_rule": r.get("UPDATE_RULE"), "delete_rule": r.get("DELETE_RULE"),
    } for r in rows]


def shape_indexes(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{
        "index_name": r.get("INDEX_NAME"),
        "non_unique": r.get("NON_UNIQUE"),
        "column": r.get("COLUMN_NAME"),
        "ordinal": r.get("ORDINAL_POSITION"),
        "type": r.get("TYPE"),
        "asc_or_desc": r.get("ASC_OR_DESC"),
        "cardinality": r.get("CARDINALITY"),
    } for r in rows if r.get("INDEX_NAME") is not None]


def shape_schemas(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [{"schema": r.get("TABLE_SCHEM"), "catalog": r.get("TABLE_CATALOG")} for r in rows]
