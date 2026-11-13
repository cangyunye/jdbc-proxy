"""Pure unit tests for the jdbc_proxy client internals (no server / DB needed).

Covers: parameter normalization & type hints, value conversion, result shaping,
URL parsing helpers, dialect defaults, metadata shapers and error mapping.

Run from client/:
  python3 -m unittest tests.test_units -v
"""

import base64
import os
import sys
import unittest
from datetime import date, datetime, time, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jdbc_proxy import Column, JdbcProxyError, QueryResult, Typed
from jdbc_proxy.core import (_convert, _param_to_json, build_result,
                             dialect_defaults, jdbc_db_name, normalize_params,
                             pg_schema_name, shape_columns, shape_foreign_keys,
                             shape_indexes, shape_primary_keys, shape_schemas,
                             shape_tables)
from jdbc_proxy.errors import ConnectionClosedError


def _col(sql_type, label="c", precision=None, type_name=None):
    return Column(label=label, name=label, sql_type=sql_type, precision=precision, type_name=type_name)


class TestParamNormalization(unittest.TestCase):
    """JSON wire format of every supported Python parameter type."""

    def test_scalars_pass_through(self):
        self.assertEqual(_param_to_json(None), None)
        self.assertEqual(_param_to_json(True), True)
        self.assertEqual(_param_to_json(42), 42)
        self.assertEqual(_param_to_json(-7), -7)
        self.assertEqual(_param_to_json(1.25), 1.25)
        self.assertEqual(_param_to_json("hi"), "hi")

    def test_decimal_becomes_typed_hint(self):
        self.assertEqual(_param_to_json(Decimal("12.50")),
                         {"type": "decimal", "value": "12.50"})

    def test_date_time_datetime_become_typed_hints(self):
        self.assertEqual(_param_to_json(date(2026, 1, 15)),
                         {"type": "date", "value": "2026-01-15"})
        self.assertEqual(_param_to_json(time(13, 30, 5)),
                         {"type": "time", "value": "13:30:05"})
        self.assertEqual(_param_to_json(datetime(2026, 1, 15, 13, 30, 5, 500)),
                         {"type": "timestamp", "value": "2026-01-15T13:30:05.000500"})

    def test_tz_aware_datetime_uses_timestamptz(self):
        dt = datetime(2026, 1, 15, 12, 0, tzinfo=timezone.utc)
        self.assertEqual(_param_to_json(dt),
                         {"type": "timestamptz", "value": "2026-01-15T12:00:00+00:00"})

    def test_bytes_become_base64_hint(self):
        raw = b"\x00\x01\xff"
        expected = {"type": "bytes",
                    "value": base64.b64encode(raw).decode("ascii")}
        self.assertEqual(_param_to_json(raw), expected)
        self.assertEqual(_param_to_json(bytearray(raw)), expected)

    def test_plain_dict_becomes_json_hint(self):
        self.assertEqual(_param_to_json({"a": 1}),
                         {"type": "json", "value": '{"a": 1}'})

    def test_explicit_typed_hint_passthrough(self):
        t = Typed("json", {"color": "red"})
        self.assertEqual(_param_to_json(t), {"type": "json", "value": '{"color": "red"}'})

    def test_typed_dict_form_passthrough(self):
        self.assertEqual(_param_to_json({"type": "text", "value": "x"}),
                         {"type": "text", "value": "x"})

    def test_typed_with_bytes_value_encodes_base64(self):
        self.assertEqual(Typed("bytes", b"\x00\x01").to_json(),
                         {"type": "bytes", "value": "AAE="})

    def test_normalize_params_list(self):
        out = normalize_params([1, None, Decimal("2.5"), "s"])
        self.assertEqual(out, [1, None, {"type": "decimal", "value": "2.5"}, "s"])

    def test_normalize_params_none(self):
        self.assertIsNone(normalize_params(None))


class TestValueConversion(unittest.TestCase):
    """Raw JSON values -> Python natives, driven by column metadata."""

    def test_int_family(self):
        for t in (-6, 5, 4, -5):
            self.assertEqual(_convert("7", _col(t)), 7)
            self.assertIsNone(_convert(None, _col(t)))

    def test_decimal_exact(self):
        self.assertEqual(_convert("12.50", _col(3)), Decimal("12.50"))
        # precision beyond float64 must survive
        self.assertEqual(_convert("123456789012345678.9012", _col(2)),
                         Decimal("123456789012345678.9012"))

    def test_float_family(self):
        self.assertEqual(_convert("1.5", _col(8)), 1.5)
        self.assertEqual(_convert("2", _col(7)), 2.0)

    def test_bool_bit_precision_aware(self):
        self.assertIs(_convert(True, _col(-7, precision=1)), True)
        self.assertIs(_convert(False, _col(16)), False)
        # BIT(n>1) arrives as base64 -> bytes
        b64 = base64.b64encode(b"\x05").decode()
        self.assertEqual(_convert(b64, _col(-7, precision=8)), b"\x05")

    def test_temporal(self):
        self.assertEqual(_convert("2026-01-15", _col(91)), date(2026, 1, 15))
        self.assertEqual(_convert("13:30:05", _col(92)), time(13, 30, 5))
        self.assertEqual(_convert("2026-01-15T13:30:05.5", _col(93)),
                         datetime(2026, 1, 15, 13, 30, 5, 500000))
        # ISO with offset (timestamptz) keeps the instant
        v = _convert("2026-01-15T12:00:00+08:00", _col(93))
        self.assertEqual(v.utcoffset().total_seconds(), 8 * 3600)

    def test_binary_family(self):
        raw = b"\x00\xff"
        b64 = base64.b64encode(raw).decode()
        for t in (-2, -3, -4, 2004):
            self.assertEqual(_convert(b64, _col(t)), raw)

    def test_mismatched_value_falls_back_raw(self):
        # declared int but garbage value: keep raw rather than crash
        self.assertEqual(_convert("not-a-number", _col(4)), "not-a-number")
        self.assertEqual(_convert("not-a-date", _col(91)), "not-a-date")


class TestBuildResult(unittest.TestCase):

    def _payload(self, **over):
        p = {
            "statementType": "query",
            "columns": [{"label": "ID", "name": "ID", "sqlType": 4},
                        {"label": "SAL", "name": "SAL", "sqlType": 3}],
            "rows": [[1, "99.50"], [2, None]],
            "rowCount": 2,
            "truncated": False,
            "updateCount": None,
            "generatedKeys": None,
        }
        p.update(over)
        return p

    def test_query_shape(self):
        r = build_result(self._payload())
        self.assertEqual(r.statement_type, "query")
        self.assertEqual(len(r), 2)
        self.assertEqual(r.rows[0], (1, Decimal("99.50")))
        self.assertIsNone(r.rows[1][1])
        self.assertEqual(r.column_names, ["ID", "SAL"])
        self.assertEqual(r.dict_rows()[0], {"ID": 1, "SAL": Decimal("99.50")})
        self.assertFalse(r.truncated)
        self.assertIsNone(r.update_count)

    def test_update_shape_with_generated_keys(self):
        payload = self._payload(
            statementType="update", rows=[], updateCount=1,
            generatedKeys={"columns": [{"label": "ID", "name": "ID", "sqlType": -5}],
                           "rows": [[7]], "rowCount": 1})
        r = build_result(payload)
        self.assertEqual(r.update_count, 1)
        self.assertEqual(r.generated_keys.scalar(), 7)
        self.assertEqual(r.statement_type, "update")

    def test_truncated_flag(self):
        r = build_result(self._payload(truncated=True, rows=[[1, None]], rowCount=1))
        self.assertTrue(r.truncated)

    def test_scalar_raises_on_empty(self):
        r = build_result(self._payload(rows=[], rowCount=0))
        with self.assertRaises(JdbcProxyError):
            r.scalar()

    def test_raw_rows_skip_conversion(self):
        r = build_result(self._payload(), convert=False)
        self.assertEqual(r.rows[0], (1, "99.50"))

    def test_repr(self):
        self.assertIn("rows=2", repr(build_result(self._payload())))


class TestDialectHelpers(unittest.TestCase):

    def test_jdbc_db_name(self):
        self.assertEqual(jdbc_db_name("jdbc:mysql://h:3306/default_db?x=1"), "default_db")
        self.assertEqual(jdbc_db_name("jdbc:mysql://h:3306/default_db"), "default_db")
        self.assertIsNone(jdbc_db_name("jdbc:mysql://h:3306"))
        self.assertIsNone(jdbc_db_name("jdbc:oracle:thin:@h:1521/x"))

    def test_pg_schema_name(self):
        self.assertEqual(pg_schema_name("jdbc:postgresql://h/db?currentSchema=custom"), "custom")
        self.assertEqual(pg_schema_name("jdbc:postgresql://h/db?schema=s2"), "s2")
        self.assertIsNone(pg_schema_name("jdbc:postgresql://h/db"))

    def test_dialect_defaults_mysql_family(self):
        d = {"productName": "MySQL", "jdbcUrl": "jdbc:mysql://h/db1"}
        self.assertEqual(dialect_defaults(d), ("db1", None))

    def test_dialect_defaults_pg(self):
        d = {"productName": "PostgreSQL", "jdbcUrl": "jdbc:postgresql://h/db"}
        self.assertEqual(dialect_defaults(d), (None, "public"))

    def test_dialect_defaults_oracle(self):
        d = {"productName": "Oracle", "userName": "scott"}
        self.assertEqual(dialect_defaults(d), (None, "SCOTT"))

    def test_dialect_defaults_unknown(self):
        self.assertEqual(dialect_defaults({"productName": "WeirdDB", "jdbcUrl": "jdbc:x:..."}), (None, None))


class TestMetadataShapers(unittest.TestCase):

    def test_shape_tables(self):
        rows = [{"TABLE_CAT": "c", "TABLE_SCHEM": "s", "TABLE_NAME": "T",
                 "TABLE_TYPE": "TABLE", "REMARKS": "r"}]
        self.assertEqual(shape_tables(rows),
                         [{"catalog": "c", "schema": "s", "name": "T",
                           "type": "TABLE", "remarks": "r"}])

    def test_shape_columns(self):
        rows = [{"COLUMN_NAME": "id", "TYPE_NAME": "INT", "DATA_TYPE": 4,
                 "ORDINAL_POSITION": 1, "COLUMN_SIZE": 10, "DECIMAL_DIGITS": 0,
                 "NULLABLE": "NO", "COLUMN_DEF": None, "IS_AUTOINCREMENT": "YES",
                 "REMARKS": ""}]
        c = shape_columns(rows)[0]
        self.assertTrue(c["auto_increment"])
        self.assertFalse(c["nullable"])
        self.assertEqual(c["sql_type"], 4)

    def test_shape_primary_keys_sorted(self):
        rows = [{"COLUMN_NAME": "b", "KEY_SEQ": 2, "PK_NAME": "P"},
                {"COLUMN_NAME": "a", "KEY_SEQ": 1, "PK_NAME": "P"}]
        self.assertEqual([p["column"] for p in shape_primary_keys(rows)], ["a", "b"])

    def test_shape_foreign_keys(self):
        rows = [{"FKTABLE_NAME": "c", "FKCOLUMN_NAME": "pid", "PKTABLE_NAME": "p",
                 "PKCOLUMN_NAME": "id", "FK_NAME": "fk1", "PK_NAME": "pk1",
                 "UPDATE_RULE": 3, "DELETE_RULE": 2}]
        fk = shape_foreign_keys(rows)[0]
        self.assertEqual((fk["fk_table"], fk["pk_table"]), ("c", "p"))

    def test_shape_indexes_skips_unnamed(self):
        rows = [{"INDEX_NAME": None}, {"INDEX_NAME": "i1", "NON_UNIQUE": 1,
                                       "COLUMN_NAME": "x", "ORDINAL_POSITION": 1}]
        self.assertEqual(len(shape_indexes(rows)), 1)

    def test_shape_schemas(self):
        self.assertEqual(shape_schemas([{"TABLE_SCHEM": "public", "TABLE_CATALOG": "db"}]),
                         [{"schema": "public", "catalog": "db"}])


class TestErrors(unittest.TestCase):

    def test_from_response_full_payload(self):
        e = JdbcProxyError.from_response(400, {"error": {
            "type": "sql_error", "message": "boom", "sqlState": "42S02",
            "errorCode": 1146, "status": 400, "causedBy": "inner"}})
        self.assertEqual(e.status, 400)
        self.assertEqual(e.sql_state, "42S02")
        self.assertEqual(e.error_code, 1146)
        self.assertEqual(e.error_type, "sql_error")
        self.assertIn("42S02", str(e))
        self.assertIn("inner", str(e))

    def test_from_response_empty_payload(self):
        e = JdbcProxyError.from_response(404, {})
        self.assertEqual(e.status, 404)
        self.assertIn("404", str(e))

    def test_connection_closed_error(self):
        e = ConnectionClosedError("closed")
        self.assertIsInstance(e, JdbcProxyError)


class TestColumnFromJson(unittest.TestCase):

    def test_full_mapping(self):
        c = Column.from_json({"label": "SAL", "name": "SAL", "sqlType": 3,
                              "typeName": "DECIMAL", "precision": 7, "scale": 2,
                              "nullable": 0, "autoIncrement": False, "signed": True,
                              "displaySize": 9, "catalog": "db", "schema": "",
                              "table": "EMP"})
        self.assertEqual(c.sql_type, 3)
        self.assertEqual(c.precision, 7)
        self.assertEqual(c.scale, 2)
        self.assertTrue(c.signed)
        self.assertFalse(c.auto_increment)
        self.assertEqual(c.table, "EMP")

    def test_missing_fields_default(self):
        c = Column.from_json({})
        self.assertEqual(c.label, "")
        self.assertEqual(c.sql_type, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
