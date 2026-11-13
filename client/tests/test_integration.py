"""Integration tests for the jdbc-proxy Python client.

Requirements:
  * jdbc-proxy server running (default http://127.0.0.1:8089)
  * MySQL on 127.0.0.1:3306 (root/root123456, database default_db)
  * PostgreSQL on 127.0.0.1:5432 (postgres/postgres123, database postgres_db)

Overrides via environment:
  JDBC_PROXY_URL, MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD,
  PG_URL, PG_USER, PG_PASSWORD

Run from the client/ directory:
  python3 -m unittest tests.test_integration -v
"""

import os
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jdbc_proxy import (JdbcProxyClient, JdbcConnection, JdbcProxyError,
                        QueryResult, Typed)

PROXY_URL = os.environ.get("JDBC_PROXY_URL", "http://127.0.0.1:8089")

MYSQL = {
    "url": os.environ.get("MYSQL_URL", "jdbc:mysql://127.0.0.1:3306/default_db"),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "root123456"),
}

PG = {
    "url": os.environ.get("PG_URL", "jdbc:postgresql://127.0.0.1:5432/postgres_db"),
    "user": os.environ.get("PG_USER", "postgres"),
    "password": os.environ.get("PG_PASSWORD", "postgres123"),
}


def _proxy_or_skip(cls):
    try:
        JdbcProxyClient(PROXY_URL, timeout=10).health()
    except Exception as e:
        raise unittest.SkipTest(f"jdbc-proxy not reachable at {PROXY_URL}: {e}")
    return cls


@_proxy_or_skip
class TestServerBasics(unittest.TestCase):
    def test_health(self):
        h = JdbcProxyClient(PROXY_URL).health()
        self.assertEqual(h["status"], "ok")
        self.assertIn("maxRows", h)

    def test_drivers_listed(self):
        drivers = JdbcProxyClient(PROXY_URL).drivers()
        classes = {d["className"] for d in drivers}
        self.assertIn("com.mysql.cj.jdbc.Driver", classes)
        self.assertIn("org.postgresql.Driver", classes)

    def test_connect_bad_url_rejected(self):
        client = JdbcProxyClient(PROXY_URL)
        with self.assertRaises(JdbcProxyError):
            client.connect("not-a-jdbc-url")
        with self.assertRaises(JdbcProxyError):
            client.connect("jdbc:mysql://127.0.0.1:1/none", user="x", password="x")

    def test_unknown_connection_id_404(self):
        client = JdbcProxyClient(PROXY_URL)
        with self.assertRaises(JdbcProxyError) as ctx:
            client._t.request("GET", "/api/connections/deadbeef00000000")
        self.assertEqual(ctx.exception.status, 404)


class _DatabaseCase(unittest.TestCase):
    """Shared fixtures: one fresh connection per test + a scratch table."""

    db = None  # dict with url/user/password

    @classmethod
    def _proxy(cls):
        return JdbcProxyClient(PROXY_URL)

    def setUp(self):
        self.client = self._proxy()
        self.conn = self.client.connect(**self.db)
        # scratch table unique to this test
        self.table = f"jp_proxy_test_{id(self) & 0xffffffff:x}"
        self.conn.execute(self._create_table_sql())
        self.addCleanup(self._teardown)

    def _teardown(self):
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {self.table}")
        except JdbcProxyError:
            pass
        self.conn.close()
        self.client.close()

    def _create_table_sql(self):
        raise NotImplementedError


class TestMysql(_DatabaseCase):
    db = MYSQL

    def _create_table_sql(self):
        return (f"CREATE TABLE {self.table} ("
                "id INT AUTO_INCREMENT PRIMARY KEY, "
                "name VARCHAR(50) NOT NULL, "
                "price DECIMAL(10,2), "
                "qty INT, "
                "created DATE, "
                "payload VARCHAR(100))")

    def test_query_with_conversion(self):
        self.conn.execute(
            f"INSERT INTO {self.table} (name, price, qty, created) VALUES (?, ?, ?, ?)",
            ["alpha", Decimal("12.50"), 3, date(2026, 1, 15)])
        r = self.conn.query(f"SELECT id, name, price, qty, created FROM {self.table} WHERE name = ?", ["alpha"])
        self.assertEqual(r.statement_type, "query")
        self.assertEqual(len(r), 1)
        row = r.first_row()
        self.assertEqual(row[1], "alpha")
        self.assertEqual(row[2], Decimal("12.50"))
        self.assertEqual(row[4], date(2026, 1, 15))
        d = r.dict_rows()[0]
        self.assertEqual(d["price"], Decimal("12.50"))

    def test_null_and_raw_rows(self):
        self.conn.execute(f"INSERT INTO {self.table} (name) VALUES (?)", ["nullish"])
        r = self.conn.query(f"SELECT price, qty FROM {self.table} WHERE name = ?", ["nullish"])
        self.assertEqual(r.first_row(), (None, None))
        raw = self.conn.query(f"SELECT price FROM {self.table} WHERE name = ?",
                              ["nullish"], convert=False)
        self.assertEqual(raw.first_row(), (None,))

    def test_execute_generated_keys(self):
        r = self.conn.execute(
            f"INSERT INTO {self.table} (name) VALUES (?)", ["genkey"])
        self.assertEqual(r.statement_type, "update")
        self.assertEqual(r.update_count, 1)
        self.assertIsNotNone(r.generated_keys)
        gen_id = r.generated_keys.first_row()[0]
        r2 = self.conn.query(f"SELECT id FROM {self.table} WHERE name = ?", ["genkey"])
        self.assertEqual(r2.scalar(), gen_id)

    def test_query_rejects_dml(self):
        with self.assertRaises(JdbcProxyError):
            self.conn.query(f"INSERT INTO {self.table} (name) VALUES (?)", ["nope"])

    def test_batch_execute_many(self):
        rows = [["b%d" % i, Decimal("1.10"), i] for i in range(1, 6)]
        b = self.conn.execute_many(
            f"INSERT INTO {self.table} (name, price, qty) VALUES (?, ?, ?)", rows)
        self.assertEqual(b.update_counts, [1] * 5)
        count = self.conn.query(f"SELECT COUNT(*) FROM {self.table}").scalar()
        self.assertEqual(count, 5)
        if b.generated_keys:
            self.assertEqual(len(b.generated_keys.rows), 5)

    def test_batch_reports_sql_error(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn.execute_many(
                f"INSERT INTO {self.table} (name) VALUES (?)",
                [["ok"], [None]])  # name NOT NULL -> second item fails
        self.assertEqual(ctx.exception.sql_state, "23000")  # MySQL integrity violation

    def test_transaction_commit_and_rollback(self):
        with self.conn.transaction():
            self.conn.execute(f"INSERT INTO {self.table} (name) VALUES (?)", ["t1"])
        self.assertEqual(
            self.conn.query(f"SELECT COUNT(*) FROM {self.table} WHERE name = ?", ["t1"]).scalar(), 1)

        with self.assertRaises(ValueError):
            with self.conn.transaction():
                self.conn.execute(f"INSERT INTO {self.table} (name) VALUES (?)", ["t2"])
                raise ValueError("boom")
        self.assertEqual(
            self.conn.query(f"SELECT COUNT(*) FROM {self.table} WHERE name = ?", ["t2"]).scalar(), 0)

    def test_max_rows_and_truncated(self):
        for i in range(10):
            self.conn.execute(f"INSERT INTO {self.table} (name) VALUES (?)", [f"m{i}"])
        r = self.conn.query(f"SELECT name FROM {self.table} ORDER BY name", max_rows=3)
        self.assertEqual(len(r), 3)
        self.assertTrue(r.truncated)
        r_all = self.conn.query(f"SELECT name FROM {self.table} ORDER BY name")
        self.assertEqual(len(r_all), 10)
        self.assertFalse(r_all.truncated)

    def test_sql_error_payload(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn.execute("SELECT * FROM definitely_missing_table_xyz")
        self.assertEqual(ctx.exception.sql_state, "42S02")
        self.assertIn("definitely_missing_table_xyz", str(ctx.exception))

    def test_dialect_metadata(self):
        d = self.conn.dialect()
        self.assertEqual(d["productName"], "MySQL")
        self.assertEqual(d["identifierQuoteString"], "`")
        self.assertTrue(d["supportsGetGeneratedKeys"])
        d2 = self.conn.dialect(refresh=True)
        self.assertEqual(d2["productName"], "MySQL")

    def test_list_tables_defaults_to_url_database(self):
        tables = self.conn.list_tables()
        names = [t["name"] for t in tables]
        self.assertIn(self.table, names)
        self.assertNotIn("innodb_index_stats", names)  # no system tables

    def test_describe_table(self):
        cols = self.conn.describe_table(self.table)
        names = [c["name"] for c in cols]
        self.assertEqual(names, ["id", "name", "price", "qty", "created", "payload"])
        id_col = cols[0]
        self.assertTrue(id_col["auto_increment"])
        self.assertTrue(id_col["nullable"] is False)
        price = cols[2]
        self.assertEqual(price["type"], "DECIMAL")
        self.assertEqual(price["precision"], 10)
        self.assertEqual(price["scale"], 2)

    def test_primary_keys_and_indexes(self):
        pks = self.conn.get_primary_keys(self.table)
        self.assertEqual([p["column"] for p in pks], ["id"])
        idx = self.conn.get_indexes(self.table)
        self.assertTrue(any(i["index_name"] == "PRIMARY" for i in idx))

    def test_type_info(self):
        info = self.conn.get_type_info()
        self.assertTrue(any(t["TYPE_NAME"] == "VARCHAR" for t in info))

    def test_ping_and_close(self):
        self.assertTrue(self.conn.ping())
        self.conn.close()
        self.assertTrue(self.conn.is_closed())
        with self.assertRaises(JdbcProxyError):
            self.conn.ping()


class TestPostgres(_DatabaseCase):
    db = PG

    def _create_table_sql(self):
        return (f'CREATE TABLE "{self.table}" ('
                'id SERIAL PRIMARY KEY, '
                '"name" VARCHAR(50) NOT NULL, '
                '"price" NUMERIC(10,2), '
                '"qty" INT, '
                '"created" DATE, '
                '"payload" VARCHAR(100), '
                '"meta" JSONB)')

    def test_query_with_conversion(self):
        self.conn.execute(
            f'INSERT INTO "{self.table}" ("name", "price", "qty", "created") VALUES (?, ?, ?, ?)',
            ["alpha", Decimal("12.50"), 3, date(2026, 1, 15)])
        r = self.conn.query(
            f'SELECT id, "name", "price", "qty", "created" FROM "{self.table}" WHERE "name" = ?',
            ["alpha"])
        row = r.first_row()
        self.assertEqual(row[1], "alpha")
        self.assertEqual(row[2], Decimal("12.50"))
        self.assertEqual(row[4], date(2026, 1, 15))

    def test_jsonb_param_via_typed_and_dict(self):
        self.conn.execute(
            f'INSERT INTO "{self.table}" ("name", "meta") VALUES (?, ?)',
            ["withjson", Typed("json", '{"a": 1, "b": [2, 3]}')])
        self.conn.execute(
            f'INSERT INTO "{self.table}" ("name", "meta") VALUES (?, ?)',
            ["withdict", {"a": 9, "b": ["x"]}])
        r1 = self.conn.query(f'SELECT "meta" FROM "{self.table}" WHERE "name" = ?', ["withjson"])
        self.assertEqual(r1.first_row()[0], '{"a": 1, "b": [2, 3]}')
        r2 = self.conn.query(f'SELECT "meta" FROM "{self.table}" WHERE "name" = ?', ["withdict"])
        self.assertIn('"a": 9', r2.first_row()[0])

    def test_returning_statement(self):
        r = self.conn.execute(
            f'INSERT INTO "{self.table}" ("name") VALUES (?) RETURNING id, "name"',
            ["retkey"])
        self.assertEqual(r.update_count, 1)
        keys = r.generated_keys
        self.assertIsNotNone(keys)
        gen_id = keys.first_row()[0]
        r2 = self.conn.query(f'SELECT id FROM "{self.table}" WHERE "name" = ?', ["retkey"])
        self.assertEqual(r2.scalar(), gen_id)

    def test_batch_execute_many(self):
        rows = [["pg%d" % i, Decimal("2.20"), i] for i in range(1, 4)]
        b = self.conn.execute_many(
            f'INSERT INTO "{self.table}" ("name", "price", "qty") VALUES (?, ?, ?)', rows)
        self.assertEqual(b.update_counts, [1] * 3)
        self.assertEqual(self.conn.query(f'SELECT COUNT(*) FROM "{self.table}"').scalar(), 3)
        if b.generated_keys:
            self.assertEqual(len(b.generated_keys.rows), 3)

    def test_transaction_commit_and_rollback(self):
        with self.conn.transaction(isolation="READ_COMMITTED"):
            self.conn.execute(f'INSERT INTO "{self.table}" ("name") VALUES (?)', ["t1"])
        self.assertEqual(
            self.conn.query(f'SELECT COUNT(*) FROM "{self.table}" WHERE "name" = ?', ["t1"]).scalar(), 1)

        with self.assertRaises(RuntimeError):
            with self.conn.transaction():
                self.conn.execute(f'INSERT INTO "{self.table}" ("name") VALUES (?)', ["t2"])
                raise RuntimeError("boom")
        self.assertEqual(
            self.conn.query(f'SELECT COUNT(*) FROM "{self.table}" WHERE "name" = ?', ["t2"]).scalar(), 0)

    def test_max_rows_and_truncated(self):
        for i in range(10):
            self.conn.execute(f'INSERT INTO "{self.table}" ("name") VALUES (?)', [f"m{i}"])
        r = self.conn.query(f'SELECT "name" FROM "{self.table}" ORDER BY "name"', max_rows=3)
        self.assertEqual(len(r), 3)
        self.assertTrue(r.truncated)

    def test_sql_error_payload(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn.execute("SELECT * FROM definitely_missing_table_xyz")
        self.assertEqual(ctx.exception.sql_state, "42P01")
        self.assertIn("definitely_missing_table_xyz", str(ctx.exception))

    def test_dialect_metadata(self):
        d = self.conn.dialect()
        self.assertEqual(d["productName"], "PostgreSQL")
        self.assertEqual(d["identifierQuoteString"], '"')

    def test_describe_table(self):
        cols = self.conn.describe_table(self.table)
        names = [c["name"] for c in cols]
        self.assertEqual(names, ["id", "name", "price", "qty", "created", "payload", "meta"])
        self.assertEqual(cols[0]["type"], "serial")

    def test_primary_keys(self):
        pks = self.conn.get_primary_keys(self.table)
        self.assertEqual([p["column"] for p in pks], ["id"])

    def test_schemas_and_list_tables(self):
        schemas = [s["schema"] for s in self.conn.get_schemas()]
        self.assertIn("public", schemas)
        tables = self.conn.list_tables(schema="public")
        self.assertIn(self.table, [t["name"] for t in tables])


class TestTimestamps(unittest.TestCase):
    """TIMESTAMP handling on MySQL (server formats ISO without offset)."""

    @classmethod
    def setUpClass(cls):
        try:
            JdbcProxyClient(PROXY_URL, timeout=10).health()
        except Exception as e:
            raise unittest.SkipTest(f"jdbc-proxy not reachable: {e}")
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.conn = cls.client.connect(**MYSQL)
        cls.table = "jp_proxy_ts_test"
        cls.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {cls.table} ("
            "id INT AUTO_INCREMENT PRIMARY KEY, ts TIMESTAMP(6))")
        cls.addClassCleanup(cls._teardown)

    @classmethod
    def _teardown(cls):
        try:
            cls.conn.execute(f"DROP TABLE IF EXISTS {cls.table}")
        except JdbcProxyError:
            pass
        cls.conn.close()
        cls.client.close()

    def test_timestamp_roundtrip(self):
        stamp = datetime(2026, 3, 14, 15, 9, 26, 535897)
        self.conn.execute(f"DELETE FROM {self.table}")
        self.conn.execute(f"INSERT INTO {self.table} (ts) VALUES (?)", [stamp])
        r = self.conn.query(f"SELECT ts FROM {self.table} WHERE id = (SELECT MAX(id) FROM {self.table})")
        self.assertEqual(r.first_row()[0], stamp)


if __name__ == "__main__":
    unittest.main(verbosity=2)
