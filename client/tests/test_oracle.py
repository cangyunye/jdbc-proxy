"""Oracle (23ai Free) functional tests — the first non-MySQL/PG dialect.

Covers: dialect auto-detection + explicit declaration, ping (FROM DUAL),
CRUD with IDENTITY generated keys, Oracle type mapping (NUMBER/DATE/TIMESTAMP/
TIMESTAMP WITH TIME ZONE/CLOB/BLOB), transactions, and metadata with Oracle's
schema-as-namespace model (catalog unused, schema defaults to the user).

Prerequisites:
  docker compose up -d oracle      # first start takes several minutes
  SCOTT/TIGER seeded by oracle/init-scott.sh (see oracle/scott_seed.sql)

Run from client/:
  python3 -m unittest tests.test_oracle -v
"""

import os
import sys
import time
import unittest
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_integration import PROXY_URL

from jdbc_proxy import JdbcProxyClient, JdbcProxyError, Typed

ORACLE = {
    "url": os.environ.get("ORACLE_URL", "jdbc:oracle:thin:@//127.0.0.1:1521/XEPDB1"),
    "user": os.environ.get("ORACLE_USER", "scott"),
    "password": os.environ.get("ORACLE_PASSWORD", "tiger"),
}

_oracle_state = None  # cached probe result: True | False | "pending"


def _oracle_reachable(client) -> bool:
    """Probe once per test run; wait up to ~4 min for the container to finish
    its first-time initialization, then cache the verdict."""
    global _oracle_state
    if _oracle_state is None:
        _oracle_state = "pending"
        deadline = time.time() + 240
        while time.time() < deadline:
            try:
                c = client.connect(**ORACLE)
                c.ping()
                c.close()
                _oracle_state = True
                return True
            except JdbcProxyError:
                time.sleep(5)
        _oracle_state = False
    return _oracle_state is True


def _oracle_case(cls):
    def wrapper():
        client = JdbcProxyClient(PROXY_URL, timeout=10)
        try:
            client.health()
        except Exception as e:
            raise unittest.SkipTest(f"jdbc-proxy not reachable: {e}")
        if not _oracle_reachable(client):
            raise unittest.SkipTest(
                "Oracle not reachable at XEPDB1 — start it with: docker compose up -d oracle "
                "(first initialization takes a few minutes)")
        return cls
    return wrapper()


@_oracle_case
class TestOracleBasics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.conn = cls.client.connect(**ORACLE)
        cls.addClassCleanup(cls._teardown)

    @classmethod
    def _teardown(cls):
        cls.conn.close()
        cls.client.close()

    def test_dialect_auto_detected_as_oracle(self):
        d = self.conn.dialect()
        self.assertEqual(d["productName"], "Oracle")
        self.assertEqual(d["dialect"], "oracle")
        self.assertEqual(d["dialectHint"], "auto")
        self.assertEqual(d["catalogTerm"], "")  # Oracle has no catalogs

    def test_ping_uses_from_dual(self):
        self.assertTrue(self.conn.ping())

    def test_select_from_dual(self):
        r = self.conn.query("SELECT 1 + 1 AS two FROM DUAL")
        self.assertEqual(r.scalar(), 2)

    def test_explicit_dialect_hint_echoed(self):
        resp = self.client._t.request("POST", "/api/connections", {**ORACLE, "dialect": "oracle"})
        self.assertEqual(resp["dialect"], "oracle")
        c = self.client._t.request("GET", f"/api/connections/{resp['connectionId']}")
        self.assertEqual(c["dialect"], "oracle")
        self.client._t.request("DELETE", f"/api/connections/{resp['connectionId']}")

    def test_unknown_dialect_hint_rejected(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.client._t.request("POST", "/api/connections", {**ORACLE, "dialect": "sqlite"})
        self.assertEqual(ctx.exception.status, 400)
        self.assertIn("unknown dialect", str(ctx.exception))

    def test_oracle_family_is_respected_even_when_declared_for_other_db(self):
        # declaring "postgresql" for an Oracle URL is trusted (client's call):
        # family resolves to postgresql — proves the hint overrides inference
        resp = self.client._t.request("POST", "/api/connections", {**ORACLE, "dialect": "pg"})
        c = self.client._t.request("GET", f"/api/connections/{resp['connectionId']}")
        self.assertEqual(c["dialect"], "pg")
        d = self.client._t.request("GET", f"/api/connections/{resp['connectionId']}/metadata/dialect")
        self.assertEqual(d["dialect"], "postgresql")
        self.assertEqual(d["dialectHint"], "pg")
        self.client._t.request("DELETE", f"/api/connections/{resp['connectionId']}")


@_oracle_case
class TestOracleCrud(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.conn = cls.client.connect(**ORACLE)
        cls.addClassCleanup(cls._teardown)

    @classmethod
    def _teardown(cls):
        try:
            cls.conn.execute("DROP TABLE jp_o_demo PURGE")
        except JdbcProxyError:
            pass
        cls.conn.close()
        cls.client.close()

    def setUp(self):
        try:
            self.conn.execute("DROP TABLE jp_o_demo PURGE")
        except JdbcProxyError:
            pass  # first run of this process
        self.conn.execute(
            "CREATE TABLE jp_o_demo ("
            "id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            "name VARCHAR2(50) NOT NULL, "
            "salary NUMBER(7,2), "
            "hired DATE, "
            "ts TIMESTAMP(6), "
            "tswtz TIMESTAMP(3) WITH TIME ZONE, "
            "bio CLOB, "
            "avatar BLOB)")

    def test_insert_with_identity_generated_keys(self):
        r = self.conn.execute(
            "INSERT INTO jp_o_demo (name, salary) VALUES (?, ?)", ["scott", Decimal("3000.00")])
        self.assertEqual(r.update_count, 1)
        self.assertIsNotNone(r.generated_keys, "ojdbc must return keys for the insert")
        col = r.generated_keys.columns[0]
        first = r.generated_keys.first_row()[0]
        if col.label.upper() == "ROWID":
            # Oracle JDBC returns the ROWID pseudo-column (not the identity value):
            # use it to fetch the real generated id — the documented two-step pattern
            stored = self.conn.query(
                "SELECT id, name FROM jp_o_demo WHERE rowid = ?", [first]).dict_rows()[0]
            self.assertEqual(stored["NAME"], "scott")  # Oracle identifiers come back upper-case
            self.assertIsInstance(stored["ID"], int)
        else:
            stored = self.conn.query("SELECT id FROM jp_o_demo WHERE name = ?", ["scott"]).scalar()
            self.assertEqual(stored, first)

    def test_number_mapping_exact_decimal(self):
        # NUMBER(7,2): 5 integer digits + 2 decimals
        exact = Decimal("99999.99")
        self.conn.execute("INSERT INTO jp_o_demo (name, salary) VALUES (?, ?)", ["pay", exact])
        self.assertEqual(self.conn.query("SELECT salary FROM jp_o_demo WHERE name = ?", ["pay"]).scalar(), exact)

    def test_integer_number_becomes_int(self):
        self.conn.execute("INSERT INTO jp_o_demo (name, salary) VALUES (?, ?)", ["n", Decimal("42")])
        self.assertEqual(self.conn.query("SELECT salary FROM jp_o_demo WHERE name = ?", ["n"]).scalar(), 42)

    def test_oracle_date_keeps_time_component(self):
        # Oracle DATE has a time part; ojdbc maps it to Types.TIMESTAMP
        dt = datetime(2026, 3, 14, 15, 9, 26)
        self.conn.execute("INSERT INTO jp_o_demo (name, hired) VALUES (?, ?)", ["d", dt])
        got = self.conn.query("SELECT hired FROM jp_o_demo WHERE name = ?", ["d"]).scalar()
        self.assertEqual(got, dt)

    def test_timestamp_and_timestamptz(self):
        naive = datetime(2026, 1, 2, 3, 4, 5, 123456)
        aware = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=8)))
        self.conn.execute("INSERT INTO jp_o_demo (name, ts, tswtz) VALUES (?, ?, ?)",
                          ["t", naive, aware])
        row = self.conn.query("SELECT ts, tswtz FROM jp_o_demo WHERE name = ?", ["t"]).first_row()
        self.assertEqual(row[0], naive)
        self.assertEqual(row[1], aware)  # same instant, offset preserved via ISO string

    def test_clob_and_blob_roundtrip(self):
        text = "Oracle CLOB 中文 " * 100
        raw = bytes(range(256))
        self.conn.execute("INSERT INTO jp_o_demo (name, bio, avatar) VALUES (?, ?, ?)",
                          ["lob", text, Typed("bytes", raw)])
        row = self.conn.query("SELECT bio, avatar FROM jp_o_demo WHERE name = ?", ["lob"]).first_row()
        self.assertEqual(row[0], text)
        self.assertEqual(row[1], raw)

    def test_date_typed_param(self):
        self.conn.execute("INSERT INTO jp_o_demo (name, hired) VALUES (?, ?)",
                          ["dt", date(2020, 2, 29)])
        self.assertEqual(self.conn.query("SELECT hired FROM jp_o_demo WHERE name = ?", ["dt"]).scalar(),
                         datetime(2020, 2, 29))

    def test_unicode_varchar2(self):
        self.conn.execute("INSERT INTO jp_o_demo (name) VALUES (?)", ["数据库Oracle🦄"])
        self.assertEqual(self.conn.query("SELECT name FROM jp_o_demo WHERE name = ?", ["数据库Oracle🦄"]).scalar(),
                         "数据库Oracle🦄")

    def test_transaction_rollback(self):
        self.conn.execute("INSERT INTO jp_o_demo (name) VALUES (?)", ["keep"])
        with self.assertRaises(RuntimeError):
            with self.conn.transaction():
                self.conn.execute("INSERT INTO jp_o_demo (name) VALUES (?)", ["ghost"])
                raise RuntimeError("boom")
        n = self.conn.query("SELECT COUNT(*) FROM jp_o_demo WHERE name = ?", ["ghost"]).scalar()
        self.assertEqual(n, 0)


@_oracle_case
class TestOracleMetadata(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.conn = cls.client.connect(**ORACLE)
        cls.addClassCleanup(cls._teardown)

    @classmethod
    def _teardown(cls):
        cls.conn.close()
        cls.client.close()

    def test_list_tables_defaults_to_user_schema(self):
        tables = self.conn.list_tables()
        names = [t["name"] for t in tables]
        self.assertIn("EMP", names)
        self.assertIn("STUDENTS", names)
        # no system dictionary tables leak in
        self.assertNotIn("HELP", names)

    def test_describe_emp(self):
        cols = self.conn.describe_table("EMP")
        names = [c["name"] for c in cols]
        self.assertEqual(names, ["EMPNO", "ENAME", "JOB", "MGR", "HIREDATE", "SAL", "COMM", "DEPTNO"])
        sal = [c for c in cols if c["name"] == "SAL"][0]
        self.assertEqual(sal["type"], "NUMBER")
        self.assertEqual(sal["precision"], 7)
        self.assertEqual(sal["scale"], 2)

    def test_primary_keys(self):
        pks = self.conn.get_primary_keys("EMP")
        self.assertEqual([p["column"] for p in pks], ["EMPNO"])

    def test_foreign_keys(self):
        fks = self.conn.get_foreign_keys("EMP")
        self.assertEqual(len(fks), 1)
        self.assertEqual(fks[0]["fk_column"], "DEPTNO")
        self.assertEqual(fks[0]["pk_table"], "DEPT")

    def test_type_info(self):
        info = self.conn.get_type_info()
        self.assertTrue(any(t["TYPE_NAME"] == "NUMBER" for t in info))

    def test_14_seed_rows_in_emp(self):
        self.assertEqual(self.conn.query("SELECT COUNT(*) FROM EMP").scalar(), 14)

    def test_schema_explicit_override(self):
        # explicit schema=None searches everywhere; SCOTT tables are still found
        tables = self.conn.list_tables(schema=None)
        self.assertIn("EMP", [t["name"] for t in tables])


if __name__ == "__main__":
    unittest.main(verbosity=2)
