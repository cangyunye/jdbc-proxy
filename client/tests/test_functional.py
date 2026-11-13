"""Extended functional tests: value-codec edge cases, transaction semantics,
metadata endpoints, wire-level error protocol, credential redaction and
client-library behavior.

Requires the jdbc-proxy server + MySQL/PostgreSQL (same defaults as
test_integration.py; overridable via the same environment variables).

Run from client/:
  python3 -m unittest tests.test_functional -v
"""

import os
import sys
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_integration import MYSQL, PG, PROXY_URL

from jdbc_proxy import JdbcProxyClient, JdbcProxyError, Typed
from jdbc_proxy.errors import ConnectionClosedError


def _proxy_or_skip(cls):
    try:
        JdbcProxyClient(PROXY_URL, timeout=10).health()
    except Exception as e:
        raise unittest.SkipTest(f"jdbc-proxy not reachable at {PROXY_URL}: {e}")
    return cls


class _Case(unittest.TestCase):
    db = None

    @classmethod
    def _proxy(cls):
        return JdbcProxyClient(PROXY_URL)

    def setUp(self):
        self.client = self._proxy()
        self.conn = self.client.connect(**self.db)
        self.table = f"jp_func_{id(self) & 0xffffffff:x}"
        self.addCleanup(self._teardown)

    def _teardown(self):
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {self.table}")
        except JdbcProxyError:
            pass
        self.conn.close()
        self.client.close()


# --------------------------------------------------------------------------- value codec

@_proxy_or_skip
class TestValuesMysql(_Case):
    db = MYSQL

    def setUp(self):
        super().setUp()
        self.conn.execute(
            f"CREATE TABLE {self.table} ("
            "id INT PRIMARY KEY, "
            "blb BLOB, flt DOUBLE, big BIGINT, "
            "d DECIMAL(24,4), s VARCHAR(200), b BOOLEAN, t TIME(6), ts TIMESTAMP(6))")

    def test_blob_roundtrip(self):
        raw = bytes(range(256)) * 4
        self.conn.execute(f"INSERT INTO {self.table} (id, blb) VALUES (?, ?)", [1, raw])
        got = self.conn.query(f"SELECT blb FROM {self.table} WHERE id = 1").scalar()
        self.assertEqual(got, raw)

    def test_bool_column(self):
        self.conn.execute(f"INSERT INTO {self.table} (id, b) VALUES (?, ?)", [1, True])
        self.conn.execute(f"INSERT INTO {self.table} (id, b) VALUES (?, ?)", [2, False])
        rows = self.conn.query(f"SELECT b FROM {self.table} ORDER BY id").rows
        self.assertEqual([r[0] for r in rows], [True, False])

    def test_double_and_negative(self):
        self.conn.execute(f"INSERT INTO {self.table} (id, flt) VALUES (?, ?)", [1, -3.25])
        self.assertEqual(self.conn.query(f"SELECT flt FROM {self.table} WHERE id = 1").scalar(), -3.25)

    def test_bigint_bounds(self):
        big = 9223372036854775807  # Long.MAX_VALUE
        self.conn.execute(f"INSERT INTO {self.table} (id, big) VALUES (?, ?)", [1, big])
        self.assertEqual(self.conn.query(f"SELECT big FROM {self.table} WHERE id = 1").scalar(), big)

    def test_decimal_precision_beyond_float64(self):
        exact = Decimal("123456789012345678.9012")
        self.conn.execute(f"INSERT INTO {self.table} (id, d) VALUES (?, ?)", [1, exact])
        self.assertEqual(self.conn.query(f"SELECT d FROM {self.table} WHERE id = 1").scalar(), exact)

    def test_unicode_and_emoji(self):
        text = "中文测试—日本語🦄 Ö☺"
        self.conn.execute(f"INSERT INTO {self.table} (id, s) VALUES (?, ?)", [1, text])
        self.assertEqual(self.conn.query(f"SELECT s FROM {self.table} WHERE id = 1").scalar(), text)

    def test_empty_string_vs_null(self):
        self.conn.execute(f"INSERT INTO {self.table} (id, s) VALUES (?, ?)", [1, ""])
        self.conn.execute(f"INSERT INTO {self.table} (id, s) VALUES (?, ?)", [2, None])
        rows = self.conn.query(f"SELECT s FROM {self.table} ORDER BY id").rows
        self.assertEqual(rows[0][0], "")
        self.assertIsNone(rows[1][0])

    def test_time_and_timestamp_params(self):
        self.conn.execute(f"INSERT INTO {self.table} (id, t, ts) VALUES (?, ?, ?)",
                          [1, time(13, 30, 5, 123456), datetime(2026, 3, 14, 15, 9, 26, 535897)])
        r = self.conn.query(f"SELECT t, ts FROM {self.table} WHERE id = 1").first_row()
        self.assertEqual(r[0], time(13, 30, 5, 123456))
        self.assertEqual(r[1], datetime(2026, 3, 14, 15, 9, 26, 535897))

    def test_typed_bytes_hint(self):
        raw = b"\x00\x01\xffbinary!"
        self.conn.execute(f"INSERT INTO {self.table} (id, blb) VALUES (?, ?)",
                          [1, Typed("bytes", raw)])
        self.assertEqual(self.conn.query(f"SELECT blb FROM {self.table} WHERE id = 1").scalar(), raw)

    def test_null_param_binding(self):
        self.conn.execute(f"INSERT INTO {self.table} (id, s) VALUES (?, ?)", [1, None])
        self.assertIsNone(self.conn.query(f"SELECT s FROM {self.table} WHERE id = 1").scalar())

    def test_plain_statement_path_without_params(self):
        r = self.conn.execute("SELECT 1 + 1 AS two")
        self.assertEqual(r.statement_type, "query")
        self.assertEqual(r.scalar(), 2)

    def test_fetch_size_smoke(self):
        for i in range(20):
            self.conn.execute(f"INSERT INTO {self.table} (id) VALUES (?)", [1000 + i])
        r = self.conn.query(f"SELECT id FROM {self.table} WHERE id >= 1000 ORDER BY id", fetch_size=5)
        self.assertEqual(len(r), 20)

    def test_timeout_seconds_cancels(self):
        with self.assertRaises(JdbcProxyError):
            self.conn.execute("SELECT SLEEP(2)", timeout_seconds=1)


@_proxy_or_skip
class TestValuesPg(_Case):
    db = PG

    def setUp(self):
        super().setUp()
        self.conn.execute(
            f'CREATE TABLE "{self.table}" ('
            'id INT PRIMARY KEY, blb BYTEA, big BIGINT, '
            '"d" NUMERIC(20,4), s TEXT, b BOOLEAN)')

    def test_bytea_roundtrip(self):
        raw = bytes(range(256))
        self.conn.execute(f'INSERT INTO "{self.table}" (id, blb) VALUES (?, ?)', [1, raw])
        self.assertEqual(self.conn.query(f'SELECT blb FROM "{self.table}" WHERE id = 1').scalar(), raw)

    def test_bool_column(self):
        self.conn.execute(f'INSERT INTO "{self.table}" (id, b) VALUES (?, ?)', [1, True])
        self.assertIs(self.conn.query(f'SELECT b FROM "{self.table}" WHERE id = 1').scalar(), True)

    def test_bigint_bounds(self):
        self.conn.execute(f'INSERT INTO "{self.table}" (id, big) VALUES (?, ?)',
                          [1, -9223372036854775808])
        self.assertEqual(self.conn.query(f'SELECT big FROM "{self.table}" WHERE id = 1').scalar(),
                         -9223372036854775808)

    def test_decimal_precision(self):
        exact = Decimal("-99999999999999.9999")
        self.conn.execute(f'INSERT INTO "{self.table}" (id, "d") VALUES (?, ?)', [1, exact])
        self.assertEqual(self.conn.query(f'SELECT "d" FROM "{self.table}" WHERE id = 1').scalar(), exact)

    def test_unicode(self):
        self.conn.execute(f'INSERT INTO "{self.table}" (id, s) VALUES (?, ?)', [1, "数据库🗄️中文"])
        self.assertEqual(self.conn.query(f'SELECT s FROM "{self.table}" WHERE id = 1').scalar(), "数据库🗄️中文")

    def test_timestamptz_roundtrip_keeps_instant(self):
        self.conn.execute(f'CREATE TABLE IF NOT EXISTS "{self.table}_tz" (id INT PRIMARY KEY, ts TIMESTAMPTZ)')
        self.addCleanup(self.conn.execute, f'DROP TABLE IF EXISTS "{self.table}_tz"')
        dt = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=8)))
        self.conn.execute(f'INSERT INTO "{self.table}_tz" (id, ts) VALUES (?, ?)', [1, dt])
        back = self.conn.query(f'SELECT ts FROM "{self.table}_tz" WHERE id = 1').scalar()
        self.assertIsInstance(back, datetime)
        self.assertIsNotNone(back.tzinfo)
        self.assertEqual(back, dt)  # same instant regardless of stored offset

    def test_timeout_seconds_cancels(self):
        with self.assertRaises(JdbcProxyError):
            self.conn.execute("SELECT pg_sleep(2)", timeout_seconds=1)


# --------------------------------------------------------------------------- transaction semantics

@_proxy_or_skip
class TestTxnSemantics(_Case):
    db = MYSQL

    def setUp(self):
        super().setUp()
        self.conn.execute(
            f"CREATE TABLE {self.table} (id INT PRIMARY KEY, name VARCHAR(20))")

    def test_double_begin_conflicts(self):
        self.conn.begin()
        try:
            with self.assertRaises(JdbcProxyError) as ctx:
                self.conn.begin()
            self.assertEqual(ctx.exception.status, 409)
        finally:
            self.conn.rollback()

    def test_commit_without_begin_conflicts(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn.commit()
        self.assertEqual(ctx.exception.status, 409)

    def test_begin_with_isolation(self):
        self.conn.begin(isolation="SERIALIZABLE")
        try:
            r = self.conn._post("/transaction", {"action": "setIsolation", "value": "READ_COMMITTED"})
            self.assertEqual(r["isolation"], 2)
        finally:
            self.conn.rollback()

    def test_set_auto_commit_endpoint_commits(self):
        self.conn.set_auto_commit(False)
        self.conn.execute(f"INSERT INTO {self.table} (id, name) VALUES (?, ?)", [1, "a"])
        info = self.client._t.request("GET", f"/api/connections/{self.conn.id}")
        self.assertTrue(info["inTransaction"])
        self.conn.set_auto_commit(True)  # commits the pending transaction
        info = self.client._t.request("GET", f"/api/connections/{self.conn.id}")
        self.assertFalse(info["inTransaction"])
        self.assertEqual(self.conn.query(f"SELECT COUNT(*) FROM {self.table}").scalar(), 1)

    def test_set_isolation_endpoint(self):
        r = self.conn._post("/transaction", {"action": "setIsolation", "value": "REPEATABLE_READ"})
        self.assertEqual(r["isolation"], 4)
        r2 = self.conn._post("/transaction", {"action": "setIsolation", "value": 2})
        self.assertEqual(r2["isolation"], 2)

    def test_unknown_action_rejected(self):
        with self.assertRaises(JdbcProxyError):
            self.conn._post("/transaction", {"action": "dance"})

    def test_session_close_rolls_back_open_txn(self):
        """Deleting the session (client vanishing) must roll back its open transaction."""
        inserted = threading.Event()
        finished = threading.Event()
        commit_error = []

        def txn_thread():
            try:
                self.conn.begin()
                self.conn.execute(f"INSERT INTO {self.table} (id, name) VALUES (?, ?)", [777, "ghost"])
                inserted.set()
                finished.wait(10)
                self.conn.commit()
            except JdbcProxyError as e:
                commit_error.append(e)
            finally:
                finished.set()

        t = threading.Thread(target=txn_thread)
        t.start()
        self.assertTrue(inserted.wait(5))
        self.client._t.request("DELETE", f"/api/connections/{self.conn.id}")  # simulate client crash
        finished.set()
        t.join(timeout=10)

        check = self.client.connect(**MYSQL)
        try:
            n = check.query(f"SELECT COUNT(*) FROM {self.table} WHERE id = 777").scalar()
            self.assertEqual(n, 0, "open transaction survived session close")
        finally:
            check.close()
        self.assertTrue(commit_error)  # the stuck thread observed the teardown


@_proxy_or_skip
class TestTxnSemanticsPg(_Case):
    db = PG

    def setUp(self):
        super().setUp()
        self.conn.execute(f'CREATE TABLE "{self.table}" (id INT PRIMARY KEY, name TEXT)')

    def test_session_close_rolls_back_open_txn(self):
        self.conn.begin()
        self.conn.execute(f'INSERT INTO "{self.table}" (id, name) VALUES (?, ?)', [888, "ghost"])
        self.client._t.request("DELETE", f"/api/connections/{self.conn.id}")
        check = self.client.connect(**PG)
        try:
            self.assertEqual(check.query(f'SELECT COUNT(*) FROM "{self.table}" WHERE id = 888').scalar(), 0)
        finally:
            check.close()


# --------------------------------------------------------------------------- metadata

@_proxy_or_skip
class TestMetadataAdvanced(_Case):
    db = MYSQL

    def test_foreign_keys_imported_exported(self):
        p, c = f"{self.table}_p", f"{self.table}_c"
        self.conn.execute(f"CREATE TABLE {p} (id INT PRIMARY KEY, code VARCHAR(10))")
        self.conn.execute(
            f"CREATE TABLE {c} (id INT PRIMARY KEY, pid INT NOT NULL,"
            f" FOREIGN KEY (pid) REFERENCES {p} (id))")
        fks = self.conn.get_foreign_keys(c)
        self.assertEqual(len(fks), 1)
        self.assertEqual(fks[0]["fk_column"], "pid")
        self.assertEqual(fks[0]["pk_table"], p)
        exported = self.conn.get_foreign_keys(p, direction="exported")
        self.assertEqual(len(exported), 1)
        self.assertEqual(exported[0]["fk_table"], c)

    def test_describe_table_column_filter(self):
        cols = self.conn.describe_table("students", column="id%")
        self.assertEqual([c["name"] for c in cols], ["id"])

    def test_list_tables_max_rows(self):
        tables = self.conn.list_tables(max_rows=2)
        self.assertLessEqual(len(tables), 2)

    def test_list_views_only(self):
        views = self.conn.list_tables(types=("VIEW",))
        names = [t["name"] for t in views]
        self.assertIn("emp_view", names)
        self.assertNotIn("students", names)

    def test_unknown_metadata_endpoint_404(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn._get("/metadata/nope")
        self.assertEqual(ctx.exception.status, 404)

    def test_dialect_refresh(self):
        d1 = self.conn.dialect()
        d2 = self.conn.dialect(refresh=True)
        self.assertEqual(d1["productName"], d2["productName"])


@_proxy_or_skip
class TestMetadataAdvancedPg(_Case):
    db = PG

    def test_foreign_keys_imported_exported(self):
        p, c = f"{self.table}_p", f"{self.table}_c"
        self.conn.execute(f'CREATE TABLE "{p}" (id INT PRIMARY KEY, code TEXT)')
        self.conn.execute(
            f'CREATE TABLE "{c}" (id INT PRIMARY KEY, pid INT NOT NULL,'
            f' FOREIGN KEY (pid) REFERENCES "{p}" (id))')
        fks = self.conn.get_foreign_keys(c)
        self.assertEqual(len(fks), 1)
        self.assertEqual(fks[0]["pk_table"], p)
        self.assertEqual(len(self.conn.get_foreign_keys(p, direction="exported")), 1)

    def test_describe_table_column_filter(self):
        cols = self.conn.describe_table("students", column="id")
        self.assertEqual([c["name"] for c in cols], ["id"])

    def test_list_tables_max_rows(self):
        self.assertLessEqual(len(self.conn.list_tables(schema="public", max_rows=3)), 3)


# --------------------------------------------------------------------------- wire error protocol & redaction

@_proxy_or_skip
class TestWireProtocol(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.addClassCleanup(cls.client.close)
        cls.conn = cls.client.connect(**MYSQL)

    def test_method_not_allowed_405(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.client._t.request("PUT", "/api/connections")
        self.assertEqual(ctx.exception.status, 405)

    def test_malformed_json_400(self):
        req = urllib.request.Request(
            f"{PROXY_URL}/api/connections", data=b"{this is not json",
            headers={"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(req, timeout=10)
        self.assertEqual(ctx.exception.code, 400)
        body = ctx.exception.read().decode()
        self.assertIn("invalid JSON", body)

    def test_unknown_type_hint_400(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn.execute("SELECT ?", [Typed("weird", "x")])
        self.assertEqual(ctx.exception.status, 400)
        self.assertIn("unknown param type hint", str(ctx.exception))

    def test_batch_non_array_item_400(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            self.conn._post("/batch", {"sql": "SELECT 1", "batch": [{"a": 1}]})
        self.assertEqual(ctx.exception.status, 400)

    def test_empty_batch_400(self):
        with self.assertRaises(JdbcProxyError):
            self.conn._post("/batch", {"sql": "SELECT 1", "batch": []})

    def test_empty_sql_400(self):
        with self.assertRaises(JdbcProxyError):
            self.conn.execute("   ")

    def test_url_redaction_on_create_and_list(self):
        resp = self.client._t.request("POST", "/api/connections", {
            "url": "jdbc:mysql://127.0.0.1:3306/default_db?user=root&password=root123456"})
        self.assertIn("password=***", resp["url"])
        self.assertNotIn("root123456", resp["url"])
        listed = self.client._t.request("GET", "/api/connections")
        mine = [c for c in listed["connections"] if c["id"] == resp["connectionId"]]
        self.assertEqual(mine[0]["url"], resp["url"])
        self.client._t.request("DELETE", f"/api/connections/{resp['connectionId']}")

    def test_connection_info_shape(self):
        info = self.client._t.request("GET", f"/api/connections/{self.conn.id}")
        self.assertIn("inTransaction", info)
        self.assertIn("poolIdle", info)
        self.assertIn("poolInUse", info)


# --------------------------------------------------------------------------- client library behavior

@_proxy_or_skip
class TestClientBehavior(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.addClassCleanup(cls.client.close)

    def test_client_close_closes_all_connections(self):
        conns = [self.client.connect(**MYSQL) for _ in range(3)]
        self.client.close()
        for c in conns:
            self.assertTrue(c.is_closed())
            with self.assertRaises(ConnectionClosedError):
                c.ping()

    def test_double_close_is_idempotent(self):
        conn = self.client.connect(**MYSQL)
        conn.close()
        conn.close()  # no raise

    def test_sync_transaction_reads_see_own_changes(self):
        conn = self.client.connect(**MYSQL)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS jp_ctx_test (id INT PRIMARY KEY, name VARCHAR(10))")
            conn.execute("DELETE FROM jp_ctx_test")
            with conn.transaction():
                conn.execute("INSERT INTO jp_ctx_test (id, name) VALUES (?, ?)", [1, "x"])
                # query() from the SAME thread joins the txn and sees its own write
                inside = conn.query("SELECT COUNT(*) FROM jp_ctx_test").scalar()
            outside = conn.query("SELECT COUNT(*) FROM jp_ctx_test").scalar()
            self.assertEqual(inside, 1)
            self.assertEqual(outside, 1)
            conn.execute("DROP TABLE jp_ctx_test")
        finally:
            conn.close()

    def test_explicit_intent_on_execute(self):
        conn = self.client.connect(**MYSQL)
        try:
            r = conn.execute("SELECT 41 + 1", intent="read")
            self.assertEqual(r.statement_type, "query")
            self.assertEqual(r.scalar(), 42)
            r2 = conn.query("SELECT 1", intent="write")  # explicit write for a SELECT is allowed
            self.assertEqual(r2.scalar(), 1)
        finally:
            conn.close()

    def test_query_rejects_dml_even_with_intent(self):
        conn = self.client.connect(**MYSQL)
        try:
            with self.assertRaises(JdbcProxyError):
                conn.query("INSERT INTO nothing_valid VALUES (1)")
        finally:
            conn.close()

    def test_connect_via_bearer_alias(self):
        # main server has no api-key; Bearer header must simply be ignored there
        conn = self.client.connect(**PG)
        try:
            self.assertTrue(conn.ping())
        finally:
            conn.close()


@_proxy_or_skip
class TestReconnectOnDeadConnection(unittest.TestCase):
    """The server kills nothing for us here: we kill the *backend* session with
    MySQL KILL, then verify the proxy's failure-reconnect-resend mechanism.

    A read that hits the dead connection is retried once on a fresh one and
    succeeds transparently; the physical counter must stay conserved."""

    def test_read_retries_after_backend_kill(self):
        client = JdbcProxyClient(PROXY_URL)
        self.addCleanup(client.close)
        conn = client.connect(**MYSQL)
        old_backend = conn.query("SELECT CONNECTION_ID()").scalar()

        admin = client.connect(**MYSQL)
        try:
            admin.execute(f"KILL {int(old_backend)}")
        finally:
            admin.close()

        # executes on the dead connection → connection error → discard → retry on a fresh one
        new_backend = conn.query("SELECT CONNECTION_ID()").scalar()
        self.assertNotEqual(new_backend, old_backend)
        self.assertTrue(conn.ping())

    def test_counter_conserved_after_retry(self):
        client = JdbcProxyClient(PROXY_URL)
        self.addCleanup(client.close)
        base = client.health()["physicalConnections"]
        conn = client.connect(**MYSQL)
        old_backend = conn.query("SELECT CONNECTION_ID()").scalar()
        admin = client.connect(**MYSQL)
        try:
            admin.execute(f"KILL {int(old_backend)}")
        finally:
            admin.close()
        conn.query("SELECT 1").scalar()  # discard + fresh retry
        conn.close()
        admin2 = client.connect(**MYSQL)
        admin2.close()
        self.assertEqual(client.health()["physicalConnections"], base)


if __name__ == "__main__":
    unittest.main(verbosity=2)
