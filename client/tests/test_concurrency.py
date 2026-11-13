"""Concurrency, read-first scheduling and async tests for the jdbc-proxy client.

Validates the server's per-session physical connection pool:
  * parallel reads on ONE logical connection (8 threads / 8 asyncio tasks)
  * reads are not blocked by a long transaction on another thread
  * uncommitted data is invisible across pool connections (session isolation)

Run from client/:
  python3 -m unittest tests.test_concurrency -v
"""

import asyncio
import os
import sys
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_integration import MYSQL, PG, PROXY_URL  # reuse env-configured DSNs

from jdbc_proxy import AsyncJdbcProxyClient, JdbcProxyClient


def _proxy_or_skip(cls):
    try:
        JdbcProxyClient(PROXY_URL, timeout=10).health()
    except Exception as e:
        raise unittest.SkipTest(f"jdbc-proxy not reachable at {PROXY_URL}: {e}")
    return cls


@_proxy_or_skip
class TestParallelReads(unittest.TestCase):
    """8 threads issuing SELECT ... SLEEP on ONE logical connection."""

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.mysql = cls.client.connect(**MYSQL)
        cls.pg = cls.client.connect(**PG)
        cls.addClassCleanup(cls._teardown)

    @classmethod
    def _teardown(cls):
        cls.mysql.close()
        cls.pg.close()
        cls.client.close()

    def test_parallel_reads_mysql(self):
        with ThreadPoolExecutor(max_workers=8) as ex:
            t0 = time.perf_counter()
            results = list(ex.map(
                lambda _: self.mysql.query("SELECT SLEEP(0.3), CONNECTION_ID()").first_row(),
                range(8)))
            elapsed = time.perf_counter() - t0
        self.assertEqual(len(results), 8)
        # distinct physical connections prove real parallelism
        self.assertGreaterEqual(len({r[1] for r in results}), 4,
                                "all queries landed on one physical connection")
        # serialized would be >= 2.4s; parallel ~= 0.3-0.6s
        self.assertLess(elapsed, 1.8, f"reads appear serialized: {elapsed:.2f}s")

    def test_parallel_reads_pg(self):
        with ThreadPoolExecutor(max_workers=8) as ex:
            t0 = time.perf_counter()
            results = list(ex.map(
                lambda _: self.pg.query("SELECT pg_sleep(0.3), pg_backend_pid()").first_row(),
                range(8)))
            elapsed = time.perf_counter() - t0
        self.assertEqual(len(results), 8)
        self.assertGreaterEqual(len({r[1] for r in results}), 4)
        self.assertLess(elapsed, 1.8, f"reads appear serialized: {elapsed:.2f}s")

    def test_mixed_read_write_concurrent(self):
        """Reads and small writes concurrently on one connection — no errors."""
        def work(i):
            if i % 3 == 0:
                r = self.mysql.execute("SELECT 1, ?", [i])
                return r.update_count
            r = self.mysql.query("SELECT SLEEP(0.02), ? AS t", [i])
            return r.first_row()[1]

        with ThreadPoolExecutor(max_workers=9) as ex:
            out = list(ex.map(work, range(9)))
        self.assertEqual(len(out), 9)


@_proxy_or_skip
class TestReadFirst(unittest.TestCase):
    """Reads must not queue behind transactions / long writes on the same session."""

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)
        cls.mysql = cls.client.connect(**MYSQL)
        cls.mysql.execute("CREATE TABLE IF NOT EXISTS jp_rf_test (id INT PRIMARY KEY, name VARCHAR(20))")
        cls.addClassCleanup(cls._teardown)

    @classmethod
    def _teardown(cls):
        try:
            cls.mysql.execute("DROP TABLE IF EXISTS jp_rf_test")
        except Exception:
            pass
        cls.mysql.close()
        cls.client.close()

    def test_reads_not_blocked_by_long_txn(self):
        started = threading.Event()
        done = threading.Event()

        def txn_thread():
            self.mysql.begin()  # pins one physical connection
            started.set()
            try:
                self.mysql.execute("SELECT SLEEP(1.0)")  # long statement inside txn
            finally:
                self.mysql.rollback()
                done.set()

        t = threading.Thread(target=txn_thread)
        t.start()
        self.assertTrue(started.wait(5))
        time.sleep(0.2)  # let the long statement start on the pinned connection

        t0 = time.perf_counter()
        self.mysql.query("SELECT 1")  # intent=read → borrows a different pool connection
        read_elapsed = time.perf_counter() - t0
        self.assertTrue(done.wait(10))
        t.join(timeout=10)
        self.assertLess(read_elapsed, 0.6,
                        f"read blocked behind transaction: {read_elapsed:.2f}s")

    def test_uncommitted_invisible_across_pool(self):
        """A transaction's uncommitted row is invisible to reads on other pool connections."""
        self.mysql.execute("DELETE FROM jp_rf_test")

        inserted = threading.Event()
        finished = threading.Event()

        def txn_thread():
            self.mysql.begin()
            try:
                self.mysql.execute("INSERT INTO jp_rf_test (id, name) VALUES (?, ?)", [99, "uncommitted"])
                inserted.set()
                finished.wait(10)
            finally:
                self.mysql.rollback()

        t = threading.Thread(target=txn_thread)
        t.start()
        self.assertTrue(inserted.wait(5))

        # same logical connection, different thread → intent=read → different physical conn
        n = self.mysql.query("SELECT COUNT(*) FROM jp_rf_test").scalar()
        finished.set()
        t.join(timeout=10)
        self.assertEqual(n, 0, "uncommitted row leaked into a parallel read")

        # after commit it becomes visible
        self.mysql.begin()
        self.mysql.execute("INSERT INTO jp_rf_test (id, name) VALUES (?, ?)", [98, "committed"])
        self.mysql.commit()
        n2 = self.mysql.query("SELECT COUNT(*) FROM jp_rf_test WHERE id = ?", [98]).scalar()
        self.assertEqual(n2, 1)
        self.mysql.execute("DELETE FROM jp_rf_test")


@_proxy_or_skip
class TestAsync(unittest.TestCase):
    """asyncio client: gather of concurrent queries + transaction context."""

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL)  # used only for the skip check

    def _run(self, coro):
        return asyncio.run(coro)

    def test_async_gather_mysql(self):
        async def go():
            async with AsyncJdbcProxyClient(PROXY_URL) as client:
                conn = await client.connect(**MYSQL)
                async with conn:
                    t0 = time.perf_counter()
                    results = await asyncio.gather(*[
                        conn.query("SELECT SLEEP(0.3), CONNECTION_ID()") for _ in range(8)])
                    elapsed = time.perf_counter() - t0
                pids = {r.first_row()[1] for r in results}
                return elapsed, pids

        elapsed, pids = self._run(go())
        self.assertGreaterEqual(len(pids), 4)
        self.assertLess(elapsed, 1.8, f"async reads appear serialized: {elapsed:.2f}s")

    def test_async_gather_pg(self):
        async def go():
            async with AsyncJdbcProxyClient(PROXY_URL) as client:
                conn = await client.connect(**PG)
                async with conn:
                    t0 = time.perf_counter()
                    results = await asyncio.gather(*[
                        conn.query("SELECT pg_sleep(0.3), pg_backend_pid()") for _ in range(8)])
                    elapsed = time.perf_counter() - t0
                pids = {r.first_row()[1] for r in results}
                return elapsed, pids

        elapsed, pids = self._run(go())
        self.assertGreaterEqual(len(pids), 4)
        self.assertLess(elapsed, 1.8, f"async reads appear serialized: {elapsed:.2f}s")

    def test_async_transaction_and_metadata(self):
        async def go():
            async with AsyncJdbcProxyClient(PROXY_URL) as client:
                conn = await client.connect(**PG)
                async with conn:
                    await conn.execute('DROP TABLE IF EXISTS jp_async_test')
                    await conn.execute('CREATE TABLE IF NOT EXISTS jp_async_test (id SERIAL PRIMARY KEY, "name" TEXT)')
                    try:
                        async with conn.transaction():
                            await conn.execute('INSERT INTO jp_async_test ("name") VALUES (?)', ["a"])
                            r = await conn.query('SELECT COUNT(*) FROM jp_async_test')
                            inside = r.scalar()
                    except Exception:
                        inside = None
                    tables = await conn.list_tables(table="jp_async_test")
                    cols = await conn.describe_table("jp_async_test")
                    n = (await conn.query('SELECT COUNT(*) FROM jp_async_test')).scalar()
                    await conn.execute("DROP TABLE IF EXISTS jp_async_test")
                    return inside, len(tables), [c["name"] for c in cols], n

        inside, n_tables, col_names, n = self._run(go())
        self.assertEqual(inside, 1)  # reads inside txn see their own changes
        self.assertEqual(n_tables, 1)
        self.assertEqual(col_names, ["id", "name"])
        self.assertEqual(n, 1)

    def test_async_rollback_on_error(self):
        async def go():
            async with AsyncJdbcProxyClient(PROXY_URL) as client:
                conn = await client.connect(**MYSQL)
                async with conn:
                    await conn.execute("CREATE TABLE IF NOT EXISTS jp_async_rb (id INT PRIMARY KEY, name VARCHAR(20))")
                    try:
                        async with conn.transaction():
                            await conn.execute("INSERT INTO jp_async_rb (id, name) VALUES (?, ?)", [1, "x"])
                            raise RuntimeError("boom")
                    except RuntimeError:
                        pass
                    n = (await conn.query("SELECT COUNT(*) FROM jp_async_rb")).scalar()
                    await conn.execute("DROP TABLE IF EXISTS jp_async_rb")
                    return n

        self.assertEqual(self._run(go()), 0)


@_proxy_or_skip
class TestPhysicalCounterConservation(unittest.TestCase):
    """Regression: physicalConnections counter and global permits must not leak
    across open/close cycles (previously went negative after ~256 borrows)."""

    def test_counter_returns_to_zero(self):
        client = JdbcProxyClient(PROXY_URL)
        self.addCleanup(client.close)
        base = client.health()["physicalConnections"]
        for _ in range(12):
            conn = client.connect(**MYSQL)
            conn.query("SELECT 1").scalar()
            conn.close()
        for _ in range(6):
            conn = client.connect(**PG)
            conn.query("SELECT 1").scalar()
            conn.close()
        self.assertEqual(client.health()["physicalConnections"], base)

    def test_many_sessions_exhaustion_recovery(self):
        """Opening/closing many concurrent sessions must never wedge the global
        permit pool (would surface as 503 'global physical connection limit
        reached' on later connects)."""
        client = JdbcProxyClient(PROXY_URL)
        self.addCleanup(client.close)
        errors = []

        def cycle(i):
            try:
                c = client.connect(**MYSQL if i % 2 == 0 else PG)
                c.query("SELECT 1").scalar()
                c.close()
            except Exception as e:
                errors.append(e)

        with ThreadPoolExecutor(max_workers=16) as ex:
            list(ex.map(cycle, range(48)))
        self.assertEqual(errors, [])
        # the proxy must still be fully usable afterwards
        c = client.connect(**MYSQL)
        c.query("SELECT 1").scalar()
        c.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
