"""Server-flag tests: spawn a dedicated jdbc-proxy instance to verify
--api-key authentication and --max-connections enforcement end to end.

Run from client/:
  python3 -m unittest tests.test_server_flags -v
"""

import os
import subprocess
import sys
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_integration import MYSQL, PROXY_URL

from jdbc_proxy import JdbcProxyClient, JdbcProxyError

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
JAR = os.path.join(REPO, "server", "target", "jdbc-proxy-server.jar")
JARS_DIR = os.path.join(REPO, "jars")
PORT = 8091
BASE = f"http://127.0.0.1:{PORT}"
PORT2 = 8092
BASE2 = f"http://127.0.0.1:{PORT2}"
KEY = "unit-test-key"


def _spawn(port, extra_args):
    proc = subprocess.Popen(
        ["java", "-jar", JAR, "--host", "127.0.0.1", "--port", str(port),
         "--drivers-dir", JARS_DIR, *extra_args],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 40
    base = f"http://127.0.0.1:{port}"
    while time.time() < deadline:
        try:
            JdbcProxyClient(base, timeout=2).health()
            return proc
        except Exception:
            time.sleep(0.3)
    proc.terminate()
    raise RuntimeError(f"test server on :{port} did not become healthy")


def _stop(proc):
    if proc is not None:
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()


@unittest.skipUnless(os.path.exists(JAR), "server jar not built (run: mvn -pl server package)")
class TestServerAuthAndLimits(unittest.TestCase):
    proc = None

    @classmethod
    def setUpClass(cls):
        cls.proc = _spawn(PORT, ["--api-key", KEY, "--max-connections", "2", "--idle-timeout", "0"])

    @classmethod
    def tearDownClass(cls):
        _stop(cls.proc)

    def test_health_is_exempt_from_auth(self):
        h = JdbcProxyClient(BASE, timeout=5).health()  # no key
        self.assertEqual(h["status"], "ok")

    def test_missing_key_rejected_401(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            JdbcProxyClient(BASE, timeout=5).drivers()
        self.assertEqual(ctx.exception.status, 401)

    def test_wrong_key_rejected_401(self):
        with self.assertRaises(JdbcProxyError) as ctx:
            JdbcProxyClient(BASE, api_key="nope", timeout=5).drivers()
        self.assertEqual(ctx.exception.status, 401)

    def test_x_api_key_accepted(self):
        client = JdbcProxyClient(BASE, api_key=KEY, timeout=5)
        drivers = client.drivers()
        self.assertTrue(any(d["className"] == "com.mysql.cj.jdbc.Driver" for d in drivers))

    def test_bearer_authorization_accepted(self):
        client = JdbcProxyClient(BASE, timeout=5)
        client._t.api_key = None
        import http.client
        conn = http.client.HTTPConnection("127.0.0.1", PORT, timeout=5)
        conn.request("GET", "/drivers", headers={"Authorization": f"Bearer {KEY}"})
        resp = conn.getresponse()
        body = resp.read()
        conn.close()
        self.assertEqual(resp.status, 200)
        self.assertIn("com.mysql.cj.jdbc.Driver", body.decode())

    def test_max_connections_enforced_503(self):
        client = JdbcProxyClient(BASE, api_key=KEY, timeout=10)
        c1 = client.connect(**MYSQL)
        c2 = client.connect(**MYSQL)
        with self.assertRaises(JdbcProxyError) as ctx:
            client.connect(**MYSQL)
        self.assertEqual(ctx.exception.status, 503)
        c2.close()  # frees a slot
        c3 = client.connect(**MYSQL)  # now allowed
        self.assertTrue(c3.ping())
        c1.close()
        c3.close()


@unittest.skipUnless(os.path.exists(JAR), "server jar not built (run: mvn -pl server package)")
class TestKeepaliveAndReconnect(unittest.TestCase):
    """Dedicated server with aggressive keepalive (1s) to prove, end to end:

    * background sweep drops DB-side-killed connections
    * borrow-time stale check transparently re-establishes connections
    * a killed session never surfaces to the client as a failed read
    """

    proc = None

    @classmethod
    def setUpClass(cls):
        cls.proc = _spawn(PORT2, [
            "--keepalive-interval", "1",   # probe idle conns older than 1s
            "--keepalive-timeout", "2",
            "--pool-size", "2",
            "--idle-timeout", "0",
        ])

    @classmethod
    def tearDownClass(cls):
        _stop(cls.proc)

    def _kill_backend(self, conn):
        """Terminate the underlying MySQL connection with KILL."""
        backend = conn.query("SELECT CONNECTION_ID()").scalar()
        client = JdbcProxyClient(BASE2, timeout=10)
        admin = client.connect(**MYSQL)
        try:
            admin.execute(f"KILL {int(backend)}")
        finally:
            admin.close()
            client.close()
        return backend

    def test_borrow_time_stale_check_reconnects_transparently(self):
        client = JdbcProxyClient(BASE2, timeout=15)
        self.addCleanup(client.close)
        conn = client.connect(**MYSQL)
        old_backend = self._kill_backend(conn)
        time.sleep(2.5)  # keepalive sweep runs every 1s; idleSince passes the threshold

        r = conn.query("SELECT CONNECTION_ID()").scalar()  # must not raise
        self.assertNotEqual(r, old_backend, "expected a fresh physical connection")

    def test_background_sweep_drops_dead_connections(self):
        client = JdbcProxyClient(BASE2, timeout=15)
        self.addCleanup(client.close)
        conn = client.connect(**MYSQL)
        before = client.health()["physicalConnections"]
        self._kill_backend(conn)
        time.sleep(3.0)  # allow at least two sweeps
        after = client.health()["physicalConnections"]
        self.assertEqual(after, before - 1, "dead connection was not reaped by the keepalive sweep")
        # pool lazily rebuilds on next use
        self.assertTrue(conn.ping())
        self.assertEqual(client.health()["physicalConnections"], before)

    def test_counter_conserves_across_kill_cycles(self):
        client = JdbcProxyClient(BASE2, timeout=15)
        self.addCleanup(client.close)
        base = client.health()["physicalConnections"]
        for _ in range(4):
            conn = client.connect(**MYSQL)
            self._kill_backend(conn)
            time.sleep(2.2)
            conn.query("SELECT 1").scalar()  # transparent reconnect
            conn.close()
        self.assertEqual(client.health()["physicalConnections"], base)


@unittest.skipUnless(os.path.exists(JAR), "server jar not built")
class TestActiveInfo(unittest.TestCase):
    """create/list/info responses expose liveness info (main server, defaults)."""

    @classmethod
    def setUpClass(cls):
        cls.client = JdbcProxyClient(PROXY_URL, timeout=5)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def test_create_response_has_active(self):
        resp = self.client._t.request("POST", "/api/connections", {**MYSQL, "dialect": "mysql"})
        self.assertTrue(resp["active"])
        self.assertIsNotNone(resp.get("lastValidatedAt"))
        self.client._t.request("DELETE", f"/api/connections/{resp['connectionId']}")

    def test_list_and_info_have_active(self):
        conn = self.client.connect(**MYSQL)
        info = self.client._t.request("GET", f"/api/connections/{conn.id}")
        self.assertTrue(info["active"])
        self.assertIsNotNone(info.get("lastValidatedAt"))
        listed = [c for c in self.client.connections() if c["id"] == conn.id]
        self.assertTrue(listed[0]["active"])
        conn.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
