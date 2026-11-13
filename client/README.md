# jdbc-proxy-client

Python client for [jdbc-proxy](../README.md) — call JDBC drivers of any database through the proxy's HTTP API.

Pure standard library (no third-party dependencies), Python >= 3.9.

```bash
pip install .
```

```python
from jdbc_proxy import JdbcProxyClient

with JdbcProxyClient("http://127.0.0.1:8089") as client:
    with client.connect(url="jdbc:mysql://127.0.0.1:3306/default_db",
                        user="root", password="***") as conn:
        rows = conn.query("SELECT * FROM EMP WHERE deptno > ?", [10]).dict_rows()
        res = conn.execute("INSERT INTO students (name) VALUES (?)", ["Tom"])
        batch = conn.execute_many("INSERT INTO students (name) VALUES (?)", [["a"], ["b"]])
        print(conn.describe_table("EMP"), conn.list_tables(), conn.get_primary_keys("EMP"))
```

See the [main README](../README.md) for the full API and protocol documentation.

Tests (needs the proxy + MySQL/PostgreSQL running):

```bash
python3 -m unittest tests.test_integration -v
```
