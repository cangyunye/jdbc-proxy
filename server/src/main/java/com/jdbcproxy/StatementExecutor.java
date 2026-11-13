package com.jdbcproxy;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;

/**
 * Implements /execute, /batch and /transaction on a pooled session.
 *
 * <p>Scheduling semantics:
 * <ul>
 *   <li>{@code intent=read} statements always run autocommit on a pool connection
 *       (reader-priority queue) — they never wait behind transactions.</li>
 *   <li>Default (write) statements join the session's active transaction (pinned
 *       connection) when one exists, otherwise borrow from the writer queue.</li>
 *   <li>Read statements that hit a dead connection are retried once on a fresh
 *       connection (SELECT is idempotent; writes are never retried).</li>
 * </ul>
 */
final class StatementExecutor {
    private static final Map<String, Integer> ISOLATIONS = Map.of(
            "NONE", Connection.TRANSACTION_NONE,
            "READ_UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED,
            "READ_COMMITTED", Connection.TRANSACTION_READ_COMMITTED,
            "REPEATABLE_READ", Connection.TRANSACTION_REPEATABLE_READ,
            "SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);

    private StatementExecutor() {
    }

    // ---------------------------------------------------------------- execute

    static JsonObject execute(JdbcSession s, JsonObject req, Config cfg) {
        String sql = requireSql(req);
        JsonArray params = optArray(req, "params");
        boolean usePrepared = params != null && params.size() > 0;
        int hardCap = cfg.maxRows();
        Integer reqMaxRows = optInt(req, "maxRows");
        int maxRows = reqMaxRows == null || reqMaxRows <= 0 ? hardCap : Math.min(reqMaxRows, hardCap);
        Integer timeout = optInt(req, "timeoutSeconds");
        int fetchSize = optInt(req, "fetchSize") == null ? 0 : optInt(req, "fetchSize");
        boolean readIntent = "read".equalsIgnoreCase(getString(req, "intent", "write"));
        boolean pgLike = s.isPgLike();

        s.touch();

        // Inside a transaction: all non-read statements run on the pinned connection
        // (so the client sees its own uncommitted changes), serialized by txnLock.
        if (!readIntent && s.inTransaction()) {
            s.txnLock().lock();
            try {
                return runOn(s.pinned(), sql, params, usePrepared, maxRows, timeout, fetchSize, pgLike, true);
            } catch (SQLException e) {
                throw sqlError(e);
            } finally {
                s.txnLock().unlock();
            }
        }

        PhysicalPool pool = s.pool();
        PooledConn pc = pool.borrow(readIntent, false);
        try {
            try {
                JsonObject out = runOn(pc, sql, params, usePrepared, maxRows, timeout, fetchSize, pgLike, true);
                s.touch(); // statement completed: connection proven alive
                return out;
            } catch (SQLException e) {
                if (isConnectionError(e)) {
                    s.markDead();
                    pool.discard(pc);
                    pc = null;
                    if (readIntent) { // SELECT is idempotent — transparent single retry
                        PooledConn fresh = pool.borrow(true, false);
                        try {
                            return runOn(fresh, sql, params, usePrepared, maxRows, timeout, fetchSize, pgLike, true);
                        } catch (SQLException e2) {
                            throw sqlError(e2);
                        } finally {
                            pool.releaseSafely(fresh);
                        }
                    }
                }
                throw sqlError(e);
            }
        } finally {
            if (pc != null) pool.releaseSafely(pc);
        }
    }

    private static JsonObject runOn(PooledConn pc, String sql, JsonArray params, boolean usePrepared,
                                    int maxRows, Integer timeout, int fetchSize,
                                    boolean pgLike, boolean allowGeneratedKeys) throws SQLException {
        JsonObject out = new JsonObject();
        if (usePrepared) {
            PreparedStatement ps = pc.prepared(sql); // cached, RETURN_GENERATED_KEYS
            applySettings(ps, timeout, maxRows, fetchSize);
            bind(ps, params, pgLike);
            boolean isQuery = ps.execute();
            if (isQuery) {
                readQueryResult(ps, out, maxRows);
            } else {
                fillUpdate(out, ps, maxRows, allowGeneratedKeys);
            }
            return out;
        }
        Statement stmt = pc.conn.createStatement();
        try {
            applySettings(stmt, timeout, maxRows, fetchSize);
            boolean isQuery = stmt.execute(sql);
            if (isQuery) {
                readQueryResult(stmt, out, maxRows);
            } else {
                fillUpdate(out, stmt, maxRows, allowGeneratedKeys);
            }
        } finally {
            stmt.close();
        }
        return out;
    }

    private static void fillUpdate(JsonObject out, Statement stmt, int maxRows, boolean allowGeneratedKeys) throws SQLException {
        out.addProperty("statementType", "update");
        out.addProperty("updateCount", stmt.getUpdateCount());
        // dialects such as PostgreSQL expose INSERT ... RETURNING rows even though
        // execute() reported an update; attach them when present
        addReturningRows(stmt, out, maxRows);
        // only DML reaches this path now, so getGeneratedKeys is worth a roundtrip
        JsonObject keys = allowGeneratedKeys ? readGeneratedKeys(stmt) : null;
        out.add("generatedKeys", keys == null ? JsonNull.INSTANCE : keys);
    }

    private static void readQueryResult(Statement stmt, JsonObject out, int maxRows) throws SQLException {
        try (ResultSet rs = stmt.getResultSet()) {
            if (rs == null) {
                // driver lied about isQuery; degrade gracefully
                out.addProperty("statementType", "update");
                out.addProperty("updateCount", stmt.getUpdateCount());
                return;
            }
            ResultSetMetaData md = rs.getMetaData();
            JsonArray cols = ValueCodec.describeColumns(md);
            JsonArray rows = new JsonArray();
            while (rows.size() < maxRows && rs.next()) {
                rows.add(ValueCodec.rowToJson(rs, md));
            }
            boolean truncated = rows.size() >= maxRows && rs.next(); // sentinel row present?
            out.addProperty("statementType", "query");
            out.add("columns", cols);
            out.add("rows", rows);
            out.addProperty("rowCount", rows.size());
            out.addProperty("truncated", truncated);
            out.add("updateCount", JsonNull.INSTANCE);
        }
    }

    private static void addReturningRows(Statement stmt, JsonObject out, int maxRows) {
        try (ResultSet rs = stmt.getResultSet()) {
            if (rs == null) return;
            ResultSetMetaData md = rs.getMetaData();
            JsonArray cols = ValueCodec.describeColumns(md);
            JsonArray rows = new JsonArray();
            while (rows.size() < maxRows && rs.next()) {
                rows.add(ValueCodec.rowToJson(rs, md));
            }
            out.add("columns", cols);
            out.add("rows", rows);
            out.addProperty("rowCount", rows.size());
        } catch (SQLException ignored) {
            // driver refuses getResultSet() after an update; nothing to attach
        }
    }

    // ---------------------------------------------------------------- batch

    static JsonObject batch(JdbcSession s, JsonObject req, Config cfg) {
        String sql = requireSql(req);
        JsonArray batch = optArray(req, "batch");
        if (batch == null || batch.size() == 0) {
            throw new ApiException(400, "'batch' must be a non-empty array of parameter arrays");
        }
        if (batch.size() > cfg.maxBatch()) {
            throw new ApiException(400, "batch size " + batch.size() + " exceeds --max-batch " + cfg.maxBatch());
        }
        for (JsonElement row : batch) {
            if (!row.isJsonArray()) {
                throw new ApiException(400, "each batch item must be a parameter array, got: " + row);
            }
        }
        boolean pgLike = s.isPgLike();
        s.touch();

        if (s.inTransaction()) {
            s.txnLock().lock();
            try {
                return batchOn(s.pinned(), sql, batch, pgLike);
            } catch (SQLException e) {
                throw sqlError(e);
            } finally {
                s.txnLock().unlock();
            }
        }

        PhysicalPool pool = s.pool();
        PooledConn pc = pool.borrow(false, false);
        try {
            try {
                return batchOn(pc, sql, batch, pgLike);
            } catch (SQLException e) {
                if (isConnectionError(e)) {
                    pool.discard(pc);
                    pc = null;
                }
                throw sqlError(e);
            }
        } finally {
            if (pc != null) pool.releaseSafely(pc);
        }
    }

    private static JsonObject batchOn(PooledConn pc, String sql, JsonArray batch, boolean pgLike) throws SQLException {
        PreparedStatement ps = pc.prepared(sql);
        try {
            for (JsonElement row : batch) {
                bind(ps, row.getAsJsonArray(), pgLike);
                ps.addBatch();
            }
            int[] counts;
            SQLException batchError = null;
            try {
                counts = ps.executeBatch();
            } catch (SQLException e) {
                // keep partial counts from BatchUpdateException, then surface a structured error
                counts = e instanceof BatchUpdateException bue ? bue.getUpdateCounts() : new int[0];
                batchError = e;
            }
            JsonObject out = new JsonObject();
            out.addProperty("statementType", "batch");
            out.add("updateCounts", countsToJson(counts));
            JsonObject keys = readGeneratedKeys(ps);
            out.add("generatedKeys", keys == null ? JsonNull.INSTANCE : keys);
            if (batchError != null) {
                Map<String, Object> extras = ApiException.sqlExtras(batchError);
                extras.put("updateCounts", countsToString(counts));
                throw new ApiException(400, "batch failed: " + batchError.getMessage(), "sql_error", extras);
            }
            return out;
        } finally {
            try {
                ps.clearBatch();
            } catch (SQLException ignored) {
            }
            try {
                ResultSet rs = ps.getResultSet();
                if (rs != null) rs.close();
            } catch (SQLException ignored) {
            }
        }
    }

    private static JsonArray countsToJson(int[] counts) {
        JsonArray arr = new JsonArray();
        for (int c : counts) arr.add(c);
        return arr;
    }

    private static String countsToString(int[] counts) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < counts.length; i++) {
            if (i > 0) sb.append(", ");
            int c = counts[i];
            sb.append(c == Statement.SUCCESS_NO_INFO ? "SUCCESS_NO_INFO"
                    : c == Statement.EXECUTE_FAILED ? "FAILED" : String.valueOf(c));
        }
        return sb.append("]").toString();
    }

    // ---------------------------------------------------------------- transaction

    static JsonObject transaction(JdbcSession s, JsonObject req) {
        String action = getString(req, "action", null);
        if (action == null) {
            throw new ApiException(400, "'action' is required: begin | commit | rollback | setAutoCommit | setIsolation");
        }
        s.touch();
        switch (action.toLowerCase()) {
            case "begin":
                s.beginTxn(parseIsolation(req.get("isolation")));
                break;
            case "commit":
                s.commitTxn();
                break;
            case "rollback":
                s.rollbackTxn();
                break;
            case "setautocommit": {
                JsonElement v = req.get("value");
                if (v == null || v.isJsonNull() || !v.isJsonPrimitive()) {
                    throw new ApiException(400, "setAutoCommit requires boolean 'value'");
                }
                s.setAutoCommitTxn(v.getAsBoolean());
                break;
            }
            case "setisolation": {
                Integer iso = parseIsolation(req.get("value"));
                if (iso == null) throw new ApiException(400, "setIsolation requires 'value' (name or JDBC level)");
                s.changeIsolation(iso);
                break;
            }
            default:
                throw new ApiException(400, "unknown action '" + action
                        + "': begin | commit | rollback | setAutoCommit | setIsolation");
        }
        JsonObject out = new JsonObject();
        out.addProperty("action", action);
        out.addProperty("inTransaction", s.inTransaction());
        out.addProperty("autoCommit", !s.inTransaction());
        Integer iso = s.isolationLevel();
        if (iso == null && s.inTransaction()) {
            try {
                iso = s.pinned().conn.getTransactionIsolation();
            } catch (SQLException ignored) {
            }
        }
        out.add("isolation", iso == null ? JsonNull.INSTANCE : new JsonPrimitive(iso));
        return out;
    }

    // ---------------------------------------------------------------- helpers

    private static void applySettings(Statement stmt, Integer timeout, int maxRows, int fetchSize) {
        setQuietly(() -> stmt.setQueryTimeout(timeout == null ? 0 : Math.max(0, timeout)));
        // one sentinel row beyond the cap so truncation is detectable
        setQuietly(() -> stmt.setMaxRows(maxRows + 1));
        setQuietly(() -> stmt.setFetchSize(Math.max(0, fetchSize)));
    }

    private static JsonObject readGeneratedKeys(Statement stmt) {
        try (ResultSet rs = stmt.getGeneratedKeys()) {
            ResultSetMetaData md = rs.getMetaData();
            JsonArray cols = ValueCodec.describeColumns(md);
            JsonArray rows = new JsonArray();
            while (rs.next()) rows.add(ValueCodec.rowToJson(rs, md));
            if (rows.isEmpty() || cols.size() == 0) return null;
            JsonObject o = new JsonObject();
            o.add("columns", cols);
            o.add("rows", rows);
            o.addProperty("rowCount", rows.size());
            return o;
        } catch (SQLException e) {
            return null; // driver doesn't support generated keys for this statement
        }
    }

    /**
     * Heuristic for dead connections (worth a discard + optional retry) vs
     * ordinary statement errors. Covers SQLState 08xxx and common driver texts.
     * Package-private for unit tests.
     */
    static boolean isConnectionError(SQLException e) {
        String st = e.getSQLState();
        if (st != null && st.startsWith("08")) return true;
        SQLException cause = e;
        for (int i = 0; i < 5 && cause != null; i++) {
            String m = cause.getMessage();
            if (m != null) {
                String ml = m.toLowerCase();
                if (ml.contains("connection is closed") || ml.contains("connection closed")
                        || ml.contains("connection has been closed")
                        || ml.contains("communications link failure") || ml.contains("connection refused")
                        || ml.contains("no operations allowed after connection closed")
                        || ml.contains("an i/o error occurred while sending to the backend")) {
                    return true;
                }
            }
            // drivers like ojdbc mix non-SQLException types into the chain — never assume
            Throwable next = cause.getNextException() != null
                    ? cause.getNextException() : cause.getCause();
            cause = next instanceof SQLException se ? se : null;
        }
        return false;
    }

    private static ApiException sqlError(SQLException e) {
        return new ApiException(400, e.getMessage(), "sql_error", ApiException.sqlExtras(e));
    }

    private static String requireSql(JsonObject req) {
        String sql = getString(req, "sql", null);
        if (sql == null || sql.isBlank()) {
            throw new ApiException(400, "'sql' is required and must be a non-empty string");
        }
        return sql;
    }

    /** Binds one parameter array positionally (1-based JDBC indices). */
    static void bind(PreparedStatement ps, JsonArray params, boolean pgLike) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            bindOne(ps, i + 1, params.get(i), pgLike);
        }
    }

    private static void bindOne(PreparedStatement ps, int idx, JsonElement el, boolean pgLike) throws SQLException {
        if (el == null || el.isJsonNull()) {
            ps.setObject(idx, null);
        } else if (el.isJsonObject()) {
            bindTyped(ps, idx, el.getAsJsonObject(), pgLike);
        } else if (el.isJsonPrimitive()) {
            JsonPrimitive p = el.getAsJsonPrimitive();
            if (p.isBoolean()) {
                ps.setObject(idx, p.getAsBoolean());
            } else if (p.isNumber()) {
                String num = p.getAsString();
                try {
                    ps.setObject(idx, Long.parseLong(num));
                } catch (NumberFormatException e) {
                    ps.setObject(idx, new BigDecimal(num));
                }
            } else {
                ps.setObject(idx, p.getAsString());
            }
        } else {
            // nested arrays/objects are passed as their JSON text
            ps.setObject(idx, el.toString());
        }
    }

    /**
     * Explicit type hints: {"type": "decimal|date|time|timestamp|json|bytes|int|float|bool|text", "value": ...}.
     * These exist because strict dialects (PostgreSQL) reject implicit varchar casts
     * into numeric/date/jsonb columns, and because JSON cannot carry Decimal/bytes natively.
     */
    private static void bindTyped(PreparedStatement ps, int idx, JsonObject o, boolean pgLike) throws SQLException {
        JsonElement tv = o.get("type");
        String type = tv != null && tv.isJsonPrimitive() ? tv.getAsString().toLowerCase() : "";
        JsonElement v = o.get("value");
        if (v == null || v.isJsonNull()) {
            ps.setObject(idx, null);
            return;
        }
        String s = v.isJsonPrimitive() ? v.getAsString() : v.toString();
        try {
            switch (type) {
                case "decimal":
                case "numeric":
                    ps.setObject(idx, new BigDecimal(s));
                    return;
                case "int":
                case "integer":
                case "bigint":
                    try {
                        ps.setObject(idx, Long.parseLong(s));
                    } catch (NumberFormatException e) {
                        ps.setObject(idx, new java.math.BigInteger(s));
                    }
                    return;
                case "float":
                case "double":
                    ps.setObject(idx, Double.parseDouble(s));
                    return;
                case "bool":
                case "boolean":
                    ps.setObject(idx, Boolean.parseBoolean(s));
                    return;
                case "date":
                    ps.setObject(idx, java.sql.Date.valueOf(java.time.LocalDate.parse(s)));
                    return;
                case "time":
                    try {
                        ps.setObject(idx, java.time.LocalTime.parse(s)); // keeps microseconds
                    } catch (SQLException e) {
                        ps.setObject(idx, java.sql.Time.valueOf(java.time.LocalTime.parse(s))); // legacy driver
                    }
                    return;
                case "timestamp":
                    ps.setObject(idx, java.sql.Timestamp.valueOf(java.time.LocalDateTime.parse(s)));
                    return;
                case "timestamptz":
                case "timestamp_tz":
                    ps.setObject(idx, java.time.OffsetDateTime.parse(s));
                    return;
                case "json":
                case "jsonb":
                    // PG-family wants Types.OTHER (unknown) so the server casts to jsonb;
                    // other dialects accept a plain string into JSON columns
                    if (pgLike) {
                        ps.setObject(idx, s, Types.OTHER);
                    } else {
                        ps.setObject(idx, s);
                    }
                    return;
                case "bytes":
                case "blob":
                case "binary":
                    ps.setBytes(idx, java.util.Base64.getDecoder().decode(s));
                    return;
                case "text":
                case "string":
                case "varchar":
                    ps.setObject(idx, s);
                    return;
                case "":
                    bindOne(ps, idx, v, pgLike);
                    return;
                default:
                    throw new ApiException(400, "unknown param type hint '" + type
                            + "' (supported: decimal, int, float, bool, date, time, timestamp, timestamptz, json, bytes, text)");
            }
        } catch (IllegalArgumentException e) {
            throw new ApiException(400, "invalid value '" + s + "' for param type hint '" + type + "': " + e.getMessage());
        }
    }

    /** Package-private for unit tests. */
    static Integer parseIsolation(JsonElement el) {
        if (el == null || el.isJsonNull()) return null;
        if (el.isJsonPrimitive()) {
            JsonPrimitive p = el.getAsJsonPrimitive();
            if (p.isNumber()) return p.getAsInt();
            String name = p.getAsString().trim().toUpperCase();
            Integer lvl = ISOLATIONS.get(name);
            if (lvl == null) {
                throw new ApiException(400, "unknown isolation level '" + name + "'; supported: " + ISOLATIONS.keySet());
            }
            return lvl;
        }
        throw new ApiException(400, "isolation level must be a name or integer");
    }

    private static void setQuietly(SqlAction action) {
        try {
            action.run();
        } catch (SQLException e) {
            // tuning is best-effort; drivers reject fetchSize/queryTimeout in varying ways
        }
    }

    interface SqlAction {
        void run() throws SQLException;
    }

    static String getString(JsonObject o, String key, String def) {
        JsonElement el = o.get(key);
        return el != null && el.isJsonPrimitive() ? el.getAsString() : def;
    }

    static Integer optInt(JsonObject o, String key) {
        JsonElement el = o.get(key);
        return el != null && el.isJsonPrimitive() && el.getAsJsonPrimitive().isNumber() ? el.getAsInt() : null;
    }

    static JsonArray optArray(JsonObject o, String key) {
        JsonElement el = o.get(key);
        return el != null && el.isJsonArray() ? el.getAsJsonArray() : null;
    }
}
