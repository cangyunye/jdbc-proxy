package com.jdbcproxy;

import com.google.gson.JsonArray;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;

/**
 * Dialect introspection plus the standard DatabaseMetaData queries
 * (tables/columns/keys/indexes/...), each returned in the common
 * {@code {columns, rows, rowCount, truncated}} shape.
 *
 * <p>Metadata calls borrow a read connection (never the transaction-pinned one
 * unless a transaction is active), and honor an optional {@code max} query
 * parameter capped by the server's {@code --max-rows}.</p>
 */
final class MetadataService {
    private final Config cfg;

    MetadataService(Config cfg) {
        this.cfg = cfg;
    }

    interface ConnFn<T> {
        T apply(PooledConn pc) throws SQLException;
    }

    private <T> T withConn(JdbcSession s, ConnFn<T> fn) throws SQLException {
        if (s.inTransaction()) {
            s.txnLock().lock();
            try {
                return fn.apply(s.pinned());
            } finally {
                s.txnLock().unlock();
            }
        }
        PooledConn pc = s.pool().borrow(true, false);
        try {
            return fn.apply(pc);
        } finally {
            s.pool().releaseSafely(pc);
        }
    }

    // ---------------------------------------------------------------- dialect

    JsonObject dialect(JdbcSession s) throws SQLException {
        JsonObject cached = s.dialectCache();
        if (cached != null) return cached;
        JsonObject d = withConn(s, pc -> buildDialect(pc.conn.getMetaData(), s));
        s.setDialectCache(d);
        return d;
    }

    private JsonObject buildDialect(DatabaseMetaData md, JdbcSession s) {
        JsonObject d = new JsonObject();
        // resolved dialect family + the client's declaration, for compat-mode
        // databases (OceanBase/OpenGauss) whose product name is ambiguous
        d.addProperty("dialect", s.dialectFamily());
        d.addProperty("dialectHint", s.dialectHint() == null ? "auto" : s.dialectHint());

        putStr(d, "productName", md::getDatabaseProductName);
        putStr(d, "productVersion", md::getDatabaseProductVersion);
        putInt(d, "databaseMajorVersion", md::getDatabaseMajorVersion);
        putInt(d, "databaseMinorVersion", md::getDatabaseMinorVersion);
        putStr(d, "driverName", md::getDriverName);
        putStr(d, "driverVersion", md::getDriverVersion);
        putInt(d, "driverMajorVersion", md::getDriverMajorVersion);
        putInt(d, "driverMinorVersion", md::getDriverMinorVersion);
        putStr(d, "jdbcUrl", md::getURL);
        putStr(d, "userName", md::getUserName);

        putStr(d, "catalogTerm", md::getCatalogTerm);
        putStr(d, "schemaTerm", md::getSchemaTerm);
        putStr(d, "procedureTerm", md::getProcedureTerm);
        putStr(d, "catalogSeparator", md::getCatalogSeparator);
        putStr(d, "identifierQuoteString", md::getIdentifierQuoteString);
        putStr(d, "searchStringEscape", md::getSearchStringEscape);
        putStr(d, "extraNameCharacters", md::getExtraNameCharacters);
        putStr(d, "sqlKeywords", md::getSQLKeywords);
        putStr(d, "numericFunctions", md::getNumericFunctions);
        putStr(d, "stringFunctions", md::getStringFunctions);
        putStr(d, "systemFunctions", md::getSystemFunctions);

        putBool(d, "storesUpperCaseIdentifiers", md::storesUpperCaseIdentifiers);
        putBool(d, "storesLowerCaseIdentifiers", md::storesLowerCaseIdentifiers);
        putBool(d, "storesMixedCaseIdentifiers", md::storesMixedCaseIdentifiers);
        putBool(d, "storesUpperCaseQuotedIdentifiers", md::storesUpperCaseQuotedIdentifiers);
        putBool(d, "storesLowerCaseQuotedIdentifiers", md::storesLowerCaseQuotedIdentifiers);
        putBool(d, "storesMixedCaseQuotedIdentifiers", md::storesMixedCaseQuotedIdentifiers);

        putBool(d, "supportsTransactions", md::supportsTransactions);
        putBool(d, "supportsBatchUpdates", md::supportsBatchUpdates);
        putBool(d, "supportsSavepoints", md::supportsSavepoints);
        putBool(d, "supportsStoredProcedures", md::supportsStoredProcedures);
        putBool(d, "supportsSelectForUpdate", md::supportsSelectForUpdate);
        putBool(d, "supportsGetGeneratedKeys", md::supportsGetGeneratedKeys);
        putBool(d, "supportsSchemasInDataManipulation", md::supportsSchemasInDataManipulation);
        putBool(d, "supportsCatalogsInDataManipulation", md::supportsCatalogsInDataManipulation);
        putBool(d, "nullsAreSortedHigh", md::nullsAreSortedHigh);

        putInt(d, "defaultTransactionIsolation", md::getDefaultTransactionIsolation);
        putInt(d, "maxColumnNameLength", md::getMaxColumnNameLength);
        putInt(d, "maxTableNameLength", md::getMaxTableNameLength);
        putInt(d, "maxSchemaNameLength", md::getMaxSchemaNameLength);
        putInt(d, "maxCatalogNameLength", md::getMaxCatalogNameLength);
        putInt(d, "maxConnections", md::getMaxConnections);

        return d;
    }

    // ---------------------------------------------------------------- metadata queries

    JsonObject tables(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, md -> md.getTables(
                q.get("catalog"), q.get("schemaPattern"), q.get("tablePattern"),
                q.get("types") == null ? null : q.get("types").split(",")));
    }

    JsonObject columns(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, md -> md.getColumns(
                q.get("catalog"), q.get("schemaPattern"), q.get("tablePattern"), q.get("columnPattern")));
    }

    JsonObject primaryKeys(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, md -> md.getPrimaryKeys(
                q.get("catalog"), q.get("schema"), q.get("table")));
    }

    JsonObject foreignKeys(JdbcSession s, Map<String, String> q) throws SQLException {
        boolean exported = "exported".equalsIgnoreCase(q.getOrDefault("direction", "imported"));
        return read(s, q, md -> exported
                ? md.getExportedKeys(q.get("catalog"), q.get("schema"), q.get("table"))
                : md.getImportedKeys(q.get("catalog"), q.get("schema"), q.get("table")));
    }

    JsonObject indexes(JdbcSession s, Map<String, String> q) throws SQLException {
        boolean unique = Boolean.parseBoolean(q.getOrDefault("unique", "false"));
        boolean approximate = !Boolean.parseBoolean(q.getOrDefault("approximate", "true"));
        return read(s, q, md -> md.getIndexInfo(
                q.get("catalog"), q.get("schema"), q.get("table"), unique, approximate));
    }

    JsonObject schemas(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, DatabaseMetaData::getSchemas);
    }

    JsonObject catalogs(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, DatabaseMetaData::getCatalogs);
    }

    JsonObject tableTypes(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, DatabaseMetaData::getTableTypes);
    }

    JsonObject typeInfo(JdbcSession s, Map<String, String> q) throws SQLException {
        return read(s, q, DatabaseMetaData::getTypeInfo);
    }

    // ---------------------------------------------------------------- plumbing

    interface RsCall {
        ResultSet open(DatabaseMetaData md) throws SQLException;
    }

    private int rowCap(Map<String, String> q) {
        Integer m = null;
        try {
            if (q.get("max") != null) m = Integer.parseInt(q.get("max"));
        } catch (NumberFormatException ignored) {
        }
        if (m == null || m <= 0) return cfg.maxRows();
        return Math.min(m, cfg.maxRows());
    }

    private JsonObject read(JdbcSession s, Map<String, String> q, RsCall call) throws SQLException {
        s.touch();
        int cap = rowCap(q);
        return withConn(s, pc -> {
            try (ResultSet rs = call.open(pc.conn.getMetaData())) {
                ResultSetMetaData md = rs.getMetaData();
                JsonArray cols = ValueCodec.describeColumns(md);
                JsonArray rows = new JsonArray();
                boolean truncated = false;
                while (true) {
                    if (rows.size() >= cap) {
                        truncated = rs.next();
                        break;
                    }
                    if (!rs.next()) break;
                    rows.add(ValueCodec.rowToJson(rs, md));
                }
                JsonObject out = new JsonObject();
                out.add("columns", cols);
                out.add("rows", rows);
                out.addProperty("rowCount", rows.size());
                out.addProperty("truncated", truncated);
                return out;
            }
        });
    }

    // ---------------------------------------------------------------- guarded putters

    interface ThrowingStr {
        String get() throws SQLException;
    }

    interface ThrowingInt {
        int get() throws SQLException;
    }

    interface ThrowingBool {
        boolean get() throws SQLException;
    }

    private static void putStr(JsonObject o, String key, ThrowingStr fn) {
        try {
            String v = fn.get();
            o.add(key, v == null ? JsonNull.INSTANCE : new com.google.gson.JsonPrimitive(v));
        } catch (Throwable e) {
            o.add(key, JsonNull.INSTANCE);
        }
    }

    private static void putInt(JsonObject o, String key, ThrowingInt fn) {
        try {
            o.addProperty(key, fn.get());
        } catch (Throwable e) {
            o.add(key, JsonNull.INSTANCE);
        }
    }

    private static void putBool(JsonObject o, String key, ThrowingBool fn) {
        try {
            o.addProperty(key, fn.get());
        } catch (Throwable e) {
            o.add(key, JsonNull.INSTANCE);
        }
    }
}
