package com.jdbcproxy;

import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StatementExecutorTest {

    // ---------------------------------------------------------------- isConnectionError

    @Test
    void sqlState08PrefixMeansConnectionError() {
        assertTrue(StatementExecutor.isConnectionError(new SQLException("nope", "08001")));
        assertTrue(StatementExecutor.isConnectionError(new SQLException("nope", "08S01")));
    }

    @Test
    void driverMessageHeuristics() {
        assertTrue(StatementExecutor.isConnectionError(new SQLException("Communications link failure")));
        assertTrue(StatementExecutor.isConnectionError(new SQLException("This connection has been closed")));
        assertTrue(StatementExecutor.isConnectionError(
                new SQLException("No operations allowed after connection closed.", "S1009")));
        assertTrue(StatementExecutor.isConnectionError(
                new SQLException("An I/O error occurred while sending to the backend.", "08006")));
    }

    @Test
    void ordinarySqlErrorsAreNotConnectionErrors() {
        assertFalse(StatementExecutor.isConnectionError(new SQLException("Table 'x' missing", "42S02")));
        assertFalse(StatementExecutor.isConnectionError(new SQLException("Duplicate entry", "23000")));
        assertFalse(StatementExecutor.isConnectionError(new SQLException("syntax error", "42601")));
        assertFalse(StatementExecutor.isConnectionError(new SQLException("timeout cancelled", "57014")));
    }

    @Test
    void chainedCauseIsExamined() {
        SQLException root = new SQLException("communications link failure", "S0001");
        SQLException wrapped = new SQLException("batch execution failed", "S1009", root);
        assertTrue(StatementExecutor.isConnectionError(wrapped));
        SQLException viaNext = new SQLException("outer", "S1009");
        viaNext.setNextException(new SQLException("connection closed", "08003"));
        assertTrue(StatementExecutor.isConnectionError(viaNext));
    }

    @Test
    void nullStateAndMessageAreSafe() {
        assertFalse(StatementExecutor.isConnectionError(new SQLException()));
    }

    // ---------------------------------------------------------------- transaction payload sanity

    @Test
    void isolationNameResolution() {
        // parseIsolation accepts JDBC names (case-insensitive) and integers; unknown names are 400s
        assertEquals(2, StatementExecutor.parseIsolation(new com.google.gson.JsonPrimitive("read_committed")));
        assertEquals(8, StatementExecutor.parseIsolation(new com.google.gson.JsonPrimitive("SERIALIZABLE")));
        assertEquals(4, StatementExecutor.parseIsolation(new com.google.gson.JsonPrimitive(4)));
        ApiException e = assertThrows(ApiException.class,
                () -> StatementExecutor.parseIsolation(new com.google.gson.JsonPrimitive("nope")));
        assertEquals(400, e.status);
    }

    @Test
    void nullIsolationIsNull() {
        org.junit.jupiter.api.Assertions.assertNull(StatementExecutor.parseIsolation(null));
    }

    private static <T extends Throwable> T assertThrows(Class<T> type, ThrowingRunnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) return type.cast(t);
            throw new AssertionError("expected " + type.getSimpleName() + " but got " + t, t);
        }
        throw new AssertionError("expected " + type.getSimpleName() + " but nothing was thrown");
    }

    interface ThrowingRunnable {
        void run() throws Throwable;
    }

    // ---------------------------------------------------------------- error payload shape

    @Test
    void apiExceptionCarriesSqlFields() {
        ApiException e = new ApiException(400, "boom", "sql_error",
                Map.of("sqlState", "42S02", "errorCode", 1146));
        JsonObject err = e.toJson().getAsJsonObject("error");
        assertEquals(400, err.get("status").getAsInt());
        assertEquals("boom", err.get("message").getAsString());
        assertEquals("42S02", err.get("sqlState").getAsString());
        assertEquals(1146, err.get("errorCode").getAsInt());
    }

    @Test
    void sqlExtrasIncludesChainedCauses() {
        SQLException main = new SQLException("main", "S0001", 101);
        main.setNextException(new SQLException("child", "S0002", 102));
        Map<String, Object> m = ApiException.sqlExtras(main);
        assertEquals("S0001", m.get("sqlState"));
        assertEquals(101, m.get("errorCode"));
        assertTrue(((String) m.get("causedBy")).contains("[S0002|102] child"));
    }
}
