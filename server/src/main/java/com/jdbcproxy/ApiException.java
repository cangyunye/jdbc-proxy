package com.jdbcproxy;

import com.google.gson.JsonObject;

import java.util.LinkedHashMap;
import java.util.Map;

/** Exception carrying an HTTP status plus structured error payload. */
final class ApiException extends RuntimeException {
    final int status;
    final String code;
    final Map<String, Object> extras;

    ApiException(int status, String message) {
        this(status, message, "api_error", Map.of());
    }

    ApiException(int status, String message, String code, Map<String, Object> extras) {
        super(message);
        this.status = status;
        this.code = code;
        this.extras = extras;
    }

    JsonObject toJson() {
        JsonObject err = new JsonObject();
        err.addProperty("type", code);
        err.addProperty("message", getMessage());
        err.addProperty("status", status);
        for (Map.Entry<String, Object> e : extras.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Integer i) err.addProperty(e.getKey(), i);
            else if (v instanceof Long l) err.addProperty(e.getKey(), l);
            else if (v instanceof String s) err.addProperty(e.getKey(), s);
            else if (v == null) err.add(e.getKey(), com.google.gson.JsonNull.INSTANCE);
            else err.addProperty(e.getKey(), String.valueOf(v));
        }
        JsonObject out = new JsonObject();
        out.add("error", err);
        return out;
    }

    /** Structured payload for a SQLException (message + sqlState + vendor code + chained causes). */
    static Map<String, Object> sqlExtras(java.sql.SQLException e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("sqlState", e.getSQLState());
        m.put("errorCode", e.getErrorCode());
        StringBuilder chain = new StringBuilder();
        int i = 0;
        for (java.sql.SQLException c = e.getNextException(); c != null && i < 10; c = c.getNextException(), i++) {
            if (i > 0) chain.append("\n");
            chain.append(String.format("[%s|%d] %s", c.getSQLState(), c.getErrorCode(), c.getMessage()));
        }
        if (i > 0) m.put("causedBy", chain.toString());
        return m;
    }
}
