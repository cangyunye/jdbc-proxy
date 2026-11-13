package com.jdbcproxy;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.UUID;

/**
 * Converts JDBC result sets to JSON.
 *
 * <p>Fidelity rules:
 * <ul>
 *   <li>Integral types (TINYINT..BIGINT) &rarr; JSON number; BIGINT values beyond 64-bit
 *       (e.g. MySQL {@code BIGINT UNSIGNED}) fall back to string.</li>
 *   <li>DECIMAL/NUMERIC &rarr; string via {@code toPlainString()} to preserve exact
 *       precision (JSON doubles would corrupt money values).</li>
 *   <li>DATE/TIME/TIMESTAMP &rarr; ISO-8601 string; time-zone-aware timestamps use
 *       {@link OffsetDateTime} when the driver supports it.</li>
 *   <li>Binary/BLOB &rarr; base64 string. CLOB &rarr; string (truncated at {@link #MAX_TEXT_CHARS}).</li>
 * </ul>
 */
final class ValueCodec {
    static final int MAX_TEXT_CHARS = 4 * 1024 * 1024;

    private static final DateTimeFormatter TS_FMT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
    private static final DateTimeFormatter TS_TZ_FMT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private ValueCodec() {
    }

    interface MetaStr {
        String get(int i) throws SQLException;
    }

    interface MetaInt {
        int get(int i) throws SQLException;
    }

    interface MetaBool {
        boolean get(int i) throws SQLException;
    }

    static JsonArray describeColumns(ResultSetMetaData md) throws SQLException {
        JsonArray arr = new JsonArray();
        int n = md.getColumnCount();
        for (int i = 1; i <= n; i++) {
            JsonObject c = new JsonObject();
            addStr(c, "label", md::getColumnLabel, i);
            addStr(c, "name", md::getColumnName, i);
            addInt(c, "sqlType", md::getColumnType, i);
            addStr(c, "typeName", md::getColumnTypeName, i);
            addInt(c, "precision", md::getPrecision, i);
            addInt(c, "scale", md::getScale, i);
            addInt(c, "displaySize", md::getColumnDisplaySize, i);
            addInt(c, "nullable", md::isNullable, i);
            addBool(c, "autoIncrement", md::isAutoIncrement, i);
            addBool(c, "signed", md::isSigned, i);
            addStr(c, "catalog", md::getCatalogName, i);
            addStr(c, "schema", md::getSchemaName, i);
            addStr(c, "table", md::getTableName, i);
            arr.add(c);
        }
        return arr;
    }

    static JsonArray rowToJson(ResultSet rs, ResultSetMetaData md) throws SQLException {
        JsonArray row = new JsonArray();
        int n = md.getColumnCount();
        for (int i = 1; i <= n; i++) {
            row.add(cellToJson(rs, md, i));
        }
        return row;
    }

    static JsonElement cellToJson(ResultSet rs, ResultSetMetaData md, int i) throws SQLException {
        int type = md.getColumnType(i);
        String typeName = md.getColumnTypeName(i);
        try {
            switch (type) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER: {
                    long v = rs.getLong(i);
                    return rs.wasNull() ? JsonNull.INSTANCE : new JsonPrimitive(v);
                }
                case Types.BIGINT: {
                    BigDecimal v = rs.getBigDecimal(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    try {
                        return new JsonPrimitive(v.longValueExact());
                    } catch (ArithmeticException e) {
                        return new JsonPrimitive(v.toPlainString());
                    }
                }
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE: {
                    double v = rs.getDouble(i);
                    return rs.wasNull() ? JsonNull.INSTANCE : new JsonPrimitive(v);
                }
                case Types.DECIMAL:
                case Types.NUMERIC: {
                    BigDecimal v = rs.getBigDecimal(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    return new JsonPrimitive(v.toPlainString());
                }
                case Types.BIT:
                case Types.BOOLEAN: {
                    if (type == Types.BIT && md.getPrecision(i) > 1) {
                        // MySQL BIT(n>1): read as bytes
                        byte[] v = rs.getBytes(i);
                        if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                        return new JsonPrimitive(Base64.getEncoder().encodeToString(v));
                    }
                    boolean v = rs.getBoolean(i);
                    return rs.wasNull() ? JsonNull.INSTANCE : new JsonPrimitive(v);
                }
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.NCHAR:
                case Types.NVARCHAR:
                case Types.LONGNVARCHAR: {
                    String v = rs.getString(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    return new JsonPrimitive(truncate(v));
                }
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB: {
                    byte[] v = rs.getBytes(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    return new JsonPrimitive(Base64.getEncoder().encodeToString(v));
                }
                case Types.CLOB:
                case Types.NCLOB: {
                    String v = rs.getString(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    return new JsonPrimitive(truncate(v));
                }
                case Types.DATE: {
                    Date v = rs.getDate(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    return new JsonPrimitive(v.toLocalDate().toString());
                }
                case Types.TIME:
                case Types.TIME_WITH_TIMEZONE: {
                    // driver-native LocalTime keeps microseconds; java.sql.Time is millis-only
                    try {
                        LocalTime v = rs.getObject(i, LocalTime.class);
                        return rs.wasNull() || v == null ? JsonNull.INSTANCE : new JsonPrimitive(v.toString());
                    } catch (Throwable ignored) {
                        // driver without JDBC 4.2 conversion: fall back to java.sql.Time
                    }
                    Time v = rs.getTime(i);
                    if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
                    return new JsonPrimitive(v.toLocalTime().toString());
                }
                case Types.TIMESTAMP:
                case Types.TIMESTAMP_WITH_TIMEZONE:
                case -101: // oracle.jdbc.OracleTypes.TIMESTAMPTZ (ojdbc private code)
                case -102: // oracle.jdbc.OracleTypes.TIMESTAMPLTZ
                    return timestampJson(rs, i, typeName);
                default:
                    return genericToJson(rs.getObject(i), rs.wasNull());
            }
        } catch (SQLException primary) {
            // getter specialized for the declared SQL type failed; try a generic read instead
            try {
                return genericToJson(rs.getObject(i), rs.wasNull());
            } catch (SQLException secondary) {
                throw primary;
            }
        }
    }

    private static JsonElement timestampJson(ResultSet rs, int i, String typeName) throws SQLException {
        String tn = typeName == null ? "" : typeName.toLowerCase();
        boolean withTz = tn.contains("timestamptz") || tn.contains("with time zone") || tn.contains("tz");
        if (withTz) {
            try {
                OffsetDateTime v = rs.getObject(i, OffsetDateTime.class);
                return v == null ? JsonNull.INSTANCE : new JsonPrimitive(TS_TZ_FMT.format(v));
            } catch (Throwable ignored) {
                // driver may not offer getObject(x, OffsetDateTime.class)
            }
        }
        Timestamp v = rs.getTimestamp(i);
        if (rs.wasNull() || v == null) return JsonNull.INSTANCE;
        LocalDateTime dt = v.toLocalDateTime();
        return new JsonPrimitive(TS_FMT.format(dt));
    }

    static JsonElement genericToJson(Object v, boolean wasNull) {
        if (v == null || wasNull) return JsonNull.INSTANCE;
        if (v instanceof Boolean b) return new JsonPrimitive(b);
        if (v instanceof Byte || v instanceof Short || v instanceof Integer || v instanceof Long) {
            return new JsonPrimitive(((Number) v).longValue());
        }
        if (v instanceof BigInteger bi) {
            return bi.bitLength() <= 63 ? new JsonPrimitive(bi.longValue()) : new JsonPrimitive(bi.toString());
        }
        if (v instanceof BigDecimal bd) {
            try {
                if (bd.scale() == 0) return new JsonPrimitive(bd.longValueExact());
            } catch (ArithmeticException ignored) {
            }
            return new JsonPrimitive(bd.toPlainString());
        }
        if (v instanceof Float || v instanceof Double) return new JsonPrimitive(((Number) v).doubleValue());
        if (v instanceof byte[] bytes) return new JsonPrimitive(Base64.getEncoder().encodeToString(bytes));
        if (v instanceof Timestamp ts) return new JsonPrimitive(TS_FMT.format(ts.toLocalDateTime()));
        if (v instanceof Date d) return new JsonPrimitive(d.toLocalDate().toString());
        if (v instanceof Time t) return new JsonPrimitive(t.toLocalTime().toString());
        if (v instanceof OffsetDateTime o) return new JsonPrimitive(TS_TZ_FMT.format(o));
        if (v instanceof LocalDateTime ldt) return new JsonPrimitive(TS_FMT.format(ldt));
        if (v instanceof java.time.LocalDate ld) return new JsonPrimitive(ld.toString());
        if (v instanceof LocalTime lt) return new JsonPrimitive(lt.toString());
        if (v instanceof UUID u) return new JsonPrimitive(u.toString());
        if (v instanceof Clob c) return new JsonPrimitive(readClob(c));
        if (v instanceof Blob b) {
            try {
                int len = (int) Math.min(b.length(), MAX_TEXT_CHARS);
                return new JsonPrimitive(Base64.getEncoder().encodeToString(b.getBytes(1, len)));
            } catch (SQLException e) {
                return new JsonPrimitive(String.valueOf(v));
            }
        }
        if (v instanceof SQLXML xml) {
            try {
                return new JsonPrimitive(truncate(xml.getString()));
            } catch (SQLException e) {
                return new JsonPrimitive(String.valueOf(v));
            }
        }
        if (v instanceof RowId rid) {
            // keep the raw ROWID text (Oracle: 18-char base64-style id) so clients
            // can use it directly: SELECT ... WHERE rowid = ?
            byte[] b = rid.getBytes();
            return new JsonPrimitive(new String(b, java.nio.charset.StandardCharsets.ISO_8859_1));
        }
        return new JsonPrimitive(truncate(String.valueOf(v)));
    }

    private static String readClob(Clob c) {
        try {
            long len = Math.min(c.length(), MAX_TEXT_CHARS);
            return c.getSubString(1, (int) len);
        } catch (Exception e) {
            try {
                return c.getSubString(1, MAX_TEXT_CHARS);
            } catch (SQLException e2) {
                return String.valueOf(c);
            }
        }
    }

    private static String truncate(String s) {
        return s.length() > MAX_TEXT_CHARS ? s.substring(0, MAX_TEXT_CHARS) : s;
    }

    private static void addStr(JsonObject o, String key, MetaStr fn, int i) {
        try {
            o.addProperty(key, fn.get(i));
        } catch (SQLException e) {
            o.add(key, JsonNull.INSTANCE);
        }
    }

    private static void addInt(JsonObject o, String key, MetaInt fn, int i) {
        try {
            o.addProperty(key, fn.get(i));
        } catch (SQLException e) {
            o.add(key, JsonNull.INSTANCE);
        }
    }

    private static void addBool(JsonObject o, String key, MetaBool fn, int i) {
        try {
            o.addProperty(key, fn.get(i));
        } catch (SQLException e) {
            o.add(key, JsonNull.INSTANCE);
        }
    }
}
