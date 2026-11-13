package com.jdbcproxy;

import com.google.gson.JsonPrimitive;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Pure-function coverage for the JDBC -> JSON value codec (no database needed). */
class ValueCodecTest {

    @Test
    void zeroScaleDecimalBecomesNumber() {
        JsonPrimitive e = (JsonPrimitive) ValueCodec.genericToJson(new BigDecimal("42"), false);
        assertTrue(e.isNumber());
        assertEquals(42L, e.getAsLong());
    }

    @Test
    void fractionalDecimalStaysStringToPreservePrecision() {
        // JSON doubles would corrupt 0.10 / money values
        assertEquals(new JsonPrimitive("0.10"),
                ValueCodec.genericToJson(new BigDecimal("0.10"), false));
    }

    @Test
    void highPrecisionDecimalStaysString() {
        String exact = "123456789012345678.9012";
        assertEquals(new JsonPrimitive(exact),
                ValueCodec.genericToJson(new BigDecimal(exact), false));
    }

    @Test
    void bigIntegerBeyond64bitFallsBackToString() {
        String unsignedMax = "18446744073709551615"; // MySQL BIGINT UNSIGNED max
        assertEquals(new JsonPrimitive(unsignedMax),
                ValueCodec.genericToJson(new BigInteger(unsignedMax), false));
    }

    @Test
    void bigIntegerWithin64bitBecomesNumber() {
        assertEquals(new JsonPrimitive(Long.MAX_VALUE),
                ValueCodec.genericToJson(BigInteger.valueOf(Long.MAX_VALUE), false));
    }

    @Test
    void bytesBecomeBase64() {
        byte[] raw = {0, 1, (byte) 0xff, 127};
        assertEquals(new JsonPrimitive(Base64.getEncoder().encodeToString(raw)),
                ValueCodec.genericToJson(raw, false));
    }

    @Test
    void timestampFormatsIsoWithNanos() {
        Timestamp ts = Timestamp.valueOf("2024-01-02 03:04:05.123456");
        assertEquals(new JsonPrimitive("2024-01-02T03:04:05.123456"),
                ValueCodec.genericToJson(ts, false));
    }

    @Test
    void timestampWithoutNanosOmitsFraction() {
        assertEquals(new JsonPrimitive("2024-01-02T03:04:05"),
                ValueCodec.genericToJson(Timestamp.valueOf("2024-01-02 03:04:05"), false));
    }

    @Test
    void uuidBecomesString() {
        UUID u = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        assertEquals(new JsonPrimitive("123e4567-e89b-12d3-a456-426614174000"),
                ValueCodec.genericToJson(u, false));
    }

    @Test
    void floatBecomesNumber() {
        assertEquals(1.5, ((JsonPrimitive) ValueCodec.genericToJson(1.5f, false)).getAsDouble(), 1e-9);
    }

    @Test
    void shortAndByteBecomeNumbers() {
        assertEquals((long) Short.MAX_VALUE, ((JsonPrimitive) ValueCodec.genericToJson(Short.MAX_VALUE, false)).getAsLong());
        assertEquals((long) Byte.MIN_VALUE, ((JsonPrimitive) ValueCodec.genericToJson(Byte.MIN_VALUE, false)).getAsLong());
    }

    @Test
    void wasNullWinsOverValue() {
        assertTrue(ValueCodec.genericToJson("non-null value", true).isJsonNull());
        assertTrue(ValueCodec.genericToJson(null, false).isJsonNull());
    }

    @Test
    void unknownObjectFallsBackToString() {
        Object weird = new Object() {
            @Override
            public String toString() {
                return "weird!";
            }
        };
        assertEquals(new JsonPrimitive("weird!"), ValueCodec.genericToJson(weird, false));
    }

    @Test
    void longTextTruncated() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ValueCodec.MAX_TEXT_CHARS / 10 + 100; i++) sb.append("0123456789");
        JsonPrimitive e = (JsonPrimitive) ValueCodec.genericToJson(sb.toString(), false);
        assertEquals(ValueCodec.MAX_TEXT_CHARS, e.getAsString().length());
    }

    @Test
    void primitiveTypesSurviveRoundTrip() {
        assertFalse(ValueCodec.genericToJson(Boolean.TRUE, false).isJsonNull());
        assertEquals(new JsonPrimitive("plain"), ValueCodec.genericToJson("plain", false));
    }
}
