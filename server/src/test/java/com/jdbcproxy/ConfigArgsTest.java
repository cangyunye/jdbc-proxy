package com.jdbcproxy;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigArgsTest {

    @Test
    void defaults() {
        Config c = Config.fromArgs(new String[0], Map.of());
        assertEquals("127.0.0.1", c.host());
        assertEquals(8089, c.port());
        assertEquals(64, c.maxConnections());
        assertEquals(10000, c.maxRows());
        assertEquals(0, c.threads()); // virtual threads by default
        assertEquals(8, c.poolSize());
        assertEquals(2, c.reserveReads());
        assertEquals(30000L, c.borrowWaitMs());
        assertEquals(256, c.maxPhysical());
        assertEquals(10000, c.maxBatch());
        assertNull(c.apiKey());
    }

    @Test
    void flagParsingBothForms() {
        Config c = Config.fromArgs(new String[]{"--port", "9000", "--host=0.0.0.0", "--pool-size", "4"}, Map.of());
        assertEquals(9000, c.port());
        assertEquals("0.0.0.0", c.host());
        assertEquals(4, c.poolSize());
    }

    @Test
    void envFallback() {
        Config c = Config.fromArgs(new String[0], Map.of("JDBC_PROXY_PORT", "7777", "JDBC_PROXY_API_KEY", " k "));
        assertEquals(7777, c.port());
        assertEquals("k", c.apiKey()); // blank-trimmed
    }

    @Test
    void flagWinsOverEnv() {
        Config c = Config.fromArgs(new String[]{"--port", "1"}, Map.of("JDBC_PROXY_PORT", "7777"));
        assertEquals(1, c.port());
    }

    @Test
    void blankApiKeyBecomesNull() {
        Config c = Config.fromArgs(new String[]{"--api-key", "  "}, Map.of());
        assertNull(c.apiKey());
    }

    @Test
    void invalidIntegerRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> Config.fromArgs(new String[]{"--port", "abc"}, Map.of()));
    }

    @Test
    void missingValueRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> Config.fromArgs(new String[]{"--port"}, Map.of()));
    }

    @Test
    void unknownArgRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> Config.fromArgs(new String[]{"nonsense"}, Map.of()));
    }
}
