package com.jdbcproxy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HttpApiTest {

    @Test
    void redactsEmbeddedCredentials() {
        assertEquals("jdbc:pg://***:***@h/db", HttpApi.redactUrl("jdbc:pg://alice:s3cr3t@h/db"));
    }

    @Test
    void redactsPasswordQueryParameter() {
        assertEquals("jdbc:mysql://h/db?user=u&password=***",
                HttpApi.redactUrl("jdbc:mysql://h/db?user=u&password=top-secret"));
    }

    @Test
    void redactionIsCaseInsensitive() {
        assertEquals("jdbc:x://h?PASSWORD=***", HttpApi.redactUrl("jdbc:x://h?PASSWORD=top-secret"));
    }

    @Test
    void cleanUrlPassesThrough() {
        assertEquals("jdbc:mysql://h:3306/db", HttpApi.redactUrl("jdbc:mysql://h:3306/db"));
    }

    @Test
    void passwordInPathStyleUrlRedacted() {
        assertEquals("jdbc:oracle:thin://***:***@h:1521/xepdb1",
                HttpApi.redactUrl("jdbc:oracle:thin://scott:tiger@h:1521/xepdb1"));
    }

    @Test
    void nullSafe() {
        assertNull(HttpApi.redactUrl(null));
    }
}
