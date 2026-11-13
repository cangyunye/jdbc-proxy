package com.jdbcproxy;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DialectsTest {

    // ---------------------------------------------------------------- normalizeHint

    @Test
    void knownHintsAccepted() {
        for (String h : new String[]{"mysql", "mariadb", "oceanbase_mysql",
                "postgresql", "pg", "opengauss", "gaussdb", "oceanbase_pg",
                "oracle", "oceanbase_oracle"}) {
            assertEquals(h, Dialects.normalizeHint(h));
        }
    }

    @Test
    void autoAndBlankMeanInfer() {
        assertNull(Dialects.normalizeHint(null));
        assertNull(Dialects.normalizeHint(""));
        assertNull(Dialects.normalizeHint("auto"));
        assertNull(Dialects.normalizeHint("AUTO"));
    }

    @Test
    void caseAndWhitespaceTolerated() {
        assertEquals("oceanbase_mysql", Dialects.normalizeHint(" OceanBase_MySQL "));
    }

    @Test
    void unknownHintRejected() {
        ApiException e = assertThrows(ApiException.class, () -> Dialects.normalizeHint("sqlite"));
        assertEquals(400, e.status);
        assertTrue(e.getMessage().contains("supported"));
    }

    // ---------------------------------------------------------------- family resolution

    @Test
    void mysqlFamily() {
        for (String h : new String[]{"mysql", "mariadb", "oceanbase_mysql"}) {
            assertEquals("mysql", Dialects.familyOf(h));
        }
    }

    @Test
    void postgresqlFamilyIncludesCompatModes() {
        for (String h : new String[]{"postgresql", "pg", "opengauss", "gaussdb", "oceanbase_pg"}) {
            assertEquals("postgresql", Dialects.familyOf(h));
        }
    }

    @Test
    void oracleFamilyIncludesOceanbaseOracleMode() {
        assertEquals("oracle", Dialects.familyOf("oracle"));
        assertEquals("oracle", Dialects.familyOf("oceanbase_oracle"));
    }

    @Test
    void unknownFamilyForUnresolved() {
        assertEquals("unknown", Dialects.familyOf("unknown"));
        assertEquals("unknown", Dialects.familyOf(null));
    }

    // ---------------------------------------------------------------- inference

    @Test
    void inferFromProductName() {
        assertEquals("oracle", Dialects.infer("oracle", "jdbc:x:..."));
        assertEquals("postgresql", Dialects.infer("postgresql", "jdbc:x:..."));
        assertEquals("opengauss", Dialects.infer("opengauss", "jdbc:x:..."));
        assertEquals("mysql", Dialects.infer("MySQL", "jdbc:x:..."));
    }

    @Test
    void inferFromUrlWhenProductUnknown() {
        assertEquals("oracle", Dialects.infer("", "jdbc:oracle:thin:@h:1521/x"));
        assertEquals("postgresql", Dialects.infer("", "jdbc:postgresql://h/db"));
        assertEquals("mysql", Dialects.infer("", "jdbc:mysql://h/db"));
        assertEquals("opengauss", Dialects.infer("", "jdbc:opengauss://h/db"));
        assertEquals("unknown", Dialects.infer("", "jdbc:oceanbase://h/db")); // ambiguous by design
    }

    @Test
    void oceanbaseRequiresExplicitDeclaration() {
        // the whole point of the hint: same URL prefix, two different dialects
        assertEquals("mysql", Dialects.familyOf(Dialects.normalizeHint("oceanbase_mysql")));
        assertEquals("oracle", Dialects.familyOf(Dialects.normalizeHint("oceanbase_oracle")));
    }

    @Test
    void hintSetIsDeclaredInDocs() {
        assertEquals(Set.of("auto", "mysql", "mariadb", "oceanbase_mysql",
                "postgresql", "pg", "opengauss", "gaussdb", "oceanbase_pg",
                "oracle", "oceanbase_oracle"), Dialects.KNOWN_HINTS);
    }
}
