package io.kestra.plugin.databricks.lakebase;

import java.nio.file.Files;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class LakebaseServiceTest {
    private static final String ENDPOINT = "projects/p/branches/b/endpoints/e";
    private static final String CLIENT_ID = "00000000-0000-0000-0000-000000000001";

    @Inject
    private RunContextFactory runContextFactory;

    @AfterEach
    void resetOverride() {
        LakebaseService.resetConnectionOverride();
    }

    @Test
    void prepareMintsTokenAndUsesClientIdAsUsername() throws Exception {
        AtomicInteger mintCalls = new AtomicInteger();
        var client = new LakebaseService.Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                mintCalls.incrementAndGet();
                assertThat(endpoint, is(ENDPOINT));
                return "fresh-token";
            }

            @Override
            public String resolveHost(String endpoint) {
                return "ep-abc.database.cloud.databricks.com";
            }
        };

        var session = LakebaseService.prepare(runContext(query()), query(), client);

        assertThat(mintCalls.get(), is(1));
        assertThat(session.token(), is("fresh-token"));
        assertThat(session.jdbcUrl(), is("jdbc:postgresql://ep-abc.database.cloud.databricks.com:5432/orders_db"));
        assertThat(session.properties().getProperty("user"), is(CLIENT_ID));
        assertThat(session.properties().getProperty("password"), is("fresh-token"));
        assertThat(session.properties().getProperty("ssl"), is("true"));
        assertThat(session.properties().getProperty("sslmode"), is("require"));
    }

    @Test
    void preparePrefersExplicitHostOverEndpointLookup() throws Exception {
        var query = queryBuilder()
            .host(Property.ofValue("explicit.example.com"))
            .port(Property.ofValue(15432))
            .build();

        var session = LakebaseService.prepare(runContext(query), query, new LakebaseService.Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                return "tok";
            }

            @Override
            public String resolveHost(String endpoint) {
                throw new AssertionError("host was provided; endpoint lookup should be skipped");
            }
        });

        assertThat(session.jdbcUrl(), is("jdbc:postgresql://explicit.example.com:15432/orders_db"));
    }

    @Test
    void prepareFailsWhenTokenIsBlank() {
        var query = query();
        var error = assertThrows(IllegalStateException.class, () -> LakebaseService.prepare(runContext(query), query, new LakebaseService.Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                return "  ";
            }

            @Override
            public String resolveHost(String endpoint) {
                return "host";
            }
        })
        );

        assertThat(error.getMessage(), containsString("empty token"));
    }

    @Test
    void prepareFailsWhenHostCannotBeResolved() {
        var query = query();
        var error = assertThrows(IllegalArgumentException.class, () -> LakebaseService.prepare(runContext(query), query, new LakebaseService.Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                return "tok";
            }

            @Override
            public String resolveHost(String endpoint) {
                return null;
            }
        })
        );

        assertThat(error.getMessage(), containsString("Set the host property"));
    }

    @Test
    void applySslWritesPemFilesAndSetsDriverProperties() throws Exception {
        var query = queryBuilder()
            .sslRootCert(Property.ofValue("-----BEGIN CERTIFICATE-----\nROOT\n-----END CERTIFICATE-----"))
            .sslCert(Property.ofValue("-----BEGIN CERTIFICATE-----\nCLIENT\n-----END CERTIFICATE-----"))
            .sslKey(Property.ofValue("-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----"))
            .sslKeyPassword(Property.ofValue("key-pass"))
            .sslMode(Property.ofValue(LakebaseConnectionInterface.SslMode.VERIFY_FULL))
            .build();

        Properties properties = new Properties();
        LakebaseService.applySsl(properties, runContext(query), query);

        assertThat(properties.getProperty("ssl"), is("true"));
        assertThat(properties.getProperty("sslmode"), is("verify-full"));
        assertThat(properties.getProperty("sslpassword"), is("key-pass"));
        assertThat(Files.readString(java.nio.file.Path.of(properties.getProperty("sslrootcert"))), containsString("ROOT"));
        assertThat(Files.readString(java.nio.file.Path.of(properties.getProperty("sslcert"))), containsString("CLIENT"));
        assertThat(Files.readString(java.nio.file.Path.of(properties.getProperty("sslkey"))), containsString("KEY"));
    }

    @Test
    void jdbcUrlUsesHostPortAndDatabase() {
        assertThat(
            LakebaseService.jdbcUrl("db.example.com", 5432, "databricks_postgres"),
            is("jdbc:postgresql://db.example.com:5432/databricks_postgres")
        );
    }

    @Test
    void expandParameterGroupTreatsFlatScalarsAsRowsWhenSqlHasOnePlaceholder() {
        var rows = LakebaseService.expandParameterGroup(List.of("a", "b", "c"), 1);

        assertThat(rows, hasSize(3));
        assertThat(rows.get(0), contains("a"));
        assertThat(rows.get(1), contains("b"));
        assertThat(rows.get(2), contains("c"));
    }

    @Test
    void expandParameterGroupKeepsASingleRowWhenPlaceholderCountMatches() {
        var rows = LakebaseService.expandParameterGroup(List.of("id", "pending"), 2);

        assertThat(rows, hasSize(1));
        assertThat(rows.getFirst(), contains("id", "pending"));
    }

    @Test
    void expandParameterGroupAcceptsAListOfRows() {
        var rows = LakebaseService.expandParameterGroup(
            List.of(List.of("1", "pending"), List.of("2", "done")),
            2
        );

        assertThat(rows, hasSize(2));
        assertThat(rows.get(0), contains("1", "pending"));
        assertThat(rows.get(1), contains("2", "done"));
    }

    @Test
    void placeholderCountIgnoresQuestionMarksInsideQuotes() {
        assertThat(LakebaseService.placeholderCount("INSERT INTO t (a, b) VALUES (?, ?)"), is(2));
        assertThat(LakebaseService.placeholderCount("SELECT * FROM t WHERE name = '?' AND id = ?"), is(1));
        assertThat(LakebaseService.placeholderCount("SELECT \"col?\" FROM t WHERE id = ?"), is(1));
    }

    @Test
    void sessionDoesNotExposeTokenInJdbcUrl() throws Exception {
        var query = query();
        var session = LakebaseService.prepare(runContext(query), query, new LakebaseService.Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                return "super-secret-token";
            }

            @Override
            public String resolveHost(String endpoint) {
                return "host";
            }
        });

        assertThat(session.jdbcUrl(), not(containsString("super-secret-token")));
        assertThat(session.properties().getProperty("password"), is("super-secret-token"));
        assertThat(session.token(), is("super-secret-token"));
        assertThat(session.properties().get("user"), not(nullValue()));
    }

    private Query query() {
        return queryBuilder().build();
    }

    private Query.QueryBuilder<?, ?> queryBuilder() {
        return Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue(CLIENT_ID))
            .clientSecret(Property.ofValue("client-secret"))
            .endpoint(Property.ofValue(ENDPOINT))
            .database(Property.ofValue("orders_db"))
            .sql(Property.ofValue("SELECT 1"));
    }

    private io.kestra.core.runners.RunContext runContext(Query query) {
        return TestsUtils.mockRunContext(runContextFactory, query, ImmutableMap.of());
    }
}
