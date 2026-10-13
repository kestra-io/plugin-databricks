package io.kestra.plugin.databricks.lakebase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
@Testcontainers(disabledWithoutDocker = true)
class LakebasePostgresIT {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withPassword("lakebase-secret-token");

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void prepareConnectsWithThePostgresDriverWhenSslIsDisabled() throws Exception {
        var task = query(false, LakebaseConnectionInterface.SslMode.DISABLE);
        var session = LakebaseService.prepare(runContext(task), task, stubClient());

        try (
            Connection connection = session.connect();
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT 1")
        ) {
            assertThat(rs.next(), is(true));
            assertThat(rs.getInt(1), is(1));
            assertThat(connection.getMetaData().getDatabaseProductName(), is("PostgreSQL"));
            assertThat(connection.getMetaData().getDriverName().toLowerCase(), containsString("postgresql"));
        }
        assertThat(session.toString(), containsString("jdbc:postgresql://"));
        assertThat(session.toString().contains("lakebase-secret-token"), is(false));
    }

    @Test
    void defaultSslModeIsHonoredByThePostgresDriver() throws Exception {
        var task = query(true, LakebaseConnectionInterface.SslMode.REQUIRE);
        var session = LakebaseService.prepare(runContext(task), task, stubClient());

        assertThat(session.properties().getProperty("sslmode"), is("require"));
        assertThrows(SQLException.class, session::connect);
    }

    @Test
    void queryStoreRoundTripsTimestampsThroughBatchFrom() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS ts_src");
            statement.execute("DROP TABLE IF EXISTS ts_dst");
            statement.execute("""
                CREATE TABLE ts_src (
                    id INT PRIMARY KEY,
                    updated_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
                """);
            statement.execute("""
                CREATE TABLE ts_dst (
                    id INT PRIMARY KEY,
                    updated_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMP NOT NULL
                )
                """);
            statement.execute("""
                INSERT INTO ts_src (id, updated_at, created_at) VALUES
                (1, TIMESTAMPTZ '2026-10-09 11:55:30.300157+00', TIMESTAMP '2026-10-09 11:55:30.300157')
                """);
        }

        LakebaseService.overrideConnection((ctx, conn) -> open());
        try {
            var query = queryBuilder(false, LakebaseConnectionInterface.SslMode.DISABLE)
                .sql(Property.ofValue("SELECT id, updated_at, created_at FROM ts_src ORDER BY id"))
                .fetchType(Property.ofValue(FetchType.STORE))
                .build();
            var stored = query.run(runContext(query));

            var batch = batchBuilder()
                .sql(Property.ofValue("INSERT INTO ts_dst (id, updated_at, created_at) VALUES (?, ?, ?)"))
                .from(Property.ofValue(stored.getUri().toString()))
                .build();
            var output = batch.run(runContext(batch));

            assertThat(output.getRowCount(), is(1L));
            try (
                Connection connection = open();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT id, updated_at, created_at FROM ts_dst")
            ) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt("id"), is(1));
                assertThat(rs.getTimestamp("updated_at").toInstant(), is(Instant.parse("2026-10-09T11:55:30.300157Z")));
                assertThat(rs.getTimestamp("created_at").toLocalDateTime(), is(LocalDateTime.parse("2026-10-09T11:55:30.300157")));
                assertThat(rs.next(), is(false));
            }
        } finally {
            LakebaseService.resetConnectionOverride();
        }
    }

    @Test
    void bindsZonedDateTimeAndInstantParameters() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS ts_params");
            statement.execute("CREATE TABLE ts_params (id INT PRIMARY KEY, updated_at TIMESTAMPTZ NOT NULL)");
        }

        LakebaseService.overrideConnection((ctx, conn) -> open());
        try {
            var batch = batchBuilder()
                .sql(Property.ofValue("INSERT INTO ts_params (id, updated_at) VALUES (?, ?)"))
                .parameterGroups(
                    Property.ofValue(
                        List.of(
                            Batch.ParameterGroup.builder()
                                .parameters(List.of(1, ZonedDateTime.parse("2026-10-09T17:25:30.300157+05:30")))
                                .build(),
                            Batch.ParameterGroup.builder()
                                .parameters(List.of(2, Instant.parse("2024-05-01T00:00:00Z")))
                                .build()
                        )
                    )
                )
                .build();
            var output = batch.run(runContext(batch));

            assertThat(output.getRowCount(), is(2L));
            try (
                Connection connection = open();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT id, updated_at FROM ts_params ORDER BY id")
            ) {
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt("id"), is(1));
                assertThat(rs.getTimestamp("updated_at").toInstant(), is(Instant.parse("2026-10-09T11:55:30.300157Z")));
                assertThat(rs.next(), is(true));
                assertThat(rs.getInt("id"), is(2));
                assertThat(rs.getTimestamp("updated_at").toInstant(), is(Instant.parse("2024-05-01T00:00:00Z")));
            }
        } finally {
            LakebaseService.resetConnectionOverride();
        }
    }

    private LakebaseService.Client stubClient() {
        return new LakebaseService.Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                return POSTGRES.getPassword();
            }

            @Override
            public String resolveHost(String endpoint) {
                throw new AssertionError("host is set; endpoint lookup must not run");
            }
        };
    }

    private Connection open() throws SQLException {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private Query query(boolean ssl, LakebaseConnectionInterface.SslMode sslMode) {
        return queryBuilder(ssl, sslMode)
            .sql(Property.ofValue("SELECT 1"))
            .build();
    }

    private Query.QueryBuilder<?, ?> queryBuilder(boolean ssl, LakebaseConnectionInterface.SslMode sslMode) {
        return Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue(POSTGRES.getUsername()))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .host(Property.ofValue(POSTGRES.getHost()))
            .port(Property.ofValue(POSTGRES.getMappedPort(5432)))
            .database(Property.ofValue(POSTGRES.getDatabaseName()))
            .ssl(Property.ofValue(ssl))
            .sslMode(Property.ofValue(sslMode));
    }

    private Batch.BatchBuilder<?, ?> batchBuilder() {
        return Batch.builder()
            .id(IdUtils.create())
            .type(Batch.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue(POSTGRES.getUsername()))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .host(Property.ofValue(POSTGRES.getHost()))
            .port(Property.ofValue(POSTGRES.getMappedPort(5432)))
            .database(Property.ofValue(POSTGRES.getDatabaseName()))
            .ssl(Property.ofValue(false))
            .sslMode(Property.ofValue(LakebaseConnectionInterface.SslMode.DISABLE));
    }

    private io.kestra.core.runners.RunContext runContext(Task task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }
}
