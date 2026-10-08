package io.kestra.plugin.databricks.lakebase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
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

    private Query query(boolean ssl, LakebaseConnectionInterface.SslMode sslMode) {
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
            .sslMode(Property.ofValue(sslMode))
            .sql(Property.ofValue("SELECT 1"))
            .build();
    }

    private io.kestra.core.runners.RunContext runContext(Query task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }
}
