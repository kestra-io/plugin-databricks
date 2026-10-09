package io.kestra.plugin.databricks.lakebase;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;

import org.postgresql.Driver;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ConfigLoader;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.postgres.Endpoint;
import com.databricks.sdk.service.postgres.GenerateDatabaseCredentialRequest;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;

/**
 * Opens a Lakebase JDBC connection by minting a fresh OAuth database credential per attempt.
 *
 * <p>
 * Each Kestra task run opens its own connection for the duration of the run, so a new token
 * is generated every time rather than pooling a long-lived credential (tokens expire after ~60
 * minutes).
 */
public final class LakebaseService {
    static final int DEFAULT_PORT = 5432;

    private static final ThreadLocal<ConnectionFactory> CONNECTION_OVERRIDE = new ThreadLocal<>();

    private LakebaseService() {
    }

    @FunctionalInterface
    public interface ConnectionFactory {
        Connection open(RunContext runContext, LakebaseConnectionInterface conn) throws Exception;
    }

    /**
     * Databricks-backed operations needed to prepare a JDBC session. Extracted so unit tests can
     * mint tokens and resolve hosts without calling the live API.
     */
    public interface Client {
        String generateDatabaseCredential(String endpoint);

        String resolveHost(String endpoint);
    }

    public record Session(String jdbcUrl, Properties properties) {
        public Connection connect() throws SQLException {
            registerDriver();
            return DriverManager.getConnection(jdbcUrl, properties);
        }

        @Override
        public String toString() {
            return "Session[jdbcUrl=" + jdbcUrl + "]";
        }
    }

    static void overrideConnection(ConnectionFactory factory) {
        CONNECTION_OVERRIDE.set(factory);
    }

    static void resetConnectionOverride() {
        CONNECTION_OVERRIDE.remove();
    }

    public static Connection connect(RunContext runContext, LakebaseConnectionInterface conn) throws Exception {
        var override = CONNECTION_OVERRIDE.get();
        if (override != null) {
            return override.open(runContext, conn);
        }
        return prepare(runContext, conn, defaultClient(runContext, conn)).connect();
    }

    public static Session prepare(RunContext runContext, LakebaseConnectionInterface conn, Client client) throws Exception {
        Objects.requireNonNull(client, "client");

        String endpoint = renderRequired(runContext, conn.getEndpoint(), "endpoint");
        String database = renderRequired(runContext, conn.getDatabase(), "database");
        String clientId = renderRequired(runContext, conn.getClientId(), "clientId");
        int port = runContext.render(conn.getPort()).as(Integer.class).orElse(DEFAULT_PORT);

        String token = client.generateDatabaseCredential(endpoint);
        if (token == null || token.isBlank()) {
            throw new IllegalStateException("Databricks generateDatabaseCredential returned an empty token for endpoint '" + endpoint + "'");
        }

        String host = runContext.render(conn.getHost()).as(String.class)
            .filter(value -> !value.isBlank())
            .orElseGet(() -> client.resolveHost(endpoint));
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(
                "Unable to resolve Lakebase Postgres host from endpoint '" + endpoint + "'. Set the host property."
            );
        }

        String jdbcUrl = jdbcUrl(host, port, database);
        Properties properties = connectionProperties(runContext, conn, clientId, token);

        runContext.logger().debug("Opening Lakebase JDBC connection to {} as user {}", jdbcUrl, clientId);

        return new Session(jdbcUrl, properties);
    }

    public static Client defaultClient(RunContext runContext, LakebaseConnectionInterface conn) throws IllegalVariableEvaluationException {
        WorkspaceClient workspaceClient = workspaceClient(runContext, conn);
        return new Client() {
            @Override
            public String generateDatabaseCredential(String endpoint) {
                return workspaceClient.postgres()
                    .generateDatabaseCredential(new GenerateDatabaseCredentialRequest().setEndpoint(endpoint))
                    .getToken();
            }

            @Override
            public String resolveHost(String endpoint) {
                Endpoint resolved = workspaceClient.postgres().getEndpoint(endpoint);
                if (resolved == null || resolved.getStatus() == null || resolved.getStatus().getHosts() == null) {
                    return null;
                }
                return resolved.getStatus().getHosts().getHost();
            }
        };
    }

    static WorkspaceClient workspaceClient(RunContext runContext, LakebaseConnectionInterface conn) throws IllegalVariableEvaluationException {
        DatabricksConfig cfg = new DatabricksConfig()
            .setHost(renderRequired(runContext, conn.getWorkspaceHost(), "workspaceHost"))
            .setClientId(renderRequired(runContext, conn.getClientId(), "clientId"))
            .setClientSecret(renderRequired(runContext, conn.getClientSecret(), "clientSecret"));

        ConfigLoader.resolve(cfg);
        return new WorkspaceClient(cfg);
    }

    static String jdbcUrl(String host, int port, String database) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }

    static Properties connectionProperties(
        RunContext runContext,
        LakebaseConnectionInterface conn,
        String username,
        String token) throws Exception {
        Properties properties = new Properties();
        properties.put("user", username);
        properties.put("password", token);
        applySsl(properties, runContext, conn);
        return properties;
    }

    static void applySsl(Properties properties, RunContext runContext, LakebaseConnectionInterface conn) throws Exception {
        boolean ssl = runContext.render(conn.getSsl()).as(Boolean.class).orElse(true);
        if (ssl) {
            properties.put("ssl", "true");
        }

        var sslMode = runContext.render(conn.getSslMode()).as(LakebaseConnectionInterface.SslMode.class)
            .orElse(ssl ? LakebaseConnectionInterface.SslMode.REQUIRE : null);
        if (sslMode != null) {
            properties.put("sslmode", sslMode.name().toLowerCase(Locale.ROOT).replace('_', '-'));
        }

        if (conn.getSslRootCert() != null) {
            String pem = runContext.render(conn.getSslRootCert()).as(String.class).orElse(null);
            if (pem != null && !pem.isBlank()) {
                properties.put("sslrootcert", tempPem(runContext, pem, ".root.pem"));
            }
        }

        if (conn.getSslCert() != null) {
            String pem = runContext.render(conn.getSslCert()).as(String.class).orElse(null);
            if (pem != null && !pem.isBlank()) {
                properties.put("sslcert", tempPem(runContext, pem, ".client.pem"));
            }
        }

        if (conn.getSslKey() != null) {
            String pem = runContext.render(conn.getSslKey()).as(String.class).orElse(null);
            if (pem != null && !pem.isBlank()) {
                properties.put("sslkey", tempPem(runContext, pem, ".client.key"));
            }
        }

        if (conn.getSslKeyPassword() != null) {
            runContext.render(conn.getSslKeyPassword()).as(String.class)
                .ifPresent(password -> properties.put("sslpassword", password));
        }
    }

    /**
     * Turns a parameter group payload into one or more prepared-statement executions.
     *
     * <p>
     * A list of lists is treated as multiple rows. A flat list whose size matches the
     * placeholder count is a single row. A flat list of scalars with a single placeholder
     * is expanded into one row per scalar so {@code parameters: "{{ inputs.rows }}"} works
     * with {@code VALUES (?)}.
     */
    @SuppressWarnings("unchecked")
    static List<List<Object>> expandParameterGroup(Object parameters, int placeholderCount) {
        if (parameters == null) {
            return List.of();
        }
        if (!(parameters instanceof List<?> list)) {
            return List.of(List.of(parameters));
        }
        if (list.isEmpty()) {
            return List.of();
        }
        Object first = list.getFirst();
        if (first instanceof List<?> || first instanceof Collection<?>) {
            List<List<Object>> rows = new ArrayList<>(list.size());
            for (Object item : list) {
                rows.add(new ArrayList<>((Collection<Object>) item));
            }
            return rows;
        }
        if (placeholderCount == 1 && list.size() != 1) {
            List<List<Object>> rows = new ArrayList<>(list.size());
            for (Object item : list) {
                rows.add(List.of(item));
            }
            return rows;
        }
        return List.of(new ArrayList<>(list));
    }

    /**
     * pgjdbc {@code PreparedStatement.setObject} cannot infer a SQL type for {@link ZonedDateTime}
     * or {@link Instant}. Query STORE writes {@code timestamptz} as {@code ZonedDateTime}.
     */
    static Object jdbcBindValue(Object value) {
        if (value instanceof ZonedDateTime zonedDateTime) {
            return zonedDateTime.toOffsetDateTime();
        }
        if (value instanceof Instant instant) {
            return instant.atOffset(ZoneOffset.UTC);
        }
        return value;
    }

    static int placeholderCount(String sql) {
        if (sql == null || sql.isBlank()) {
            return 0;
        }
        int count = 0;
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
            } else if (c == '"' && !inSingle) {
                inDouble = !inDouble;
            } else if (c == '?' && !inSingle && !inDouble) {
                count++;
            }
        }
        return count;
    }

    static void registerDriver() throws SQLException {
        if (DriverManager.drivers().noneMatch(Driver.class::isInstance)) {
            DriverManager.registerDriver(new Driver());
        }
    }

    private static String tempPem(RunContext runContext, String pem, String suffix) throws Exception {
        return runContext.workingDir()
            .createTempFile(pem.getBytes(StandardCharsets.UTF_8), suffix)
            .toAbsolutePath()
            .toString();
    }

    private static String renderRequired(
        RunContext runContext,
        io.kestra.core.models.property.Property<String> property,
        String name) throws IllegalVariableEvaluationException {
        return runContext.render(property).as(String.class)
            .filter(value -> !value.isBlank())
            .orElseThrow(() -> new IllegalArgumentException("Missing required Lakebase property: " + name));
    }
}
