package io.kestra.plugin.databricks.lakebase;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * Connection contract for Lakebase tasks and triggers.
 *
 * <p>
 * Lakebase speaks the Postgres wire protocol but authenticates with a short-lived OAuth
 * token minted via the Databricks SDK. Implementations expose workspace OAuth properties plus
 * the Postgres/SSL settings needed to open a JDBC connection after the token is issued.
 */
public interface LakebaseConnectionInterface {
    @Schema(
        title = "Databricks workspace host",
        description = "Workspace URL used by the Databricks SDK to mint a Lakebase database credential, e.g. https://<instance>.cloud.databricks.com"
    )
    @PluginProperty(group = "connection")
    Property<String> getWorkspaceHost();

    @Schema(
        title = "OAuth M2M client ID",
        description = "Service principal client ID (UUID). Also used as the Postgres username when connecting to Lakebase."
    )
    @PluginProperty(group = "connection")
    Property<String> getClientId();

    @Schema(
        title = "OAuth M2M client secret",
        description = "Service principal OAuth secret used to mint a short-lived Lakebase database credential. Render from secrets."
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getClientSecret();

    @Schema(
        title = "Lakebase endpoint resource name",
        description = "Passed to generateDatabaseCredential. Format: projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>"
    )
    @PluginProperty(group = "connection")
    Property<String> getEndpoint();

    @Schema(
        title = "Lakebase Postgres hostname",
        description = "Optional. When omitted, the hostname is resolved from the endpoint via the Databricks Postgres API (status.hosts.host)."
    )
    @PluginProperty(group = "connection")
    Property<String> getHost();

    @Schema(
        title = "Lakebase Postgres port",
        description = "Defaults to 5432"
    )
    @PluginProperty(group = "connection")
    Property<Integer> getPort();

    @Schema(
        title = "Postgres database name",
        description = "Database to open after authentication, e.g. databricks_postgres or orders_db"
    )
    @PluginProperty(group = "connection")
    Property<String> getDatabase();

    @Schema(
        title = "Enable SSL",
        description = "Lakebase requires SSL. Defaults to true."
    )
    @PluginProperty(group = "connection")
    Property<Boolean> getSsl();

    @Schema(
        title = "SSL mode",
        description = "Postgres sslmode. Defaults to REQUIRE, which is the Lakebase-recommended setting."
    )
    @PluginProperty(group = "connection")
    Property<SslMode> getSslMode();

    @Schema(
        title = "SSL root certificate",
        description = "PEM-encoded CA certificate written to a temp file and passed to the Postgres driver as sslrootcert"
    )
    @PluginProperty(group = "connection")
    Property<String> getSslRootCert();

    @Schema(
        title = "SSL client certificate",
        description = "PEM-encoded client certificate written to a temp file and passed to the Postgres driver as sslcert"
    )
    @PluginProperty(group = "connection")
    Property<String> getSslCert();

    @Schema(
        title = "SSL client key",
        description = "PEM-encoded client key written to a temp file and passed to the Postgres driver as sslkey"
    )
    @PluginProperty(group = "connection")
    Property<String> getSslKey();

    @Schema(
        title = "SSL client key password"
    )
    @PluginProperty(group = "connection", secret = true)
    Property<String> getSslKeyPassword();

    enum SslMode {
        DISABLE,
        ALLOW,
        PREFER,
        REQUIRE,
        VERIFY_CA,
        VERIFY_FULL
    }
}
