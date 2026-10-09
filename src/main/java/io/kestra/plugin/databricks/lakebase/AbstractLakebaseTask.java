package io.kestra.plugin.databricks.lakebase;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractLakebaseTask extends Task implements LakebaseConnectionInterface {
    @NotNull
    @Schema(
        title = "Databricks workspace host",
        description = "Workspace URL used by the Databricks SDK to mint a Lakebase database credential, e.g. https://<instance>.cloud.databricks.com"
    )
    @PluginProperty(group = "connection")
    protected Property<String> workspaceHost;

    @NotNull
    @Schema(
        title = "OAuth M2M client ID",
        description = "Service principal client ID (UUID). Also used as the Postgres username when connecting to Lakebase."
    )
    @PluginProperty(group = "connection")
    protected Property<String> clientId;

    @NotNull
    @Schema(
        title = "OAuth M2M client secret",
        description = "Service principal OAuth secret used to mint a short-lived Lakebase database credential. Render from secrets."
    )
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> clientSecret;

    @NotNull
    @Schema(
        title = "Lakebase endpoint resource name",
        description = "Passed to generateDatabaseCredential. Format: projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>"
    )
    @PluginProperty(group = "connection")
    protected Property<String> endpoint;

    @Schema(
        title = "Lakebase Postgres hostname",
        description = "Optional. When omitted, the hostname is resolved from the endpoint via the Databricks Postgres API (status.hosts.host)."
    )
    @PluginProperty(group = "connection")
    protected Property<String> host;

    @Builder.Default
    @Schema(
        title = "Lakebase Postgres port",
        description = "Defaults to 5432"
    )
    @PluginProperty(group = "connection")
    protected Property<Integer> port = Property.ofValue(LakebaseService.DEFAULT_PORT);

    @NotNull
    @Schema(
        title = "Postgres database name",
        description = "Database to open after authentication, e.g. databricks_postgres or orders_db"
    )
    @PluginProperty(group = "connection")
    protected Property<String> database;

    @Builder.Default
    @Schema(
        title = "Enable SSL",
        description = "Lakebase requires SSL. Defaults to true."
    )
    @PluginProperty(group = "connection")
    protected Property<Boolean> ssl = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "SSL mode",
        description = "Postgres sslmode. Defaults to REQUIRE, which is the Lakebase-recommended setting."
    )
    @PluginProperty(group = "connection")
    protected Property<SslMode> sslMode = Property.ofValue(SslMode.REQUIRE);

    @Schema(
        title = "SSL root certificate",
        description = "PEM-encoded CA certificate written to a temp file and passed to the Postgres driver as sslrootcert"
    )
    @PluginProperty(group = "connection")
    protected Property<String> sslRootCert;

    @Schema(
        title = "SSL client certificate",
        description = "PEM-encoded client certificate written to a temp file and passed to the Postgres driver as sslcert"
    )
    @PluginProperty(group = "connection")
    protected Property<String> sslCert;

    @Schema(
        title = "SSL client key",
        description = "PEM-encoded client key written to a temp file and passed to the Postgres driver as sslkey"
    )
    @PluginProperty(group = "connection")
    protected Property<String> sslKey;

    @Schema(
        title = "SSL client key password"
    )
    @PluginProperty(group = "connection", secret = true)
    protected Property<String> sslKeyPassword;
}
