package io.kestra.plugin.databricks.lakebase;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;

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
@Schema(
    title = "Wait for query results on Databricks Lakebase and trigger a flow",
    description = """
        Periodically mints a fresh Lakebase OAuth credential and polls a SQL query. The trigger fires
        when the query returns at least one row. Default interval is one minute. Use fetchType to control
        how matching rows are exposed on the execution (`trigger.rows`, `trigger.row`, or `trigger.uri`).
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "React to newly inserted rows",
            code = """
                id: lakebase_new_rows_trigger
                namespace: company.team

                triggers:
                  - id: on_new_order
                    type: io.kestra.plugin.databricks.lakebase.Trigger
                    workspaceHost: "{{ secret('DATABRICKS_HOST') }}"
                    clientId: "{{ secret('DATABRICKS_CLIENT_ID') }}"
                    clientSecret: "{{ secret('DATABRICKS_CLIENT_SECRET') }}"
                    endpoint: "{{ secret('LAKEBASE_ENDPOINT_NAME') }}"
                    database: "orders_db"
                    sql: "SELECT * FROM orders WHERE status = 'pending'"
                    interval: PT1M

                tasks:
                  - id: handle_new_order
                    type: io.kestra.plugin.core.log.Log
                    message: "New pending order: {{ trigger.rows }}"
                """
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Query.Output>, LakebaseConnectionInterface {
    @Builder.Default
    private final Duration interval = Duration.ofMinutes(1);

    @NotNull
    @Schema(
        title = "Databricks workspace host",
        description = "Workspace URL used by the Databricks SDK to mint a Lakebase database credential, e.g. https://<instance>.cloud.databricks.com"
    )
    @PluginProperty(group = "connection")
    private Property<String> workspaceHost;

    @NotNull
    @Schema(
        title = "OAuth M2M client ID",
        description = "Service principal client ID (UUID). Also used as the Postgres username when connecting to Lakebase."
    )
    @PluginProperty(group = "connection")
    private Property<String> clientId;

    @NotNull
    @Schema(
        title = "OAuth M2M client secret",
        description = "Service principal OAuth secret used to mint a short-lived Lakebase database credential. Render from secrets."
    )
    @PluginProperty(group = "connection", secret = true)
    private Property<String> clientSecret;

    @NotNull
    @Schema(
        title = "Lakebase endpoint resource name",
        description = "Passed to generateDatabaseCredential. Format: projects/<project-id>/branches/<branch-id>/endpoints/<endpoint-id>"
    )
    @PluginProperty(group = "connection")
    private Property<String> endpoint;

    @Schema(
        title = "Lakebase Postgres hostname",
        description = "Optional. When omitted, the hostname is resolved from the endpoint via the Databricks Postgres API (status.hosts.host)."
    )
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Builder.Default
    @Schema(
        title = "Lakebase Postgres port",
        description = "Defaults to 5432"
    )
    @PluginProperty(group = "connection")
    private Property<Integer> port = Property.ofValue(LakebaseService.DEFAULT_PORT);

    @NotNull
    @Schema(
        title = "Postgres database name",
        description = "Database to open after authentication, e.g. databricks_postgres or orders_db"
    )
    @PluginProperty(group = "connection")
    private Property<String> database;

    @Builder.Default
    @Schema(
        title = "Enable SSL",
        description = "Lakebase requires SSL. Defaults to true."
    )
    @PluginProperty(group = "connection")
    private Property<Boolean> ssl = Property.ofValue(true);

    @Builder.Default
    @Schema(
        title = "SSL mode",
        description = "Postgres sslmode. Defaults to REQUIRE, which is the Lakebase-recommended setting."
    )
    @PluginProperty(group = "connection")
    private Property<SslMode> sslMode = Property.ofValue(SslMode.REQUIRE);

    @Schema(
        title = "SSL root certificate",
        description = "PEM-encoded CA certificate written to a temp file and passed to the Postgres driver as sslrootcert"
    )
    @PluginProperty(group = "connection")
    private Property<String> sslRootCert;

    @Schema(
        title = "SSL client certificate",
        description = "PEM-encoded client certificate written to a temp file and passed to the Postgres driver as sslcert"
    )
    @PluginProperty(group = "connection")
    private Property<String> sslCert;

    @Schema(
        title = "SSL client key",
        description = "PEM-encoded client key written to a temp file and passed to the Postgres driver as sslkey"
    )
    @PluginProperty(group = "connection")
    private Property<String> sslKey;

    @Schema(
        title = "SSL client key password"
    )
    @PluginProperty(group = "connection", secret = true)
    private Property<String> sslKeyPassword;

    @NotNull
    @Schema(title = "SQL query to poll", description = "The trigger fires when this query returns at least one row")
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Schema(
        title = "SQL to execute after a successful poll in the same transaction",
        description = "Optional single statement used to mark processed rows so the next poll does not re-fire on the same data"
    )
    @PluginProperty(group = "advanced")
    private Property<String> afterSQL;

    @NotNull
    @Builder.Default
    @Schema(
        title = "Result fetching mode",
        description = "Triggers default to FETCH. NONE is rejected because the trigger would never fire. Use FETCH, FETCH_ONE, or STORE."
    )
    @PluginProperty(group = "main")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Schema(
        title = "Named parameter bindings",
        description = "Map of parameter names to values. Use :name placeholders in SQL."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;

    @Schema(
        title = "Time zone for temporal values"
    )
    @PluginProperty(group = "execution")
    private Property<String> timeZoneId;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        FetchType type = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        if (type == FetchType.NONE) {
            throw new IllegalArgumentException("fetchType NONE is not valid for triggers — the trigger would never fire. Use FETCH, FETCH_ONE, or STORE.");
        }

        Query.Output output = query().run(runContext);
        runContext.logger().debug("Lakebase trigger query returned {} row(s)", output == null ? 0 : output.getSize());

        if (!shouldFire(output)) {
            return Optional.empty();
        }

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    static boolean shouldFire(Query.Output output) {
        return output != null && output.getSize() != null && output.getSize() > 0;
    }

    Query query() {
        return Query.builder()
            .id(this.id)
            .type(Query.class.getName())
            .workspaceHost(this.workspaceHost)
            .clientId(this.clientId)
            .clientSecret(this.clientSecret)
            .endpoint(this.endpoint)
            .host(this.host)
            .port(this.port)
            .database(this.database)
            .ssl(this.ssl)
            .sslMode(this.sslMode)
            .sslRootCert(this.sslRootCert)
            .sslCert(this.sslCert)
            .sslKey(this.sslKey)
            .sslKeyPassword(this.sslKeyPassword)
            .sql(this.sql)
            .afterSQL(this.afterSQL)
            .fetchType(this.fetchType)
            .parameters(this.parameters)
            .timeZoneId(this.timeZoneId)
            .build();
    }
}
