package io.kestra.plugin.databricks.unitycatalog.table;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.DatabricksConnectionInterface;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogUtils;

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
@Plugin(
    examples = {
        @Example(
            title = "Start a flow when a new table appears in a schema.",
            full = true,
            code = """
                id: databricks_uc_new_table
                namespace: company.team

                tasks:
                  - id: log_new_tables
                    type: io.kestra.plugin.core.log.Log
                    message: "New table(s) detected: {{ trigger.fullNames }}"

                triggers:
                  - id: on_new_table
                    type: io.kestra.plugin.databricks.unitycatalog.table.Trigger
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                    schemaName: landing_zone
                    interval: PT5M
                """
        )
    }
)
@Schema(
    title = "Trigger a flow when a Unity Catalog table appears in a schema",
    description = """
        Periodically lists the tables of a schema and starts one execution for all the tables detected since the previous poll.
        The set of known tables is persisted in the namespace KV Store, so the first evaluation reports every existing table as new when `on` is `CREATE` (the default).
        Use `on: UPDATE` or `on: CREATE_OR_UPDATE` to also react to tables modified since the previous poll."""
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface, DatabricksConnectionInterface {
    @Schema(title = "Databricks host")
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Schema(title = "Databricks account identifier")
    @PluginProperty(group = "advanced")
    private Property<String> accountId;

    @Schema(title = "Databricks configuration file, use this if you don't want to configure each Databricks account properties one by one")
    @PluginProperty(group = "advanced")
    private Property<String> configFile;

    @Schema(
        title = "Databricks authentication configuration",
        description = """
            This property allows to configure the authentication to Databricks, different properties should be set depending on the type of authentication and the cloud provider.
            All configuration options can also be set using the standard Databricks environment variables.
            Check the [Databricks authentication guide](https://docs.databricks.com/dev-tools/auth.html) for more information."""
    )
    @PluginProperty(dynamic = true, group = "connection")
    private AbstractTask.AuthenticationConfig authentication;

    @Schema(
        title = "Catalog name",
        description = "Name of the catalog that contains the monitored schema."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> catalogName;

    @Schema(
        title = "Schema name",
        description = "Name of the schema to monitor."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> schemaName;

    @Builder.Default
    private final Duration interval = Duration.ofMinutes(5);

    @Builder.Default
    private Property<On> on = Property.ofValue(On.CREATE);

    private Property<String> stateKey;

    private Property<Duration> stateTtl;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var logger = runContext.logger();

        var rCatalogName = runContext.render(catalogName).as(String.class).orElseThrow();
        var rSchemaName = runContext.render(schemaName).as(String.class).orElseThrow();
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE);
        var rStateKey = runContext.render(stateKey).as(String.class)
            .orElseGet(() -> StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), getId()));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);

        var state = StatefulTriggerService.readState(runContext, rStateKey, rStateTtl);
        var seen = new HashSet<String>();
        var detected = new ArrayList<Map<String, Object>>();
        var detectedNames = new ArrayList<String>();

        for (var table : workspaceClient(runContext).tables().list(rCatalogName, rSchemaName)) {
            var version = String.valueOf(table.getUpdatedAt());
            var modifiedAt = table.getUpdatedAt() == null ? null : Instant.ofEpochMilli(table.getUpdatedAt());
            var candidate = StatefulTriggerService.Entry.candidate(table.getFullName(), version, modifiedAt);

            seen.add(table.getFullName());
            if (StatefulTriggerService.computeAndUpdateState(state, candidate, rOn).fire()) {
                detected.add(UnityCatalogUtils.toMap(table));
                detectedNames.add(table.getFullName());
            }
        }

        // forget deleted tables so that a table re-created later on is reported again
        state.keySet().retainAll(seen);
        StatefulTriggerService.writeState(runContext, rStateKey, state, rStateTtl);

        if (detected.isEmpty()) {
            return Optional.empty();
        }

        logger.info("Detected {} new or updated table(s) in schema '{}.{}'", detected.size(), rCatalogName, rSchemaName);

        var output = Output.builder()
            .tables(detected)
            .fullNames(detectedNames)
            .size(detected.size())
            .build();

        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Tables detected since the previous poll, as returned by Databricks")
        private final List<Map<String, Object>> tables;

        @Schema(title = "Fully qualified names (`catalog.schema.table`) of the detected tables")
        private final List<String> fullNames;

        @Schema(title = "Number of detected tables")
        private final Integer size;
    }
}
