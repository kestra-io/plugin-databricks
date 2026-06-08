package io.kestra.plugin.databricks.cli;

import java.util.List;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.runners.TargetOS;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.AbstractExecScript;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Schema(
    title = "Run Databricks CLI commands",
    description = """
        Executes Databricks CLI statements inside the official CLI container (default `ghcr.io/databricks/cli:latest`).
        Supports two authentication modes: PAT (via `databricksToken`) or OAuth M2M service principal
        (via `clientId` + `clientSecret`). When `databricksToken` is set it always takes precedence.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "List Databricks clusters",
            code = """
                id: databricks_list_clusters
                namespace: company.team

                tasks:
                  - id: list_clusters
                    type: io.kestra.plugin.databricks.cli.DatabricksCLI
                    databricksToken: "{{ secret('DATABRICKS_TOKEN') }}"
                    databricksHost: "https://<your-instance>.cloud.databricks.com"
                    commands:
                      - databricks clusters list
                """
        ),
        @Example(
            full = true,
            title = "Run a job in Databricks",
            code = """
                id: databricks_run_job
                namespace: company.team

                inputs:
                  - id: jobId
                    type: STRING

                tasks:
                  - id: run_job
                    type: io.kestra.plugin.databricks.cli.DatabricksCLI
                    databricksToken: "{{ secret('DATABRICKS_TOKEN') }}"
                    databricksHost: "https://<your-instance>.cloud.databricks.com"
                    commands:
                      - databricks jobs run-now {{ inputs.jobId }} > files.txt
                """
        ),
        @Example(
            full = true,
            title = "List Databricks clusters using OAuth M2M service principal",
            code = """
                id: databricks_cli_oauth
                namespace: company.team

                tasks:
                  - id: list_clusters
                    type: io.kestra.plugin.databricks.cli.DatabricksCLI
                    databricksHost: "{{ secret('DATABRICKS_HOST') }}"
                    clientId: "{{ secret('DATABRICKS_CLIENT_ID') }}"
                    clientSecret: "{{ secret('DATABRICKS_CLIENT_SECRET') }}"
                    commands:
                      - databricks clusters list
                """
        )
    }
)
public class DatabricksCLI extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "ghcr.io/databricks/cli:latest";

    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(title = "CLI commands to execute", description = "Commands run sequentially with host and auth vars pre-set in the environment")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<List<String>> commands;

    @Schema(title = "Databricks host URL", description = "Workspace URL including protocol, e.g. https://<instance>.cloud.databricks.com")
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> databricksHost;

    @Schema(
        title = "Databricks personal access token",
        description = """
            PAT exported to DATABRICKS_TOKEN for each command; render from secrets.
            Use this for PAT-based auth. Takes precedence over `clientId`/`clientSecret` when both are set.
            """
    )
    @PluginProperty(secret = true, group = "main")
    protected Property<String> databricksToken;

    @Schema(
        title = "OAuth M2M client ID",
        description = "Service principal client ID; exported to DATABRICKS_CLIENT_ID. Use with `clientSecret` as an alternative to `databricksToken`."
    )
    @PluginProperty(group = "main")
    protected Property<String> clientId;

    @Schema(
        title = "OAuth M2M client secret",
        description = "Service principal secret; exported to DATABRICKS_CLIENT_SECRET. Render from secrets."
    )
    @PluginProperty(secret = true, group = "main")
    protected Property<String> clientSecret;

    @Override
    protected DockerOptions injectDefaults(RunContext runContext, DockerOptions original) throws IllegalVariableEvaluationException {
        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(runContext.render(this.getContainerImage()).as(String.class).orElse(null));
        }

        return builder.build();
    }

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        var os = runContext.render(this.targetOS).as(TargetOS.class).orElse(null);

        var host = runContext.render(databricksHost).as(String.class).orElseThrow();
        var rToken = runContext.render(databricksToken).as(String.class).orElse(null);
        var rClientId = runContext.render(clientId).as(String.class).orElse(null);
        var rClientSecret = runContext.render(clientSecret).as(String.class).orElse(null);

        var envPrefix = buildEnvPrefix(host, rToken, rClientId, rClientSecret);

        var envCommands = runContext.render(commands)
            .asList(String.class)
            .stream()
            .map(cmd -> envPrefix + " " + cmd)
            .toList();

        return this.commands(runContext)
            .withInterpreter(this.interpreter)
            .withBeforeCommands(beforeCommands)
            .withBeforeCommandsWithOptions(true)
            .withCommands(Property.ofValue(envCommands))
            .withTargetOS(os)
            .run();
    }

    String buildEnvPrefix(String host, String token, String clientId, String clientSecret) {
        if (token != null) {
            return String.format("DATABRICKS_HOST=%s DATABRICKS_TOKEN=%s", host, token);
        } else if (clientId != null && clientSecret != null) {
            return String.format("DATABRICKS_HOST=%s DATABRICKS_CLIENT_ID=%s DATABRICKS_CLIENT_SECRET=%s", host, clientId, clientSecret);
        }
        throw new IllegalArgumentException(
            "DatabricksCLI requires authentication: set `databricksToken` (PAT) or both `clientId` and `clientSecret` (OAuth M2M).");
    }
}
