package io.kestra.plugin.databricks.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.Example;
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

import java.util.List;

@SuperBuilder
@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Schema(
    title = "Run Databricks CLI commands",
    description = """
        Executes Databricks CLI statements inside the official CLI container (default `ghcr.io/databricks/cli:latest`).
        Injects DATABRICKS_HOST and DATABRICKS_TOKEN as environment variables for each command.
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
        )
    }
)
public class DatabricksCLI extends AbstractExecScript implements RunnableTask<ScriptOutput> {
    private static final String DEFAULT_IMAGE = "ghcr.io/databricks/cli:latest";

    @Builder.Default
    protected Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(title = "CLI commands to execute", description = "Commands run sequentially with host/token pre-set in the environment")
    @NotNull
    protected Property<List<String>> commands;

    @Schema(title = "Databricks host URL", description = "Workspace URL including protocol, e.g. https://<instance>.cloud.databricks.com")
    @NotNull
    protected Property<String> databricksHost;

    @Schema(title = "Databricks personal access token", description = "PAT exported to DATABRICKS_TOKEN for each command; render from secrets")
    @NotNull
    protected Property<String> databricksToken;

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
        TargetOS os = runContext.render(this.targetOS).as(TargetOS.class).orElse(null);

        String host = runContext.render(databricksHost).as(String.class).orElseThrow();
        String token = runContext.render(databricksToken).as(String.class).orElseThrow();

        List<String> envCommands = runContext.render(commands)
            .asList(String.class)
            .stream()
            .map(cmd -> String.format("DATABRICKS_HOST=%s DATABRICKS_TOKEN=%s %s", host, token, cmd))
            .toList();

        return this.commands(runContext)
            .withInterpreter(this.interpreter)
            .withBeforeCommands(beforeCommands)
            .withBeforeCommandsWithOptions(true)
            .withCommands(Property.ofValue(envCommands))
            .withTargetOS(os)
            .run();
    }
}
