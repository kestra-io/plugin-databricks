package io.kestra.plugin.databricks.cli;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.*;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Execute a SQL query using Databricks SQL CLI.",
            full = true,
            code = """
                id: databricks_cli_query
                namespace: company.team

                tasks:
                  - id: run_sql_query
                    type: io.kestra.plugin.databricks.cli.DatabricksSQLCLI
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    token: "{{ secret('DATABRICKS_TOKEN') }}"
                    httpPath: "{{ secret('DATABRICKS_HTTP_PATH') }}"
                    commands:
                        - "SELECT * FROM my_catalog.my_schema.my_table LIMIT 10"
                """
        )
    }
)
@Schema(
    title = "Execute SQL via Databricks SQL CLI",
    description = """
        Runs SQL statements with dbsqlcli in a container (default `ghcr.io/kestra-io/databricks-sql-cli`).
        Renders connection values and queries, then executes them sequentially; use outputFiles to persist CLI outputs.
        """
)
public class DatabricksSQLCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

    public static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/databricks-sql-cli";

    @Schema(
        title = "Databricks host",
        description = "Workspace hostname for the SQL warehouse, without a trailing slash"
    )
    @NotNull
    private Property<String> host;

    @Schema(
        title = "Access token",
        description = "Databricks personal access token; render from secrets"
    )
    @NotNull
    private Property<String> token;

    @Schema(
        title = "HTTP path",
        description = "HTTP path for the SQL warehouse (from workspace connection details)"
    )
    @NotNull
    private Property<String> httpPath;

    @Schema(
        title = "SQL commands",
        description = "One or more SQL statements rendered then executed in order with dbsqlcli"
    )
    @NotNull
    private Property<List<String>> commands;

    @Schema(
        title = "Additional CLI options",
        description = "Map of extra dbsqlcli flags appended to the command, e.g. --output json"
    )
    private Property<Map<String, String>> options;

    @Schema(
        title = "Task runner",
        description = "Execution backend; defaults to container-based runner"
    )
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "Task runner container image", description = "Container image used when the task runner is Docker-based; default ghcr.io/kestra-io/databricks-sql-cli")
    @Builder.Default
    private Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Deprecated, use taskRunner instead",
        description = "Legacy Docker options kept for backward compatibility; prefer taskRunner"
    )
    @Deprecated
    private DockerOptions docker;

    private Object inputFiles;

    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);

        List<String> databricksCommand = getDatabricksCommand(runContext);

        return new CommandsWrapper(runContext)
            .withTaskRunner(this.taskRunner)
            .withDockerOptions(injectDefaults(this.getDocker()))
            .withContainerImage("databricks-sql-cli")
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofValue(databricksCommand))
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles)
            .run();
    }

    List<String> getDatabricksCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>();
        StringBuilder commandBuilder = new StringBuilder("dbsqlcli");

        String host = runContext.render(this.host).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing host"));
        commandBuilder.append(" --hostname ").append(host);

        String token = runContext.render(this.token).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing token"));
        commandBuilder.append(" --access-token ").append(token);

        String httpPath = runContext.render(this.httpPath).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing http-path"));
        commandBuilder.append(" --http-path ").append(httpPath);

        List<String> renderedQueries = runContext.render(this.commands).asList(String.class);

        for (String query : renderedQueries) {
            commandBuilder.append(" -e \"").append(query.replace("\"", "\\\"")).append("\"");
        }

        Map<String, String> renderedCliOptions = runContext.render(this.options).asMap(String.class, String.class);

        renderedCliOptions.forEach((key, value) -> {
            commandBuilder.append(" ").append(key);
            if (value != null && !value.isBlank()) {
                commandBuilder.append(" ").append(value);
            }
        });

        commands.add(commandBuilder.toString());
        return commands;
    }

    private DockerOptions injectDefaults(DockerOptions original) {
        if (original == null) {
            return null;
        }

        var builder = original.toBuilder();
        if (original.getImage() == null) {
            builder.image(DEFAULT_IMAGE);
        }

        return builder.build();
    }

    @Override
    public Object getInputFiles() {
        return null;
    }

    @Override
    public NamespaceFiles getNamespaceFiles() {
        return null;
    }
}
