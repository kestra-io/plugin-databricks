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
    title = "Execute SQL using Databricks SQL CLI.",
    description = "This task allows you to execute SQL commands using the Databricks SQL CLI."
)
public class DatabricksSQLCLI extends Task implements RunnableTask<ScriptOutput>, NamespaceFilesInterface, InputFilesInterface, OutputFilesInterface {

    public static final String DEFAULT_IMAGE = "ghcr.io/kestra-io/databricks-sql-cli";

    @Schema(
        title = "Databricks host",
        description = "The Databricks workspace host URL"
    )
    @NotNull
    private Property<String> host;

    @Schema(
        title = "Access token",
        description = "Databricks personal access token for authentication"
    )
    @NotNull
    private Property<String> token;

    @Schema(
        title = "HTTP path",
        description = "The HTTP path to the Databricks SQL warehouse"
    )
    @NotNull
    private Property<String> httpPath;

    @Schema(
        title = "SQL query",
        description = "The SQL query to execute"
    )
    @NotNull
    private Property<List<String>> commands;

    @Schema(
        title = "Additional CLI options"
    )
    private Property<Map<String, String>> options;

    @Schema(
        title = "The task runner to use.",
        description = "Task runners are provided by plugins, each have their own properties."
    )
    @Builder.Default
    @Valid
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "The task runner container image, only used if the task runner is container-based.")
    @Builder.Default
    private Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Deprecated, use 'taskRunner' instead"
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