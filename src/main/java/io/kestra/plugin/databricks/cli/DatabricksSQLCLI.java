package io.kestra.plugin.databricks.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

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
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;

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
        ),
        @Example(
            title = "Run a SQL file stored as a namespace file.",
            full = true,
            code = """
                id: databricks_cli_query_file
                namespace: company.team

                tasks:
                  - id: run_sql_file
                    type: io.kestra.plugin.databricks.cli.DatabricksSQLCLI
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    token: "{{ secret('DATABRICKS_TOKEN') }}"
                    httpPath: "{{ secret('DATABRICKS_HTTP_PATH') }}"
                    namespaceFiles:
                      enabled: true
                      include:
                        - query.sql
                    commands:
                      - query.sql
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
    static final String ENTRYPOINT_SCRIPT = "/usr/local/bin/kestra-entrypoint.sh";
    // The default image starts as root and its entrypoint script hands the working directory to an unprivileged user.
    // Kestra's Docker runner clears the image entrypoint by default, so it is set explicitly. Images published before
    // the non-root change do not ship the script (and may be cached), so fall back to running the command directly.
    // The runner passes the task command as Cmd, which reaches the wrapper as "$@"; the last item is $0.
    static final List<String> DEFAULT_IMAGE_ENTRYPOINT = List.of(
        "/bin/sh",
        "-c",
        "if [ -x " + ENTRYPOINT_SCRIPT + " ]; then exec " + ENTRYPOINT_SCRIPT + " \"$@\"; else exec \"$@\"; fi",
        "kestra-entrypoint"
    );

    @Schema(
        title = "Databricks host",
        description = "Workspace hostname for the SQL warehouse, without a trailing slash"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> host;

    @Schema(
        title = "Access token",
        description = "Databricks personal access token; render from secrets"
    )
    @NotNull
    @PluginProperty(secret = true, group = "main")
    private Property<String> token;

    @Schema(
        title = "HTTP path",
        description = "HTTP path for the SQL warehouse (from workspace connection details)"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> httpPath;

    @Schema(
        title = "SQL commands",
        description = "One or more SQL statements rendered then executed in order with dbsqlcli"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> commands;

    @Schema(
        title = "Additional CLI options",
        description = "Map of extra dbsqlcli flags appended to the command, e.g. `--output json`"
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, String>> options;

    @Schema(
        title = "Task runner",
        description = "Execution backend; defaults to container-based runner"
    )
    @Builder.Default
    @Valid
    @PluginProperty(group = "execution")
    private TaskRunner<?> taskRunner = Docker.instance();

    @Schema(title = "Task runner container image", description = "Container image used when the task runner is Docker-based; default ghcr.io/kestra-io/databricks-sql-cli")
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<String> containerImage = Property.ofValue(DEFAULT_IMAGE);

    @Schema(
        title = "Deprecated, use taskRunner instead",
        description = "Legacy Docker options kept for backward compatibility; prefer taskRunner"
    )
    @Deprecated
    @PluginProperty(group = "deprecated")
    private DockerOptions docker;

    @PluginProperty(group = "source")
    @Schema(title = "Namespace files", description = "Namespace files to make available to the CLI in the working directory.")
    private NamespaceFiles namespaceFiles;

    @PluginProperty(group = "source")
    @Schema(title = "Input files", description = "Files to make available to the CLI in the working directory, as a map of file name to content or a list of paths/URIs.")
    private Object inputFiles;

    @PluginProperty(group = "destination")
    @Schema(title = "Output files", description = "List of files produced by the CLI to persist to Kestra internal storage.")
    private Property<List<String>> outputFiles;

    @Override
    public ScriptOutput run(RunContext runContext) throws Exception {
        Logger logger = runContext.logger();

        var renderedOutputFiles = runContext.render(this.outputFiles).asList(String.class);

        String token = runContext.render(this.token).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing token"));

        List<String> databricksCommand = getDatabricksCommand(runContext);

        return commands(runContext, databricksCommand, token, renderedOutputFiles).run();
    }

    CommandsWrapper commands(RunContext runContext, List<String> databricksCommand, String token, List<String> renderedOutputFiles)
        throws Exception {
        var wrapper = new CommandsWrapper(runContext)
            .withTaskRunner(this.taskRunner)
            .withDockerOptions(injectDefaults(this.getDocker()))
            .withContainerImage(runContext.render(this.containerImage).as(String.class).orElseThrow())
            .withInterpreter(Property.ofValue(List.of("/bin/sh", "-c")))
            .withCommands(Property.ofValue(databricksCommand))
            .withEnv(Map.of("DATABRICKS_TOKEN", token))
            .withNamespaceFiles(namespaceFiles)
            .withInputFiles(inputFiles)
            .withOutputFiles(renderedOutputFiles.isEmpty() ? null : renderedOutputFiles);

        return applyEntrypoint(wrapper);
    }

    /**
     * Sets the non-root entrypoint on the Docker task runner when it runs the default image. Applied on the wrapper
     * rather than only in {@code injectDefaults}, which Kestra only honors for the deprecated {@code docker} property on
     * 1.3.x and never on 2.x. Custom images, other runners and user-defined entrypoints are left untouched.
     */
    static CommandsWrapper applyEntrypoint(CommandsWrapper commands) {
        TaskRunner<?> runner = commands.getTaskRunner();
        if (!(runner instanceof Docker docker)) {
            return commands;
        }

        // The Docker runner's own image wins over the task containerImage at runtime, so check it first.
        var image = docker.getImage() != null ? docker.getImage() : commands.getContainerImage();
        if (!isDefaultImage(image) || !hasDefaultEntryPoint(docker.getEntryPoint())) {
            return commands;
        }

        return commands.withTaskRunner(docker.toBuilder().entryPoint(DEFAULT_IMAGE_ENTRYPOINT).build());
    }

    private static boolean hasDefaultEntryPoint(List<String> entryPoint) {
        return entryPoint == null || entryPoint.isEmpty() || entryPoint.equals(List.of(""));
    }

    static boolean isDefaultImage(String image) {
        return image != null && (image.equals(DEFAULT_IMAGE) || image.startsWith(DEFAULT_IMAGE + ":") || image.startsWith(DEFAULT_IMAGE + "@"));
    }

    List<String> getDatabricksCommand(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> commands = new ArrayList<>();
        StringBuilder commandBuilder = new StringBuilder("dbsqlcli");

        String host = runContext.render(this.host).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing host"));
        commandBuilder.append(" --hostname ").append(host);

        commandBuilder.append(" --access-token $DATABRICKS_TOKEN");

        String httpPath = runContext.render(this.httpPath).as(String.class).orElseThrow(() -> new IllegalArgumentException("Missing http-path"));
        commandBuilder.append(" --http-path ").append(httpPath);

        List<String> renderedQueries = runContext.render(this.commands).asList(String.class);

        for (String query : renderedQueries) {
            commandBuilder.append(" -e \"").append(query.replace("\"", "\\\"")).append("\"");
        }

        Map<String, String> renderedCliOptions = runContext.render(this.options).asMap(String.class, String.class);

        renderedCliOptions.forEach((key, value) ->
        {
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
        var image = original.getImage();
        if (image == null) {
            image = DEFAULT_IMAGE;
            builder.image(image);
        }
        if (isDefaultImage(image) && (original.getEntryPoint() == null || original.getEntryPoint().isEmpty())) {
            builder.entryPoint(DEFAULT_IMAGE_ENTRYPOINT);
        }

        return builder.build();
    }
}
