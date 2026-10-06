package io.kestra.plugin.databricks.cli;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.InputFilesInterface;
import io.kestra.core.models.tasks.NamespaceFiles;
import io.kestra.core.models.tasks.runners.TaskCommands;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.models.tasks.runners.TaskRunnerDetailResult;
import io.kestra.core.models.tasks.runners.TaskRunnerResult;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

// DatabricksSQLCLITest is disabled as a whole when no Databricks credentials are set, hence a separate class.
@KestraTest
class DatabricksSQLCLIRunnerTest {
    private static final String TASK = """
        id: sql
        type: io.kestra.plugin.databricks.cli.DatabricksSQLCLI
        host: my-host
        token: my-token
        httpPath: /sql/1.0/warehouses/abc
        commands:
          - query.sql
        """;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void namespaceFilesIsReadFromYaml() throws Exception {
        var task = JacksonMapper.ofYaml().readValue(TASK + """
            namespaceFiles:
              enabled: true
              include:
                - query.sql
            """, DatabricksSQLCLI.class);

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());

        assertThat(runContext.render(task.getNamespaceFiles().getInclude()).asList(String.class), is(List.of("query.sql")));
    }

    @Test
    void inputFilesSurviveSerialization() throws Exception {
        var task = JacksonMapper.ofYaml().readValue(TASK + """
            inputFiles:
              query.sql: "SELECT 1"
            """, DatabricksSQLCLI.class);

        // the task reaches the worker serialized as JSON, so its properties must survive a round trip
        var roundTrip = JacksonMapper.ofJson().readValue(JacksonMapper.ofJson().writeValueAsString(task), DatabricksSQLCLI.class);

        assertThat(((InputFilesInterface) roundTrip).getInputFiles(), is(Map.of("query.sql", "SELECT 1")));
    }

    @Test
    void namespaceFilesAreStagedBeforeTheCommandRuns() throws Exception {
        var runner = new RecordingTaskRunner();
        var task = DatabricksSQLCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksSQLCLI.class.getName())
            .host(Property.ofValue("my-host"))
            .token(Property.ofValue("my-token"))
            .httpPath(Property.ofValue("/sql/1.0/warehouses/abc"))
            .commands(Property.ofValue(List.of("query.sql")))
            .namespaceFiles(NamespaceFiles.builder().enabled(Property.ofValue(true)).build())
            .taskRunner(runner)
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        runContext.storage().namespace().putFile(
            Path.of("query.sql"),
            new ByteArrayInputStream("SELECT 1".getBytes(StandardCharsets.UTF_8))
        );

        var output = task.run(runContext);

        assertThat(output.getExitCode(), is(0));
        assertThat(runner.workingDirectoryFiles, hasItem("query.sql"));
        assertThat(Files.readString(runContext.workingDir().path().resolve("query.sql")), is("SELECT 1"));
    }

    @Test
    void defaultContainerImageIsTheDatabricksSqlCliImage() throws Exception {
        var runner = new RecordingTaskRunner();
        var task = task(runner).build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(runner.containerImage, is("ghcr.io/kestra-io/databricks-sql-cli"));
    }

    @Test
    void containerImagePropertyIsUsed() throws Exception {
        var runner = new RecordingTaskRunner();
        var task = task(runner).containerImage(Property.ofValue("my-registry/databricks-sql-cli:1.0")).build();

        task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(runner.containerImage, is("my-registry/databricks-sql-cli:1.0"));
    }

    private static DatabricksSQLCLI.DatabricksSQLCLIBuilder<?, ?> task(RecordingTaskRunner runner) {
        return DatabricksSQLCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksSQLCLI.class.getName())
            .host(Property.ofValue("my-host"))
            .token(Property.ofValue("my-token"))
            .httpPath(Property.ofValue("/sql/1.0/warehouses/abc"))
            .commands(Property.ofValue(List.of("SELECT 1")))
            .taskRunner(runner);
    }

    // stands in for a container runner: records what it was asked to run instead of running it, so no Docker or dbsqlcli is needed
    public static class RecordingTaskRunner extends TaskRunner<TaskRunnerDetailResult> {
        private String containerImage;
        private List<String> workingDirectoryFiles;

        public RecordingTaskRunner() {
            this.type = RecordingTaskRunner.class.getName();
        }

        @Override
        public TaskRunnerResult<TaskRunnerDetailResult> run(RunContext runContext, TaskCommands taskCommands, List<String> filesToDownload) throws Exception {
            this.containerImage = taskCommands.getContainerImage();
            try (var files = Files.list(taskCommands.getWorkingDirectory())) {
                this.workingDirectoryFiles = files.map(Path::getFileName).map(Path::toString).sorted().toList();
            }
            return new TaskRunnerResult<>(0, taskCommands.getLogConsumer());
        }
    }
}
