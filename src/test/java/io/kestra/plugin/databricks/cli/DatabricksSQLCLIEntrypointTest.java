package io.kestra.plugin.databricks.cli;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.runners.TaskRunner;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.core.runner.Process;
import io.kestra.plugin.scripts.exec.scripts.models.DockerOptions;
import io.kestra.plugin.scripts.runner.docker.Docker;
import io.kestra.plugin.scripts.runner.docker.PullPolicy;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
class DatabricksSQLCLIEntrypointTest {
    private static final List<String> SCRIPT = DatabricksSQLCLI.DEFAULT_IMAGE_ENTRYPOINT;

    @Inject
    RunContextFactory runContextFactory;

    private io.kestra.plugin.scripts.exec.scripts.runners.CommandsWrapper commandsOf(TaskRunner<?> runner, String image, DockerOptions docker) throws Exception {
        var builder = DatabricksSQLCLI.builder()
            .id("sqlcli").type(DatabricksSQLCLI.class.getName())
            .host(Property.ofValue("h")).token(Property.ofValue("t")).httpPath(Property.ofValue("p"))
            .commands(Property.ofValue(List.of("select 1")))
            .docker(docker);
        if (runner != null) {
            builder.taskRunner(runner);
        }
        if (image != null) {
            builder.containerImage(Property.ofValue(image));
        }
        return builder.build().commands(runContextFactory.of(), List.of("dbsqlcli"), "t", List.of());
    }

    private List<String> entryPointOf(TaskRunner<?> runner, String image) throws Exception {
        TaskRunner<?> effective = commandsOf(runner, image, null).getTaskRunner();
        return ((Docker) effective).getEntryPoint();
    }

    private static Docker.DockerBuilder<?, ?> docker() {
        return Docker.builder().type(Docker.class.getName()).pullPolicy(Property.ofValue(PullPolicy.IF_NOT_PRESENT));
    }

    @Test
    void defaultRunnerAndImageUsesEntrypointScript() throws Exception {
        assertThat(entryPointOf(null, null), is(SCRIPT));
        assertThat(entryPointOf(docker().build(), "ghcr.io/kestra-io/databricks-sql-cli:latest"), is(SCRIPT));
    }

    @Test
    void userEntrypointIsPreserved() throws Exception {
        assertThat(entryPointOf(docker().entryPoint(List.of("/bin/custom")).build(), null), is(List.of("/bin/custom")));
    }

    @Test
    void customImageKeepsClearedEntrypoint() throws Exception {
        assertThat(entryPointOf(null, "my/image:1"), is(List.of("")));
        assertThat(entryPointOf(docker().build(), "ghcr.io/kestra-io/databricks-sql-cli-custom"), is(List.of("")));
    }

    @Test
    void imageSetOnDockerRunnerIsTheOneChecked() throws Exception {
        assertThat(entryPointOf(docker().image("python:3.11-slim").build(), null), is(List.of("")));
        assertThat(entryPointOf(docker().image("ghcr.io/kestra-io/databricks-sql-cli").build(), "python:3.11-slim"), is(SCRIPT));
    }

    @Test
    void nonDockerRunnerIsUntouched() throws Exception {
        var runner = Process.builder().type(Process.class.getName()).build();

        TaskRunner<?> effective = commandsOf(runner, null, null).getTaskRunner();

        assertThat(effective == runner, is(true));
    }

    @Test
    void deprecatedDockerOptionsGetEntrypointForDefaultImage() throws Exception {
        var options = commandsOf(null, null, DockerOptions.builder().build()).getDockerOptions();

        assertThat(options.getImage(), is(DatabricksSQLCLI.DEFAULT_IMAGE));
        assertThat(options.getEntryPoint(), is(SCRIPT));
    }
}
