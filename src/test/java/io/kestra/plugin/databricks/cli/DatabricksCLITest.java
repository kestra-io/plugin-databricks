package io.kestra.plugin.databricks.cli;

import java.util.List;
import java.util.Map;

import org.assertj.core.util.Strings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class DatabricksCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    @DisabledIf(
        value = "canNotBeEnabled",
        disabledReason = "Disabled for CI/CD as requires secrets data: host, token"
    )
    void run() throws Exception {
        var databricksCLI = DatabricksCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksCLI.class.getName())
            .databricksHost(Property.ofValue(getHost()))
            .databricksToken(Property.ofValue(getToken()))
            .commands(Property.ofValue(List.of("databricks clusters list")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, databricksCLI, Map.of());

        ScriptOutput output = databricksCLI.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

    @Test
    void buildEnvPrefix_patOnly() {
        var task = DatabricksCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksCLI.class.getName())
            .build();

        var prefix = task.buildEnvPrefix("https://host.databricks.com", "my-token", null, null);

        assertThat(prefix, containsString("DATABRICKS_TOKEN=my-token"));
        assertThat(prefix, containsString("DATABRICKS_HOST=https://host.databricks.com"));
        assertThat(prefix, not(containsString("DATABRICKS_CLIENT_ID")));
        assertThat(prefix, not(containsString("DATABRICKS_CLIENT_SECRET")));
    }

    @Test
    void buildEnvPrefix_oauthM2m() {
        var task = DatabricksCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksCLI.class.getName())
            .build();

        var prefix = task.buildEnvPrefix("https://host.databricks.com", null, "client-id", "client-secret");

        assertThat(prefix, containsString("DATABRICKS_CLIENT_ID=client-id"));
        assertThat(prefix, containsString("DATABRICKS_CLIENT_SECRET=client-secret"));
        assertThat(prefix, containsString("DATABRICKS_HOST=https://host.databricks.com"));
        assertThat(prefix, not(containsString("DATABRICKS_TOKEN")));
    }

    @Test
    void buildEnvPrefix_tokenTakesPrecedenceOverOauth() {
        var task = DatabricksCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksCLI.class.getName())
            .build();

        var prefix = task.buildEnvPrefix("https://host.databricks.com", "my-token", "client-id", "client-secret");

        assertThat(prefix, containsString("DATABRICKS_TOKEN=my-token"));
        assertThat(prefix, not(containsString("DATABRICKS_CLIENT_ID")));
        assertThat(prefix, not(containsString("DATABRICKS_CLIENT_SECRET")));
    }

    @Test
    void buildEnvPrefix_neitherThrows() {
        var task = DatabricksCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksCLI.class.getName())
            .build();

        assertThrows(IllegalArgumentException.class,
            () -> task.buildEnvPrefix("https://host.databricks.com", null, null, null));
    }

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(getHost()) || Strings.isNullOrEmpty(getToken());
    }

    protected static String getHost() {
        return System.getenv("DATABRICKS_HOST");
    }

    protected static String getToken() {
        return System.getenv("DATABRICKS_TOKEN");
    }
}
