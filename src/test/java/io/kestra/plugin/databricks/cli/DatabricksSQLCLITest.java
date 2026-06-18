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
import static org.hamcrest.Matchers.is;

@KestraTest
@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled for CI/CD as requires secrets data, such as: host, token, httpPath"
)
public class DatabricksSQLCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {

        var databricksSQLCLI = DatabricksSQLCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksSQLCLI.class.getName())
            .host(Property.ofValue(getHost()))
            .token(Property.ofValue(getToken()))
            .httpPath(Property.ofValue(getHttpPath()))
            .commands(
                Property.ofValue(List.of("dbsqlcli"))
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, databricksSQLCLI, Map.of());

        ScriptOutput output = databricksSQLCLI.run(runContext);

        assertThat(output.getExitCode(), is(0));
    }

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(getHost()) || Strings.isNullOrEmpty(getToken()) || Strings.isNullOrEmpty(getHttpPath());
    }

    protected static String getHost() {
        return "";
    }

    protected static String getToken() {
        return "";
    }

    protected static String getHttpPath() {
        return "";
    }
}
