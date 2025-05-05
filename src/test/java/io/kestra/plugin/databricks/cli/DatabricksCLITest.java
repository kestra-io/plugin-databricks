package io.kestra.plugin.databricks.cli;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.scripts.exec.scripts.models.ScriptOutput;
import jakarta.inject.Inject;
import lombok.Getter;
import org.assertj.core.util.Strings;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest
@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled for CI/CD as requires secrets data, such as: host, token, httpPath"
)
public class DatabricksCLITest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {

        var databricksCLI = DatabricksCLI.builder()
            .id(IdUtils.create())
            .type(DatabricksCLI.class.getName())
            .host(Property.of(getHost()))
            .token(Property.of(getToken()))
            .httpPath(Property.of(getHttpPath()))
            .commands(
                Property.of(List.of("dbsqlcli")))
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, databricksCLI, Map.of());

        ScriptOutput output = databricksCLI.run(runContext);

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

