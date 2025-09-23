package io.kestra.plugin.databricks.dbfs;

import com.google.api.client.util.Strings;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled because it requires Databricks secrets: host, token"
)
class DownloadTest {
    protected static final String HOST = System.getenv("DATABRICKS_HOST");
    protected static final String TOKEN = System.getenv("DATABRICKS_TOKEN");

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = Download.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(Property.ofValue(TOKEN)).build()
            )
            .host(Property.ofValue(HOST))
            .from(Property.ofValue("/Share/test.txt"))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getUri().toString(), endsWith(".txt"));
    }

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(HOST) || Strings.isNullOrEmpty(TOKEN);
    }
}