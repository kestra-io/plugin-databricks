package io.kestra.plugin.databricks.dbfs;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Disabled("Need an account to work")
class DownloadTest {
    private static final String TOKEN = "";
    private static final String HOST = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = Download.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(Property.of(TOKEN)).build()
            )
            .host(Property.of(HOST))
            .from(Property.of("/Share/test.txt"))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getUri().toString(), endsWith(".txt"));
    }
}