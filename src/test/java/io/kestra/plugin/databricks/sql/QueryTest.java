package io.kestra.plugin.databricks.sql;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
@Disabled("Need an account to work")
class QueryTest {
    private static final String TOKEN = "";
    private static final String HOST = "";
    private static final String HTTP_PATH = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .accessToken(TOKEN)
            .host(HOST)
            .httpPath(HTTP_PATH)
            .sql("SELECT 1")
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is(1L));
    }
}