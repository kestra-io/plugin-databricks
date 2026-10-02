package io.kestra.plugin.databricks.zerobus;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import com.google.common.base.Strings;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask.AuthenticationConfig;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
@DisabledIf(value = "canNotBeEnabled", disabledReason = "Requires Databricks Zerobus env vars")
class WriteRecordsIntegrationTest {

    static final String HOST = System.getenv("DATABRICKS_HOST");
    static final String TOKEN = System.getenv("DATABRICKS_TOKEN");
    static final String WORKSPACE_ID = System.getenv("DATABRICKS_WORKSPACE_ID");
    static final String REGION = System.getenv("DATABRICKS_REGION");
    static final String CATALOG = System.getenv("DATABRICKS_ZEROBUS_CATALOG");
    static final String SCHEMA = System.getenv("DATABRICKS_ZEROBUS_SCHEMA");
    static final String TABLE = System.getenv("DATABRICKS_ZEROBUS_TABLE");

    @Inject
    private RunContextFactory runContextFactory;

    static boolean canNotBeEnabled() {
        return Stream.of(HOST, TOKEN, WORKSPACE_ID, REGION, CATALOG, SCHEMA, TABLE).anyMatch(Strings::isNullOrEmpty);
    }

    @Test
    void inlineRecordsEndToEnd() throws Exception {
        WriteRecords task = WriteRecords.builder()
            .id("test-zerobus")
            .type(WriteRecords.class.getName())
            .host(Property.of(HOST))
            .authentication(AuthenticationConfig.builder().token(Property.of(TOKEN)).build())
            .workspaceId(Property.of(WORKSPACE_ID))
            .region(Property.of(REGION))
            .catalog(Property.of(CATALOG))
            .schema(Property.of(SCHEMA))
            .table(Property.of(TABLE))
            .records(
                Property.of(
                    List.of(
                        Map.of("id", 1, "data", "test-kestra-1"),
                        Map.of("id", 2, "data", "test-kestra-2")
                    )
                )
            )
            .build();

        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(2L));
    }
}
