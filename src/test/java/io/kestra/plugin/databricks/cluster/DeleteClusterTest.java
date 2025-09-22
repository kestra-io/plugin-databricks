package io.kestra.plugin.databricks.cluster;

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
import static org.hamcrest.Matchers.*;

@KestraTest
@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled because it requires Databricks secrets: host, token"
)
class DeleteClusterTest {
    protected static final String HOST = System.getenv("DATABRICKS_HOST");
    protected static final String TOKEN = System.getenv("DATABRICKS_TOKEN");

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var createTask = CreateCluster.builder()
            .id(IdUtils.create())
            .type(CreateCluster.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder()
                    .token(Property.ofValue(TOKEN))
                    .build()
            )
            .host(Property.ofValue(HOST))
            .clusterName(Property.ofValue("kestra-test-" + IdUtils.create()))
            .nodeTypeId(Property.ofValue("n2-highmem-4"))
            .numWorkers(Property.ofValue(1L))
            .sparkVersion(Property.ofValue("13.0.x-scala2.12"))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, createTask, ImmutableMap.of());
        var createOutput = createTask.run(runContext);

        assertThat(createOutput.getClusterId(), notNullValue());

        var deleteTask = DeleteCluster.builder()
            .id(IdUtils.create())
            .type(DeleteCluster.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder()
                    .token(Property.ofValue(TOKEN))
                    .build()
            )
            .host(Property.ofValue(HOST))
            .clusterId(Property.ofValue(createOutput.getClusterId()))
            .build();

        var deleteContext = TestsUtils.mockRunContext(runContextFactory, deleteTask, ImmutableMap.of());
        var deleteOutput = deleteTask.run(deleteContext);

        assertThat(deleteOutput, nullValue());
    }

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(HOST) || Strings.isNullOrEmpty(TOKEN);
    }
}