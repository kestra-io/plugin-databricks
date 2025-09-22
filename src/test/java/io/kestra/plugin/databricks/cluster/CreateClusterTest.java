package io.kestra.plugin.databricks.cluster;

import com.databricks.sdk.service.compute.State;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractDatabricksTest;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
class CreateClusterTest extends AbstractDatabricksTest {
    private static final String CLUSTER_NAME = "kestra-test";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = CreateCluster.builder()
            .id(IdUtils.create())
            .type(CreateCluster.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(Property.ofValue(TOKEN)).build()
            )
            .host(Property.ofValue(HOST))
            .clusterName(Property.ofValue(CLUSTER_NAME))
            .nodeTypeId(Property.ofValue("n2-highmem-4"))
            .numWorkers(Property.ofValue(1L))
            .sparkVersion(Property.ofValue("13.0.x-scala2.12"))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getClusterId(), notNullValue());
        assertThat(output.getClusterState(), is(State.RUNNING));
    }
}