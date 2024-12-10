package io.kestra.plugin.databricks.cluster;

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
import static org.hamcrest.Matchers.*;

@KestraTest
@Disabled("Need an account to work")
class DeleteClusterTest {
    private static final String TOKEN = "";
    private static final String HOST = "";
    private static final String CLUSTER_ID = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = DeleteCluster.builder()
            .id(IdUtils.create())
            .type(DeleteCluster.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(Property.of(TOKEN)).build()
            )
            .host(Property.of(HOST))
            .clusterId(Property.of(CLUSTER_ID))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output, nullValue());
    }
}