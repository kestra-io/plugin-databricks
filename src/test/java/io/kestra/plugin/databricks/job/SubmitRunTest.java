package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.SparkPythonTaskSource;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.cluster.DeleteCluster;
import io.kestra.plugin.databricks.job.task.SparkPythonTaskSetting;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

@MicronautTest
@Disabled("Need an account to work")
class SubmitRunTest {
    private static final String TOKEN = "";
    private static final String HOST = "";
    private static final String CLUSTER_ID = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = SubmitRun.builder()
            .id(IdUtils.create())
            .type(SubmitRun.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(TOKEN).build()
            )
            .host(HOST)
            .runTasks(
                List.of(
                    SubmitRun.RunSubmitTaskSetting.builder()
                        .existingClusterId(CLUSTER_ID)
                        .taskKey("taskKey")
                        .sparkPythonTask(
                            SparkPythonTaskSetting.builder()
                                .sparkPythonTaskSource(SparkPythonTaskSource.WORKSPACE)
                                .pythonFile("/Shared/hello.py")
                                .build()
                        )
                        .build()
                )
            )
            .waitForCompletion(Duration.ofMinutes(5))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getRunId(), notNullValue());
    }
}