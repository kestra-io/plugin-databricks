package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.Source;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.job.task.SparkPythonTaskSetting;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@Disabled("Need an account to work")
class CreateJobTest {
    private static final String TOKEN = "";
    private static final String HOST = "";
    private static final String CLUSTER_ID = "";
    @Inject
    private RunContextFactory runContextFactory;
    @Test
    void run() throws Exception {
        var task = CreateJob.builder()
            .id(IdUtils.create())
            .type(CreateJob.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(Property.of(TOKEN)).build()
            )
            .host(Property.of(HOST))
            .jobTasks(
                List.of(
                    CreateJob.JobTaskSetting.builder()
                        .existingClusterId(Property.of(CLUSTER_ID))
                        .taskKey(Property.of("taskKey"))
                        .sparkPythonTask(
                            SparkPythonTaskSetting.builder()
                                .sparkPythonTaskSource(Property.of(Source.WORKSPACE))
                                .pythonFile(Property.of("/Shared/hello.py"))
                                .build()
                        )
                        .build()
                )
            )
            .waitForCompletion(Property.of(Duration.ofMinutes(5)))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getJobId(), notNullValue());
        assertThat(output.getRunId(), notNullValue());
    }
}