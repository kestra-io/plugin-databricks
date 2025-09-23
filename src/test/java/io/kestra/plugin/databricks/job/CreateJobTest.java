package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.Source;
import com.google.api.client.util.Strings;
import com.google.common.collect.ImmutableMap;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.job.task.SparkPythonTaskSetting;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import java.time.Duration;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

@KestraTest
@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled because it requires Databricks secrets: host, token, clusterId"
)
class CreateJobTest {
    protected static final String CLUSTER_ID = System.getenv("DATABRICKS_CLUSTER_ID");
    protected static final String HOST = System.getenv("DATABRICKS_HOST");
    protected static final String TOKEN = System.getenv("DATABRICKS_TOKEN");

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void run() throws Exception {
        var task = CreateJob.builder()
            .id(IdUtils.create())
            .type(CreateJob.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(Property.ofValue(TOKEN)).build()
            )
            .host(Property.ofValue(HOST))
            .jobTasks(
                List.of(
                    CreateJob.JobTaskSetting.builder()
                        .existingClusterId(Property.ofValue(CLUSTER_ID))
                        .taskKey(Property.ofValue("taskKey"))
                        .sparkPythonTask(
                            SparkPythonTaskSetting.builder()
                                .sparkPythonTaskSource(Property.ofValue(Source.WORKSPACE))
                                .pythonFile(Property.ofValue("/Shared/hello.py"))
                                .build()
                        )
                        .build()
                )
            )
            .waitForCompletion(Property.ofValue(Duration.ofMinutes(5)))
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output.getJobId(), notNullValue());
        assertThat(output.getRunId(), notNullValue());
    }

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(HOST) || Strings.isNullOrEmpty(TOKEN) || Strings.isNullOrEmpty(CLUSTER_ID) ;
    }
}