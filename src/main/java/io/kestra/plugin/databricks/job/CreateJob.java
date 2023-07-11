package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.JobTaskSettings;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.job.task.DbtTaskSetting;
import io.kestra.plugin.databricks.job.task.NotebookTaskSetting;
import io.kestra.plugin.databricks.job.task.PipelineTaskSetting;
import io.kestra.plugin.databricks.job.task.PythonWheelTaskSetting;
import io.kestra.plugin.databricks.job.task.SparkJarTaskSetting;
import io.kestra.plugin.databricks.job.task.SparkPythonTaskSetting;
import io.kestra.plugin.databricks.job.task.SparkSubmitTaskSetting;
import io.kestra.plugin.databricks.job.task.SqlTaskSetting;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = """
                id: createJob
                type: io.kestra.plugin.databricks.job.CreateJob
                token: <your-token>
                host: <your-host>
                jobTaskSettings:
                  - existingClusterId: <your-cluster>
                    taskKey: taskKey
                    sparkPythonTaskSetting:
                      pythonFile: /Shared/hello.py
                      sparkPythonTaskSource: WORKSPACE
                waitJobTerminatedOrSkipped: PT5M
                """
        )
    }
)
public class CreateJob extends AbstractTask implements RunnableTask<CreateJob.Output> {
    @PluginProperty(dynamic = true)
    private String jobName;

    @PluginProperty
    private Duration waitJobTerminatedOrSkipped;

    @NotNull
    @NotEmpty
    @PluginProperty
    private List<JobTaskSetting> jobTaskSettings;

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<JobTaskSettings> tasks = jobTaskSettings.stream().map(throwFunction(
            setting -> new JobTaskSettings()
                .setDescription(runContext.render(setting.description))
                .setExistingClusterId(runContext.render(setting.existingClusterId))
                .setTaskKey(runContext.render(setting.taskKey))
                .setTimeoutSeconds(setting.timeoutSeconds)
                .setNotebookTask(setting.notebookTaskSetting != null ? setting.notebookTaskSetting.toNotebookTask(runContext) : null)
                .setDbtTask(setting.dbtTaskSetting != null ? setting.dbtTaskSetting.toDbtTask(runContext) :  null)
                .setPipelineTask(setting.pipelineTaskSetting != null ? setting.pipelineTaskSetting.toPipelineTask(runContext) : null)
                .setPythonWheelTask(setting.pythonWheelTaskSetting != null ? setting.pythonWheelTaskSetting.toPythonWheelTask(runContext) : null)
                .setSparkPythonTask(setting.sparkPythonTaskSetting != null ? setting.sparkPythonTaskSetting.toSparkPythonTask(runContext) : null)
                .setSqlTask(setting.sqlTaskSetting != null ? setting.sqlTaskSetting.toSqlTask(runContext) : null)
                .setSparkJarTask(setting.sparkJarTaskSetting != null ? setting.sparkJarTaskSetting.toSparkJarTask(runContext) : null)
                .setSparkSubmitTask(setting.sparkSubmitTaskSetting != null ? setting.sparkSubmitTaskSetting.toSparkSubmitTask(runContext) : null)))
            .toList();

        var workspaceClient = workspaceClient(runContext);
        var job = workspaceClient.jobs().create(new com.databricks.sdk.service.jobs.CreateJob()
            .setName(runContext.render(jobName))
            .setTasks(tasks)
        );
        var jobURI = URI.create(workspaceClient.config().getHost() + "/#job/" + job.getJobId());
        runContext.logger().info("Job created: {}", jobURI);

        var run = workspaceClient.jobs().runNow(job.getJobId()).get();
        var runURI = URI.create(workspaceClient.config().getHost() + "/#job/" + job.getJobId() + "/run/" + run.getRunId());
        runContext.logger().info("Run submitted: {}", run.getRunId());

        if (waitJobTerminatedOrSkipped != null) {
            runContext.logger().info("Waiting for job to be terminated or skipped for {}", waitJobTerminatedOrSkipped);
            workspaceClient.jobs().waitGetRunJobTerminatedOrSkipped(run.getRunId(), waitJobTerminatedOrSkipped, null);
            //FIXME fail with Retrieving the output of runs with multiple tasks is not supported. Please retrieve the output of each individual task run instead.
//            runContext.logger().info(workspaceClient.jobs().getRunOutput(run.getRunId()).getLogs());
            //TODO when finished, we have a lot of info that we can send as outputs and metrics
        }

        return Output.builder()
            .jobId(job.getJobId()).jobURI(jobURI)
            .runId(run.getRunId()).runURI(runURI)
            .build();
    }

    @Builder
    @Getter
    public static class JobTaskSetting {
        @PluginProperty(dynamic = true)
        private String description;

        @PluginProperty(dynamic = true)
        private String existingClusterId;

        @PluginProperty(dynamic = true)
        private String taskKey;

        @PluginProperty
        private Long timeoutSeconds;

        @PluginProperty
        private NotebookTaskSetting notebookTaskSetting;

        @PluginProperty
        private DbtTaskSetting dbtTaskSetting;

        @PluginProperty
        private SparkSubmitTaskSetting sparkSubmitTaskSetting;

        @PluginProperty
        private SqlTaskSetting sqlTaskSetting;

        @PluginProperty
        private SparkJarTaskSetting sparkJarTaskSetting;

        @PluginProperty
        private SparkPythonTaskSetting sparkPythonTaskSetting;

        @PluginProperty
        private PythonWheelTaskSetting pythonWheelTaskSetting;

        @PluginProperty
        private PipelineTaskSetting pipelineTaskSetting;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        private Long jobId;
        private URI jobURI;
        private Long runId;
        private URI runURI;
    }
}
