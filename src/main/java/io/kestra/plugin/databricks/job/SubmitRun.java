package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.RunSubmitTaskSettings;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.job.task.NotebookTaskSetting;
import io.kestra.plugin.databricks.job.task.PipelineTaskSetting;
import io.kestra.plugin.databricks.job.task.PythonWheelTaskSetting;
import io.kestra.plugin.databricks.job.task.SparkJarTaskSetting;
import io.kestra.plugin.databricks.job.task.SparkPythonTaskSetting;
import io.kestra.plugin.databricks.job.task.SparkSubmitTaskSetting;
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
@Plugin(examples = {
    @Example(
        code = """
            id: submitRun
            type: io.kestra.plugin.databricks.job.SubmitRun
                token: <your-token>
                host: <your-host>
            runSubmitTaskSettings:
              - existingClusterId: <your-cluster>
                taskKey: taskKey
                sparkPythonTaskSetting:
                  pythonFile: /Shared/hello.py
                  sparkPythonTaskSource: WORKSPACE
            waitRunTerminatedOrSkipped: PT5M
            """
    )
})
public class SubmitRun  extends AbstractTask implements RunnableTask<SubmitRun.Output> {
    @PluginProperty(dynamic = true)
    private String runName;

    @PluginProperty
    private Duration waitRunTerminatedOrSkipped;

    @NotNull
    @NotEmpty
    @PluginProperty
    private List<RunSubmitTaskSetting> runSubmitTaskSettings;

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<RunSubmitTaskSettings> tasks = runSubmitTaskSettings.stream().map(throwFunction(setting ->
            new RunSubmitTaskSettings()
                .setExistingClusterId(runContext.render(setting.existingClusterId))
                .setTaskKey(runContext.render(setting.taskKey))
                .setTimeoutSeconds(setting.timeoutSeconds)
                .setNotebookTask(setting.notebookTaskSetting != null ? setting.notebookTaskSetting.toNotebookTask(runContext) : null)
                .setPipelineTask(setting.pipelineTaskSetting != null ? setting.pipelineTaskSetting.toPipelineTask(runContext) : null)
                .setSparkJarTask(setting.sparkJarTaskSetting != null ? setting.sparkJarTaskSetting.toSparkJarTask(runContext) : null)
                .setSparkSubmitTask(setting.sparkSubmitTaskSetting != null ? setting.sparkSubmitTaskSetting.toSparkSubmitTask(runContext) : null)
                .setSparkPythonTask(setting.sparkPythonTaskSetting != null ? setting.sparkPythonTaskSetting.toSparkPythonTask(runContext) : null)
                .setPythonWheelTask(setting.pythonWheelTaskSetting != null ? setting.pythonWheelTaskSetting.toPythonWheelTask(runContext) : null)))
            .toList();

        var workspaceClient = workspaceClient(runContext);

        var response = workspaceClient.jobs().submit(new com.databricks.sdk.service.jobs.SubmitRun()
            .setTasks(tasks)
            .setRunName(runContext.render(runName)))
            .getResponse();

        var run = workspaceClient.jobs().getRun(response.getRunId());
        var runURI = URI.create(workspaceClient.config().getHost() + "/#job/" + run.getJobId() + "/run/" + run.getRunId());
        runContext.logger().info("Run submitted: {}", runURI);

        if (waitRunTerminatedOrSkipped != null) {
            runContext.logger().info("Waiting for run to be terminated or skipped for {}", waitRunTerminatedOrSkipped);
            workspaceClient.jobs().waitGetRunJobTerminatedOrSkipped(response.getRunId(), waitRunTerminatedOrSkipped, null);
            //FIXME fail with Retrieving the output of runs with multiple tasks is not supported. Please retrieve the output of each individual task run instead.
//            runContext.logger().info(workspaceClient.jobs().getRunOutput(response.getRunId()).getLogs());
            //TODO when finished, we have a lot of info that we can send as outputs and metrics
        }
        return Output.builder().runURI(runURI).runId(response.getRunId()).build();
    }

    @Builder
    @Getter
    public static class RunSubmitTaskSetting {
        @PluginProperty(dynamic = true)
        private String existingClusterId;

        @PluginProperty(dynamic = true)
        private String taskKey;

        @PluginProperty
        private Long timeoutSeconds;

        @PluginProperty
        private NotebookTaskSetting notebookTaskSetting;

        @PluginProperty
        private SparkSubmitTaskSetting sparkSubmitTaskSetting;

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
        private Long runId;
        private URI runURI;
    }
}
