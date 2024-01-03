package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.SubmitTask;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.job.task.*;
import io.kestra.plugin.databricks.utils.TaskUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(examples = {
    @Example(
        title = "Submit a Databricks run and wait up to 5 minutes for its completion",
        code = """
            id: submitRun
            type: io.kestra.plugin.databricks.job.SubmitRun
            authentication:
              token: <your-token>
            host: <your-host>
            runTasks:
              - existingClusterId: <your-cluster>
                taskKey: taskKey
                sparkPythonTask:
                  pythonFile: /Shared/hello.py
                  sparkPythonTaskSource: WORKSPACE
            waitForCompletion: PT5M"""
    )
})
@Schema(title = "Submit a Databricks run. Optionally, set `waitForCompletion` to a desired maximum duration to wait for the run completion.")
public class SubmitRun  extends AbstractTask implements RunnableTask<SubmitRun.Output> {
    @PluginProperty(dynamic = true)
    @Schema(title = "The name of the run")
    private String runName;

    @PluginProperty
    @Schema(title = "If set, the task will wait for the run completion")
    private Duration waitForCompletion;

    @NotNull
    @NotEmpty
    @PluginProperty
    @Schema(title = "The run tasks, if multiple tasks are defined you must set `dependsOn` on each task")
    private List<RunSubmitTaskSetting> runTasks;

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<SubmitTask> tasks = runTasks.stream().map(throwFunction(setting ->
            new SubmitTask()
                .setExistingClusterId(runContext.render(setting.existingClusterId))
                .setTaskKey(runContext.render(setting.taskKey))
                .setTimeoutSeconds(setting.timeoutSeconds)
                .setNotebookTask(setting.notebookTask != null ? setting.notebookTask.toNotebookTask(runContext) : null)
                .setPipelineTask(setting.pipelineTask != null ? setting.pipelineTask.toPipelineTask(runContext) : null)
                .setSparkJarTask(setting.sparkJarTask != null ? setting.sparkJarTask.toSparkJarTask(runContext) : null)
                .setSparkSubmitTask(setting.sparkSubmitTask != null ? setting.sparkSubmitTask.toSparkSubmitTask(runContext) : null)
                .setSparkPythonTask(setting.sparkPythonTask != null ? setting.sparkPythonTask.toSparkPythonTask(runContext) : null)
                .setPythonWheelTask(setting.pythonWheelTask != null ? setting.pythonWheelTask.toPythonWheelTask(runContext) : null)
                .setDependsOn(TaskUtils.dependsOn(setting.dependsOn))
                .setLibraries(setting.libraries != null ? setting.libraries.stream().map(throwFunction(l -> l.toLibrary(runContext))).toList() : null)))
            .toList();

        var workspaceClient = workspaceClient(runContext);

        var response = workspaceClient.jobs().submit(new com.databricks.sdk.service.jobs.SubmitRun()
            .setTasks(tasks)
            .setRunName(runContext.render(runName)))
            .getResponse();

        var run = workspaceClient.jobs().getRun(response.getRunId());
        var runURI = URI.create(workspaceClient.config().getHost() + "/#job/" + run.getJobId() + "/run/" + run.getRunId());
        runContext.logger().info("Run submitted: {}", runURI);

        if (waitForCompletion != null) {
            runContext.logger().info("Waiting for run to be terminated or skipped for {}", waitForCompletion);
            workspaceClient.jobs().waitGetRunJobTerminatedOrSkipped(response.getRunId(), waitForCompletion, null);
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
        @Schema(title = "Notebook task settings")
        private NotebookTaskSetting notebookTask;

        @PluginProperty
        @Schema(title = "Spark Submit task settings")
        private SparkSubmitTaskSetting sparkSubmitTask;

        @PluginProperty
        @Schema(title = "Spark JAR task settings")
        private SparkJarTaskSetting sparkJarTask;

        @PluginProperty
        @Schema(title = "Spark Python task settings")
        private SparkPythonTaskSetting sparkPythonTask;

        @PluginProperty
        @Schema(title = "Python Wheel task settings")
        private PythonWheelTaskSetting pythonWheelTask;

        @PluginProperty
        @Schema(title = "Pipeline task settings")
        private PipelineTaskSetting pipelineTask;

        @PluginProperty
        @Schema(title = "Task dependencies, set this if multiple tasks are defined on the run")
        private List<String> dependsOn;

        @PluginProperty
        @Schema(title = "Task libraries")
        private List<LibrarySetting> libraries;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The run identifier")
        private Long runId;

        @Schema(title = "The run URI on the Databricks console")
        private URI runURI;
    }
}
