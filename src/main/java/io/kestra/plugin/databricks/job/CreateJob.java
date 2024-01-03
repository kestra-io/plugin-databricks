package io.kestra.plugin.databricks.job;

import com.databricks.sdk.service.jobs.Task;
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
@Plugin(
    examples = {
        @Example(
            title = "Create a Databricks job, run it, and wait for completion for five minutes",
            code = """
                id: createJob
                type: io.kestra.plugin.databricks.job.CreateJob
                authentication:
                  token: <your-token>
                host: <your-host>
                jobTasks:
                  - existingClusterId: <your-cluster>
                    taskKey: taskKey
                    sparkPythonTask:
                      pythonFile: /Shared/hello.py
                      sparkPythonTaskSource: WORKSPACE
                waitForCompletion: PT5M"""
        )
    }
)
@Schema(title = "Create a Databricks job and run it. Set `waitForCompletion` to the desired maximum duration if you want the task to wait for the job completion (e.g., `PT1H` to wait up to one hour).")
public class CreateJob extends AbstractTask implements RunnableTask<CreateJob.Output> {
    @PluginProperty(dynamic = true)
    @Schema(title = "The name of the job")
    private String jobName;

    @PluginProperty
    @Schema(title = "If set, the task will wait for the job run completion for up to the `waitForCompletion` duration before timing out.")
    private Duration waitForCompletion;

    @NotNull
    @NotEmpty
    @PluginProperty
    @Schema(title = "The job tasks, if multiple tasks are defined you must set `dependsOn` on each task")
    private List<JobTaskSetting> jobTasks;

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<Task> tasks = jobTasks.stream().map(throwFunction(
            setting -> new Task()
                .setDescription(runContext.render(setting.description))
                .setExistingClusterId(runContext.render(setting.existingClusterId))
                .setTaskKey(runContext.render(setting.taskKey))
                .setTimeoutSeconds(setting.timeoutSeconds)
                .setNotebookTask(setting.notebookTask != null ? setting.notebookTask.toNotebookTask(runContext) : null)
                .setDbtTask(setting.dbtTask != null ? setting.dbtTask.toDbtTask(runContext) :  null)
                .setPipelineTask(setting.pipelineTask != null ? setting.pipelineTask.toPipelineTask(runContext) : null)
                .setPythonWheelTask(setting.pythonWheelTask != null ? setting.pythonWheelTask.toPythonWheelTask(runContext) : null)
                .setSparkPythonTask(setting.sparkPythonTask != null ? setting.sparkPythonTask.toSparkPythonTask(runContext) : null)
                .setSqlTask(setting.sqlTask != null ? setting.sqlTask.toSqlTask(runContext) : null)
                .setSparkJarTask(setting.sparkJarTask != null ? setting.sparkJarTask.toSparkJarTask(runContext) : null)
                .setSparkSubmitTask(setting.sparkSubmitTask != null ? setting.sparkSubmitTask.toSparkSubmitTask(runContext) : null)
                .setDependsOn(TaskUtils.dependsOn(setting.dependsOn))
                .setLibraries(setting.libraries != null ? setting.libraries.stream().map(throwFunction(l -> l.toLibrary(runContext))).toList() : null)))
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

        if (waitForCompletion != null) {
            runContext.logger().info("Waiting for job to be terminated or skipped for {}", waitForCompletion);
            workspaceClient.jobs().waitGetRunJobTerminatedOrSkipped(run.getRunId(), waitForCompletion, null);
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
        @Schema(title = "Task description")
        private String description;

        @PluginProperty(dynamic = true)
        @Schema(title = "The identifier of the cluster")
        private String existingClusterId;

        @PluginProperty(dynamic = true)
        @Schema(title = "Task key")
        private String taskKey;

        @PluginProperty
        @Schema(title = "Task timeout in seconds")
        private Long timeoutSeconds;

        @PluginProperty
        @Schema(title = "Notebook task settings")
        private NotebookTaskSetting notebookTask;

        @PluginProperty
        @Schema(title = "DBT task settings")
        private DbtTaskSetting dbtTask;

        @PluginProperty
        @Schema(title = "Spark Submit task settings")
        private SparkSubmitTaskSetting sparkSubmitTask;

        @PluginProperty
        @Schema(title = "SQL task settings")
        private SqlTaskSetting sqlTask;

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
        @Schema(title = "Task dependencies, set this if multiple tasks are defined on the job")
        private List<String> dependsOn;

        @PluginProperty
        @Schema(title = "Task libraries")
        private List<LibrarySetting> libraries;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "The job identifier")
        private Long jobId;

        @Schema(title = "The job URI on the Databricks console")
        private URI jobURI;

        @Schema(title = "The run identifier")
        private Long runId;

        @Schema(title = "The run URI on the Databricks console")
        private URI runURI;
    }
}
