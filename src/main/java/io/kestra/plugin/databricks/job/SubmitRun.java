package io.kestra.plugin.databricks.job;

import java.net.URI;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongFunction;

import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunLifeCycleState;
import com.databricks.sdk.service.jobs.RunResultState;
import com.databricks.sdk.service.jobs.RunState;
import com.databricks.sdk.service.jobs.SubmitTask;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.job.task.*;
import io.kestra.plugin.databricks.utils.IdempotencyTokens;
import io.kestra.plugin.databricks.utils.TaskUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Submit a Databricks run and wait up to 5 minutes for its completion. A worker-loss resubmit adopts the same Databricks run instead of launching a duplicate one.",
            full = true,
            code = """
                id: databricks_job_submit_run
                namespace: company.team

                tasks:
                  - id: submit_run
                    type: io.kestra.plugin.databricks.job.SubmitRun
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    runTasks:
                      - existingClusterId: <your-cluster>
                        taskKey: pysparkTask
                        sparkPythonTask:
                          pythonFile: /Shared/hello.py
                          sparkPythonTaskSource: WORKSPACE
                    waitForCompletion: PT5M
                """
        )
    }
)
@Schema(
    title = "Submit a Databricks run",
    description = """
        Submits one or more tasks as an ad-hoc run; optionally waits up to waitForCompletion for terminal state.
        The submission is idempotent: if the Kestra worker running this task is lost and the task is resubmitted, the already in-flight or already completed Databricks run is adopted instead of a duplicate run being launched. A plain task retry after a failure still creates a genuinely new run.
        """
)
public class SubmitRun extends AbstractTask implements RunnableTask<SubmitRun.Output> {
    // Databricks terminal result states that mean "this run belongs to a previous, unsuccessful attempt of the same idempotency token"
    private static final Set<RunResultState> FAILED_RESULT_STATES = EnumSet.complementOf(EnumSet.of(RunResultState.SUCCESS, RunResultState.SUCCESS_WITH_FAILURES));
    private static final Set<RunLifeCycleState> FRESH_LIFECYCLE_STATES = EnumSet.of(RunLifeCycleState.PENDING, RunLifeCycleState.QUEUED, RunLifeCycleState.BLOCKED);
    // Caps the idempotency token generation walk so a permanently broken taskrun fails fast instead of looping
    static final int MAX_IDEMPOTENCY_GENERATIONS = 10;

    @Schema(title = "Run name")
    @PluginProperty(group = "advanced")
    private Property<String> runName;

    @Schema(title = "Wait for completion", description = "If set, waits up to the given duration (e.g., PT30M) for the run to finish")
    @PluginProperty(group = "execution")
    private Property<Duration> waitForCompletion;

    @NotNull
    @NotEmpty
    @PluginProperty(group = "main")
    @Schema(title = "Run tasks", description = "Task definitions for this run; set dependsOn when multiple tasks are present")
    private List<RunSubmitTaskSetting> runTasks;

    @Schema(
        title = "Idempotency token seed",
        description = """
            Seed used to derive the Databricks idempotency token attached to the run submission, so that a worker-loss resubmit adopts the already in-flight or already completed run instead of launching a duplicate one.
            Defaults to this task run's Kestra identifier, which is unique per task execution attempt: a plain Kestra retry after a failed run still creates a new Databricks run, while a resubmit of the same attempt after a worker crash adopts the original run.
            Set this only if you need to key deduplication on something other than the task run itself. Two different executions sharing the same override value will cause the second one to adopt the first one's run.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<String> idempotencyToken;

    @Override
    public Output run(RunContext runContext) throws Exception {
        List<SubmitTask> tasks = runTasks.stream().map(
            throwFunction(
                setting -> new SubmitTask()
                    .setExistingClusterId(runContext.render(setting.existingClusterId))
                    .setTaskKey(runContext.render(setting.taskKey))
                    .setTimeoutSeconds(setting.timeoutSeconds)
                    .setNotebookTask(setting.notebookTask != null ? setting.notebookTask.toNotebookTask(runContext) : null)
                    .setPipelineTask(setting.pipelineTask != null ? setting.pipelineTask.toPipelineTask(runContext) : null)
                    .setRunJobTask(setting.runJobTask != null ? setting.runJobTask.toRunJobTask(runContext) : null)
                    .setSparkJarTask(setting.sparkJarTask != null ? setting.sparkJarTask.toSparkJarTask(runContext) : null)
                    .setSparkSubmitTask(setting.sparkSubmitTask != null ? setting.sparkSubmitTask.toSparkSubmitTask(runContext) : null)
                    .setSparkPythonTask(setting.sparkPythonTask != null ? setting.sparkPythonTask.toSparkPythonTask(runContext) : null)
                    .setPythonWheelTask(setting.pythonWheelTask != null ? setting.pythonWheelTask.toPythonWheelTask(runContext) : null)
                    .setDependsOn(TaskUtils.dependsOn(setting.dependsOn))
                    .setLibraries(setting.libraries != null ? setting.libraries.stream().map(throwFunction(l -> l.toLibrary(runContext))).toList() : null)
            )
        )
            .toList();

        var workspaceClient = workspaceClient(runContext);
        var rHost = workspaceClient.config().getHost();
        var rIdempotencySeed = runContext.render(idempotencyToken).as(String.class).orElse(runContext.taskRunInfo().taskRunId());
        var rRunName = runContext.render(runName).as(String.class).orElse(null);

        var outcome = submitIdempotent(
            rIdempotencySeed,
            rHost,
            token -> workspaceClient.jobs().submit(
                new com.databricks.sdk.service.jobs.SubmitRun()
                    .setTasks(tasks)
                    .setRunName(rRunName)
                    .setIdempotencyToken(token)
            )
                .getResponse()
                .getRunId(),
            workspaceClient.jobs()::getRun
        );

        var run = outcome.run();
        var runURI = runURI(rHost, run.getJobId(), run.getRunId());
        if (outcome.adopted()) {
            runContext.logger().info("Adopted existing Databricks run (state: {}): {}", run.getState(), runURI);
        } else {
            runContext.logger().info("Run submitted: {}", runURI);
        }

        if (waitForCompletion != null) {
            var time = runContext.render(waitForCompletion).as(Duration.class).orElseThrow();
            runContext.logger().info("Waiting for run to be terminated or skipped for {}", time);
            workspaceClient.jobs().waitGetRunJobTerminatedOrSkipped(run.getRunId(), time, null);
            //FIXME fail with Retrieving the output of runs with multiple tasks is not supported. Please retrieve the output of each individual task run instead.
            //            runContext.logger().info(workspaceClient.jobs().getRunOutput(response.getRunId()).getLogs());
            //TODO when finished, we have a lot of info that we can send as outputs and metrics
        }
        return Output.builder().runURI(runURI).runId(run.getRunId()).build();
    }

    private static URI runURI(String host, Long jobId, Long runId) {
        return URI.create(host + "/#job/" + jobId + "/run/" + runId);
    }

    /**
     * Walks a deterministic sequence of idempotency tokens derived from {@code tokenSeed} until a run
     * that does not belong to a previously failed attempt is found. This makes {@code submit} adopt an
     * in-flight or already-succeeded run (worker-loss resubmit) while still letting a genuine task retry
     * obtain a new run once every prior generation is observed as failed.
     * Kept free of any Databricks client so the walk is unit-testable without a live workspace.
     */
    static IdempotentSubmitOutcome submitIdempotent(String tokenSeed, String host, Function<String, Long> submit, LongFunction<Run> getRun) {
        Run lastFailedRun = null;
        for (var generation = 0; generation < MAX_IDEMPOTENCY_GENERATIONS; generation++) {
            var token = IdempotencyTokens.token(tokenSeed, generation);
            Long runId;
            try {
                runId = submit.apply(token);
            } catch (NotFound e) {
                // the run previously submitted with this token was deleted in Databricks; advance and retry
                continue;
            }

            var run = getRun.apply(runId);
            if (isFailedTerminal(run.getState())) {
                lastFailedRun = run;
                continue;
            }

            return new IdempotentSubmitOutcome(run, generation, !isFreshLifecycle(run.getState()));
        }

        throw new IllegalStateException(
            "Too many prior failed Databricks runs (%d) found for task run '%s'; last run: %s. This taskrun may be stuck in a bad state — check the Databricks Jobs UI."
                .formatted(
                    MAX_IDEMPOTENCY_GENERATIONS,
                    tokenSeed,
                    lastFailedRun != null ? runURI(host, lastFailedRun.getJobId(), lastFailedRun.getRunId()) : "none (every submission returned a deleted-token error)"
                )
        );
    }

    private static boolean isFailedTerminal(RunState state) {
        if (state == null) {
            return false;
        }
        var lifecycle = state.getLifeCycleState();
        if (lifecycle == RunLifeCycleState.INTERNAL_ERROR || lifecycle == RunLifeCycleState.SKIPPED) {
            return true;
        }
        if (lifecycle != RunLifeCycleState.TERMINATED) {
            // still queued/running/blocked/terminating: not a failure, adopt it
            return false;
        }
        var result = state.getResultState();
        return result != null && FAILED_RESULT_STATES.contains(result);
    }

    private static boolean isFreshLifecycle(RunState state) {
        return state != null && FRESH_LIFECYCLE_STATES.contains(state.getLifeCycleState());
    }

    record IdempotentSubmitOutcome(Run run, int generation, boolean adopted) {
    }

    @Builder
    @Getter
    public static class RunSubmitTaskSetting {
        @PluginProperty(dynamic = true)
        private String existingClusterId;

        @PluginProperty(dynamic = true)
        private String taskKey;

        @PluginProperty
        @Schema(title = "Task timeout (seconds)")
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
        @Schema(title = "Run job task settings")
        private RunJobTaskSetting runJobTask;

        @PluginProperty
        @Schema(title = "Task dependencies", description = "List of upstream taskKeys when multiple tasks run in the same submission")
        private List<String> dependsOn;

        @PluginProperty
        @Schema(title = "Task libraries")
        private List<LibrarySetting> libraries;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Run identifier")
        private Long runId;

        @Schema(title = "Run console URI")
        private URI runURI;
    }
}
