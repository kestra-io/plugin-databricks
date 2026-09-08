package io.kestra.plugin.databricks.utils;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunOutput;

import io.kestra.core.runners.RunContext;

public final class RunOutputs {
    private static final int MAX_LOG_CHARS = 10_000;

    private RunOutputs() {
        //utility class pattern
    }

    // A run submitted through the multi-task tasks/jobTasks format never exposes its output at the top
    // level: the Databricks API only supports retrieving it per individual task run.
    public static void logTaskOutputs(RunContext runContext, WorkspaceClient workspaceClient, Run run) {
        var taskRuns = run.getTasks();
        if (taskRuns == null || taskRuns.isEmpty()) {
            logTaskOutput(runContext, workspaceClient, run.getRunName(), run.getRunId());
            return;
        }

        for (var taskRun : taskRuns) {
            logTaskOutput(runContext, workspaceClient, taskRun.getTaskKey(), taskRun.getRunId());
        }
    }

    private static void logTaskOutput(RunContext runContext, WorkspaceClient workspaceClient, String taskKey, Long taskRunId) {
        if (taskRunId == null) {
            return;
        }

        RunOutput runOutput;
        try {
            runOutput = workspaceClient.jobs().getRunOutput(taskRunId);
        } catch (RuntimeException e) {
            // some task types (e.g. spark_submit_task, spark_jar_task, pipeline_task, run_job_task) don't
            // support output retrieval, and the API can also fail transiently; the Databricks run itself
            // already succeeded, so a best-effort log fetch must never fail the Kestra task
            runContext.logger().warn("Could not retrieve output for task '{}' (run {}): {}", taskKey, taskRunId, e.getMessage());
            return;
        }

        if (runOutput == null) {
            return;
        }

        if (runOutput.getLogs() != null) {
            runContext.logger().info("Task '{}' logs: {}", taskKey, truncate(runOutput.getLogs()));
        }
        if (runOutput.getError() != null) {
            runContext.logger().warn("Task '{}' failed: {}", taskKey, truncate(runOutput.getError()));
        }
    }

    private static String truncate(String text) {
        if (text.length() <= MAX_LOG_CHARS) {
            return text;
        }
        return text.substring(0, MAX_LOG_CHARS) + "… (truncated, " + text.length() + " chars total)";
    }
}
