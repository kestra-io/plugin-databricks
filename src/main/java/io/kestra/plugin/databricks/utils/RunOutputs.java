package io.kestra.plugin.databricks.utils;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunOutput;

import io.kestra.core.runners.RunContext;

public final class RunOutputs {
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

        RunOutput runOutput = workspaceClient.jobs().getRunOutput(taskRunId);
        if (runOutput == null) {
            return;
        }

        if (runOutput.getLogs() != null) {
            runContext.logger().info("Task '{}' logs: {}", taskKey, runOutput.getLogs());
        }
        if (runOutput.getError() != null) {
            runContext.logger().warn("Task '{}' failed: {}", taskKey, runOutput.getError());
        }
    }
}
