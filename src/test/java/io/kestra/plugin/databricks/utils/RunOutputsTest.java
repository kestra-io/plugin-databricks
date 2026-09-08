package io.kestra.plugin.databricks.utils;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.JobsAPI;
import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunOutput;
import com.databricks.sdk.service.jobs.RunTask;

import io.kestra.core.runners.RunContext;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunOutputsTest {
    private static Run runWithOneTask(long taskRunId) {
        return new Run().setRunId(100L).setTasks(List.of(new RunTask().setTaskKey("t1").setRunId(taskRunId)));
    }

    private static WorkspaceClient mockWorkspaceClient(JobsAPI jobsAPI) {
        var workspaceClient = mock(WorkspaceClient.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        return workspaceClient;
    }

    private static RunContext mockRunContext(Logger logger) {
        var runContext = mock(RunContext.class);
        when(runContext.logger()).thenReturn(logger);
        return runContext;
    }

    @Test
    void logsOutputPerTask() {
        var jobsAPI = mock(JobsAPI.class);
        when(jobsAPI.getRunOutput(1L)).thenReturn(new RunOutput().setLogs("hello"));
        var logger = mock(Logger.class);

        RunOutputs.logTaskOutputs(mockRunContext(logger), mockWorkspaceClient(jobsAPI), runWithOneTask(1L));

        verify(logger).info(eq("Task '{}' logs: {}"), eq("t1"), eq("hello"));
    }

    @Test
    void swallowsOutputRetrievalExceptionAsWarning() {
        var jobsAPI = mock(JobsAPI.class);
        when(jobsAPI.getRunOutput(anyLong())).thenThrow(new RuntimeException("output retrieval is not supported for this task type"));
        var logger = mock(Logger.class);

        // must not throw: a task type that doesn't support output retrieval must never fail the Kestra task
        RunOutputs.logTaskOutputs(mockRunContext(logger), mockWorkspaceClient(jobsAPI), runWithOneTask(1L));

        verify(logger).warn(
            eq("Could not retrieve output for task '{}' (run {}): {}"),
            eq("t1"),
            eq(1L),
            eq("output retrieval is not supported for this task type")
        );
    }

    @Test
    void truncatesOversizedLogs() {
        var jobsAPI = mock(JobsAPI.class);
        var hugeLogs = "x".repeat(20_000);
        when(jobsAPI.getRunOutput(1L)).thenReturn(new RunOutput().setLogs(hugeLogs));
        var logger = mock(Logger.class);

        RunOutputs.logTaskOutputs(mockRunContext(logger), mockWorkspaceClient(jobsAPI), runWithOneTask(1L));

        var captor = forClass(String.class);
        verify(logger).info(eq("Task '{}' logs: {}"), eq("t1"), captor.capture());
        assertThat(captor.getValue().length(), lessThan(hugeLogs.length()));
        assertThat(captor.getValue(), containsString("truncated"));
    }
}
