package io.kestra.plugin.databricks.job;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunLifeCycleState;
import com.databricks.sdk.service.jobs.RunResultState;
import com.databricks.sdk.service.jobs.RunState;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class CreateJobOutputTest {
    @Test
    void buildOutputFromTerminatedRun() {
        var jobId = 1L;
        var jobURI = URI.create("https://example.databricks.com/#job/1");
        var runId = 123L;
        var runURI = URI.create("https://example.databricks.com/#job/1/run/123");
        var start = Instant.parse("2026-09-08T10:00:00Z");
        var end = Instant.parse("2026-09-08T10:05:00Z");

        var run = new Run()
            .setRunId(runId)
            .setStartTime(start.toEpochMilli())
            .setEndTime(end.toEpochMilli())
            .setRunDuration(Duration.between(start, end).toMillis())
            .setState(
                new RunState()
                    .setLifeCycleState(RunLifeCycleState.TERMINATED)
                    .setResultState(RunResultState.SUCCESS)
                    .setStateMessage("Run succeeded")
            );

        var output = CreateJob.buildOutput(jobId, jobURI, runId, runURI, run);

        assertThat(output.getJobId(), is(jobId));
        assertThat(output.getJobURI(), is(jobURI));
        assertThat(output.getRunId(), is(runId));
        assertThat(output.getRunURI(), is(runURI));
        assertThat(output.getLifeCycleState(), is("TERMINATED"));
        assertThat(output.getResultState(), is("SUCCESS"));
        assertThat(output.getStateMessage(), is("Run succeeded"));
        assertThat(output.getStartTime(), is(start));
        assertThat(output.getEndTime(), is(end));
        assertThat(output.getDuration(), is(Duration.ofMinutes(5)));
    }

    @Test
    void buildOutputFallsBackToComputedDurationWhenRunDurationMissing() {
        var start = Instant.parse("2026-09-08T10:00:00Z");
        var end = Instant.parse("2026-09-08T10:02:30Z");

        var run = new Run()
            .setRunId(456L)
            .setStartTime(start.toEpochMilli())
            .setEndTime(end.toEpochMilli());

        var output = CreateJob.buildOutput(1L, null, 456L, null, run);

        assertThat(output.getDuration(), is(Duration.ofMinutes(2).plusSeconds(30)));
    }

    @Test
    void buildOutputWithoutRunOnlyKeepsIdentifiers() {
        var jobId = 1L;
        var jobURI = URI.create("https://example.databricks.com/#job/1");
        var runId = 789L;
        var runURI = URI.create("https://example.databricks.com/#job/1/run/789");

        var output = CreateJob.buildOutput(jobId, jobURI, runId, runURI, null);

        assertThat(output.getJobId(), is(jobId));
        assertThat(output.getJobURI(), is(jobURI));
        assertThat(output.getRunId(), is(runId));
        assertThat(output.getRunURI(), is(runURI));
        assertThat(output.getLifeCycleState(), nullValue());
        assertThat(output.getResultState(), nullValue());
        assertThat(output.getStartTime(), nullValue());
        assertThat(output.getEndTime(), nullValue());
        assertThat(output.getDuration(), nullValue());
    }
}
