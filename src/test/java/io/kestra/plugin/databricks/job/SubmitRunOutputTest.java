package io.kestra.plugin.databricks.job;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.kestra.plugin.databricks.utils.RunStateInfo;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class SubmitRunOutputTest {
    @Test
    void buildOutputMapsRunStateInfoFields() {
        var runId = 123L;
        var runURI = URI.create("https://example.databricks.com/#job/1/run/123");
        var start = Instant.parse("2026-09-08T10:00:00Z");
        var end = Instant.parse("2026-09-08T10:05:00Z");
        var state = new RunStateInfo("TERMINATED", "SUCCESS", "Run succeeded", start, end, Duration.ofMinutes(5));

        var output = SubmitRun.buildOutput(runId, runURI, state);

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
    void buildOutputWithEmptyStateOnlyKeepsIdentifiers() {
        var runId = 789L;
        var runURI = URI.create("https://example.databricks.com/#job/1/run/789");

        var output = SubmitRun.buildOutput(runId, runURI, RunStateInfo.of(null));

        assertThat(output.getRunId(), is(runId));
        assertThat(output.getRunURI(), is(runURI));
        assertThat(output.getLifeCycleState(), nullValue());
        assertThat(output.getResultState(), nullValue());
        assertThat(output.getStartTime(), nullValue());
        assertThat(output.getEndTime(), nullValue());
        assertThat(output.getDuration(), nullValue());
    }
}
