package io.kestra.plugin.databricks.utils;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunLifeCycleState;
import com.databricks.sdk.service.jobs.RunResultState;
import com.databricks.sdk.service.jobs.RunState;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunStateInfoTest {
    @Test
    void ofExtractsStateAndTiming() {
        var start = Instant.parse("2026-09-08T10:00:00Z");
        var end = Instant.parse("2026-09-08T10:05:00Z");

        var run = new Run()
            .setStartTime(start.toEpochMilli())
            .setEndTime(end.toEpochMilli())
            .setRunDuration(Duration.between(start, end).toMillis())
            .setState(
                new RunState()
                    .setLifeCycleState(RunLifeCycleState.TERMINATED)
                    .setResultState(RunResultState.SUCCESS)
                    .setStateMessage("Run succeeded")
            );

        var state = RunStateInfo.of(run);

        assertThat(state.lifeCycleState(), is("TERMINATED"));
        assertThat(state.resultState(), is("SUCCESS"));
        assertThat(state.stateMessage(), is("Run succeeded"));
        assertThat(state.startTime(), is(start));
        assertThat(state.endTime(), is(end));
        assertThat(state.duration(), is(Duration.ofMinutes(5)));
    }

    @Test
    void ofFallsBackToComputedDurationWhenRunDurationMissing() {
        var start = Instant.parse("2026-09-08T10:00:00Z");
        var end = Instant.parse("2026-09-08T10:02:30Z");

        var run = new Run().setStartTime(start.toEpochMilli()).setEndTime(end.toEpochMilli());

        assertThat(RunStateInfo.of(run).duration(), is(Duration.ofMinutes(2).plusSeconds(30)));
    }

    @Test
    void ofNullRunIsEmpty() {
        var state = RunStateInfo.of(null);

        assertThat(state.lifeCycleState(), nullValue());
        assertThat(state.resultState(), nullValue());
        assertThat(state.duration(), nullValue());
    }

    @Test
    void isUnsuccessfulFalseForSuccessStates() {
        assertThat(new RunStateInfo(null, "SUCCESS", null, null, null, null).isUnsuccessful(), is(false));
        assertThat(new RunStateInfo(null, "SUCCESS_WITH_FAILURES", null, null, null, null).isUnsuccessful(), is(false));
    }

    @Test
    void isUnsuccessfulFalseWhenRunHasNotTerminatedYet() {
        assertThat(new RunStateInfo(null, null, null, null, null, null).isUnsuccessful(), is(false));
    }

    @Test
    void isUnsuccessfulTrueForNonSuccessTerminalStates() {
        assertThat(new RunStateInfo(null, "FAILED", null, null, null, null).isUnsuccessful(), is(true));
        assertThat(new RunStateInfo(null, "TIMEDOUT", null, null, null, null).isUnsuccessful(), is(true));
        assertThat(new RunStateInfo(null, "CANCELED", null, null, null, null).isUnsuccessful(), is(true));
    }

    @Test
    void isUnsuccessfulTrueForSkippedOrInternalErrorLifecycleWithNullResultState() {
        assertThat(new RunStateInfo("SKIPPED", null, null, null, null, null).isUnsuccessful(), is(true));
        assertThat(new RunStateInfo("INTERNAL_ERROR", null, null, null, null, null).isUnsuccessful(), is(true));
    }

    @Test
    void throwIfUnsuccessfulThrowsForSkippedRunWithNullResultState() {
        var runURI = URI.create("https://example.databricks.com/#job/1/run/1");
        var state = new RunStateInfo("SKIPPED", null, "Run was skipped", null, null, null);

        var e = assertThrows(IllegalStateException.class, () -> state.throwIfUnsuccessful(runURI));

        assertThat(e.getMessage(), containsString(runURI.toString()));
    }

    @Test
    void throwIfUnsuccessfulThrowsForInternalErrorRunWithNullResultState() {
        var runURI = URI.create("https://example.databricks.com/#job/1/run/1");
        var state = new RunStateInfo("INTERNAL_ERROR", null, "Cluster failed to launch", null, null, null);

        var e = assertThrows(IllegalStateException.class, () -> state.throwIfUnsuccessful(runURI));

        assertThat(e.getMessage(), containsString(runURI.toString()));
    }

    @Test
    void throwIfUnsuccessfulIsNoOpOnSuccess() {
        new RunStateInfo(null, "SUCCESS", null, null, null, null)
            .throwIfUnsuccessful(URI.create("https://example.databricks.com/#job/1/run/1"));
    }

    @Test
    void throwIfUnsuccessfulThrowsWithRunURIResultStateAndMessage() {
        var runURI = URI.create("https://example.databricks.com/#job/1/run/1");
        var state = new RunStateInfo(null, "FAILED", "boom", null, null, null);

        var e = assertThrows(IllegalStateException.class, () -> state.throwIfUnsuccessful(runURI));

        assertThat(e.getMessage(), containsString(runURI.toString()));
        assertThat(e.getMessage(), containsString("resultState=FAILED"));
        assertThat(e.getMessage(), containsString("message=boom"));
    }
}
