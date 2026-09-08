package io.kestra.plugin.databricks.utils;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunResultState;

/**
 * Snapshot of a Databricks {@link Run}'s state and timing, extracted once so both SubmitRun and CreateJob
 * can populate their own (differently shaped) Output from the same logic.
 */
public record RunStateInfo(String lifeCycleState, String resultState, String stateMessage, Instant startTime, Instant endTime, Duration duration) {
    private static final RunStateInfo EMPTY = new RunStateInfo(null, null, null, null, null, null);
    // Defined independently from any idempotency-related result-state set: this only decides whether the
    // Kestra task itself should be reported as failed once the Databricks run has terminated.
    private static final Set<String> SUCCESS_RESULT_STATES = Set.of(RunResultState.SUCCESS.name(), RunResultState.SUCCESS_WITH_FAILURES.name());

    public static RunStateInfo of(Run run) {
        if (run == null) {
            return EMPTY;
        }

        String lifeCycleState = null;
        String resultState = null;
        String stateMessage = null;
        var state = run.getState();
        if (state != null) {
            lifeCycleState = state.getLifeCycleState() != null ? state.getLifeCycleState().name() : null;
            resultState = state.getResultState() != null ? state.getResultState().name() : null;
            stateMessage = state.getStateMessage();
        }

        Instant start = run.getStartTime() != null && run.getStartTime() > 0 ? Instant.ofEpochMilli(run.getStartTime()) : null;
        Instant end = run.getEndTime() != null && run.getEndTime() > 0 ? Instant.ofEpochMilli(run.getEndTime()) : null;

        Long durationMillis = run.getRunDuration();
        if (durationMillis == null && start != null && end != null && end.isAfter(start)) {
            durationMillis = end.toEpochMilli() - start.toEpochMilli();
        }

        return new RunStateInfo(lifeCycleState, resultState, stateMessage, start, end, durationMillis != null ? Duration.ofMillis(durationMillis) : null);
    }

    /** True once the run has terminated in a result state other than SUCCESS/SUCCESS_WITH_FAILURES. */
    public boolean isUnsuccessful() {
        return resultState != null && !SUCCESS_RESULT_STATES.contains(resultState);
    }

    /** Fails the Kestra task when the run terminated unsuccessfully; a no-op otherwise. */
    public void throwIfUnsuccessful(URI runURI) {
        if (!isUnsuccessful()) {
            return;
        }

        throw new IllegalStateException(
            "Databricks run %s did not succeed: resultState=%s, message=%s".formatted(runURI, resultState, stateMessage)
        );
    }
}
