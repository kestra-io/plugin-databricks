package io.kestra.plugin.databricks.utils;

import java.time.Duration;
import java.time.Instant;

import com.databricks.sdk.service.jobs.Run;

/**
 * Snapshot of a Databricks {@link Run}'s state and timing, extracted once so both SubmitRun and CreateJob
 * can populate their own (differently shaped) Output from the same logic.
 */
public record RunStateInfo(String lifeCycleState, String resultState, String stateMessage, Instant startTime, Instant endTime, Duration duration) {
    private static final RunStateInfo EMPTY = new RunStateInfo(null, null, null, null, null, null);

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
}
