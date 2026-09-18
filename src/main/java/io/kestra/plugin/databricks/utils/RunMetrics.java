package io.kestra.plugin.databricks.utils;

import io.kestra.core.models.executions.metrics.Timer;
import io.kestra.core.runners.RunContext;

public final class RunMetrics {
    private RunMetrics() {
        //utility class pattern
    }

    /** Emits the run.duration timer metric; a no-op when the run hasn't terminated yet. */
    public static void emitDurationMetric(RunContext runContext, RunStateInfo state) {
        if (state.duration() != null) {
            runContext.metric(Timer.of("run.duration", state.duration()));
        }
    }
}
