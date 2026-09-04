package io.kestra.plugin.databricks.job;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.stream.IntStream;

import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.jobs.Run;
import com.databricks.sdk.service.jobs.RunLifeCycleState;
import com.databricks.sdk.service.jobs.RunResultState;
import com.databricks.sdk.service.jobs.RunState;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SubmitRunIdempotencyTest {
    private static final String TASK_RUN_ID = "some-taskrun-id";
    private static final String HOST = "https://example.databricks.com";

    @Test
    void workerLossResubmitAdoptsInFlightRun() {
        var runs = List.of(run(1L, RunLifeCycleState.RUNNING, null));

        var outcome = SubmitRun.submitIdempotent(TASK_RUN_ID, HOST, submitOf(runs), getRunOf(runs));

        assertThat(outcome.generation(), is(0));
        assertThat(outcome.adopted(), is(true));
        assertThat(outcome.run().getRunId(), is(1L));
    }

    @Test
    void workerLossResubmitAfterSuccessAdoptsTheSuccessfulRun() {
        var runs = List.of(run(1L, RunLifeCycleState.TERMINATED, RunResultState.SUCCESS));

        var outcome = SubmitRun.submitIdempotent(TASK_RUN_ID, HOST, submitOf(runs), getRunOf(runs));

        assertThat(outcome.generation(), is(0));
        assertThat(outcome.adopted(), is(true));
    }

    @Test
    void plainRetryAfterAFailureCreatesAGenuinelyNewRun() {
        var runs = List.of(
            run(1L, RunLifeCycleState.TERMINATED, RunResultState.FAILED),
            run(2L, RunLifeCycleState.PENDING, null)
        );

        var outcome = SubmitRun.submitIdempotent(TASK_RUN_ID, HOST, submitOf(runs), getRunOf(runs));

        assertThat(outcome.generation(), is(1));
        assertThat(outcome.adopted(), is(false));
        assertThat(outcome.run().getRunId(), is(2L));
    }

    @Test
    void secondRetryWalksPastTwoFailedGenerations() {
        var runs = List.of(
            run(1L, RunLifeCycleState.TERMINATED, RunResultState.FAILED),
            run(2L, RunLifeCycleState.TERMINATED, RunResultState.CANCELED),
            run(3L, RunLifeCycleState.QUEUED, null)
        );

        var outcome = SubmitRun.submitIdempotent(TASK_RUN_ID, HOST, submitOf(runs), getRunOf(runs));

        assertThat(outcome.generation(), is(2));
        assertThat(outcome.adopted(), is(false));
        assertThat(outcome.run().getRunId(), is(3L));
    }

    @Test
    void deletedTokenErrorAdvancesTheGeneration() {
        var calls = new AtomicInteger(0);
        var freshRun = run(2L, RunLifeCycleState.RUNNING, null);
        Function<String, Long> submit = token -> {
            if (calls.getAndIncrement() == 0) {
                throw new NotFound("run was deleted", null);
            }
            return freshRun.getRunId();
        };
        LongFunction<Run> getRun = runId -> freshRun;

        var outcome = SubmitRun.submitIdempotent(TASK_RUN_ID, HOST, submit, getRun);

        assertThat(outcome.generation(), is(1));
        assertThat(outcome.run().getRunId(), is(2L));
    }

    @Test
    void failsWithAnActionableMessageWhenTheWalkIsExhausted() {
        var failingRuns = IntStream.range(0, SubmitRun.MAX_IDEMPOTENCY_GENERATIONS)
            .mapToObj(i -> run(i + 1L, RunLifeCycleState.TERMINATED, RunResultState.FAILED))
            .toList();

        var exception = assertThrows(
            IllegalStateException.class,
            () -> SubmitRun.submitIdempotent(TASK_RUN_ID, HOST, submitOf(failingRuns), getRunOf(failingRuns))
        );

        assertThat(exception.getMessage().contains(TASK_RUN_ID), is(true));
        assertThat(exception.getMessage().contains(String.valueOf(SubmitRun.MAX_IDEMPOTENCY_GENERATIONS)), is(true));
    }

    private static Function<String, Long> submitOf(List<Run> runs) {
        var index = new AtomicInteger(0);
        return token -> {
            var run = runs.get(Math.min(index.getAndIncrement(), runs.size() - 1));
            return run.getRunId();
        };
    }

    private static LongFunction<Run> getRunOf(List<Run> runs) {
        return runId -> runs.stream()
            .filter(run -> run.getRunId() == runId)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No stubbed run for id " + runId));
    }

    private static Run run(long runId, RunLifeCycleState lifecycle, RunResultState result) {
        return new Run()
            .setJobId(42L)
            .setRunId(runId)
            .setState(new RunState().setLifeCycleState(lifecycle).setResultState(result));
    }
}
