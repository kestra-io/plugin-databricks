package io.kestra.plugin.databricks.job.task;

import java.util.Map;

import com.databricks.sdk.service.jobs.RunJobTask;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class RunJobTaskSetting {
    @Schema(title = "Job identifier", description = "Numeric identifier of the existing Databricks job to run")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> jobId;

    @PluginProperty(dynamic = true, group = "advanced")
    private Object jobParameters;

    public RunJobTask toRunJobTask(RunContext runContext) throws IllegalVariableEvaluationException {
        Map<String, String> renderedJobParameters = ParametersUtils.mapParameters(runContext, jobParameters);
        var renderedJobId = runContext.render(jobId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The `jobId` property of `runJobTask` is required, set it to the identifier of the Databricks job to run"));

        long parsedJobId;
        try {
            parsedJobId = Long.parseLong(renderedJobId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The `jobId` property of `runJobTask` must be a number, but was '" + renderedJobId + "'", e);
        }

        return new RunJobTask()
            .setJobId(parsedJobId)
            .setJobParameters(renderedJobParameters);
    }
}
