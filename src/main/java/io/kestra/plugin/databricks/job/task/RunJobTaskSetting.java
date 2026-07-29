package io.kestra.plugin.databricks.job.task;

import java.util.Map;

import com.databricks.sdk.service.jobs.RunJobTask;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@Schema(title = "Run-job task settings")
public class RunJobTaskSetting {
    @Schema(title = "Job ID", description = "ID of an existing Databricks job to run. Required.")
    private Property<String> jobId;

    @PluginProperty(dynamic = true, group = "advanced")
    @Schema(
        title = "Job parameters",
        description = "Map of parameters passed to the triggered job. Can be a map of string/string or a variable that binds to a JSON object.",
        anyOf = { String.class, Map.class }
    )
    private Object jobParameters;

    public RunJobTask toRunJobTask(RunContext runContext) throws IllegalVariableEvaluationException {
        Map<String, String> renderedJobParameters = ParametersUtils.mapParameters(runContext, jobParameters);
        return new RunJobTask()
            .setJobId(Long.parseLong(runContext.render(jobId).as(String.class).orElseThrow()))
            .setJobParameters(renderedJobParameters);
    }
}
