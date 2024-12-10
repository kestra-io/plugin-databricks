package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.RunJobTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Builder
@Getter
public class RunJobTaskSetting {
    private Property<String> jobId;

    @PluginProperty(dynamic = true)
    private Object jobParameters;

    public RunJobTask toRunJobTask(RunContext runContext) throws IllegalVariableEvaluationException {
        Map<String, String> renderedJobParameters = ParametersUtils.mapParameters(runContext, jobParameters);
        return new RunJobTask()
            .setJobId(Long.parseLong(runContext.render(jobId).as(String.class).orElseThrow()))
            .setJobParameters(renderedJobParameters);
    }
}
