package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.SparkSubmitTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class SparkSubmitTaskSetting {
    @PluginProperty(dynamic = true)
    private List<String> parameters;

    public SparkSubmitTask toSparkSubmitTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new SparkSubmitTask()
            .setParameters(parameters != null ? runContext.render(parameters) : null);
    }
}
