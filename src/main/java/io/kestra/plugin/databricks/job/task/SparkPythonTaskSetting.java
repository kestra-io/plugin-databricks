package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.Source;
import com.databricks.sdk.service.jobs.SparkPythonTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import javax.validation.constraints.NotNull;

@Builder
@Getter
public class SparkPythonTaskSetting {

    @PluginProperty(dynamic = true)
    @NotNull
    private String pythonFile;

    @PluginProperty(dynamic = true)
    private List<String> parameters;

    @PluginProperty
    @NotNull
    private Source sparkPythonTaskSource;

    public SparkPythonTask toSparkPythonTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new SparkPythonTask()
            .setPythonFile(runContext.render(pythonFile))
            .setParameters(parameters != null ? runContext.render(parameters) : null)
            .setSource(sparkPythonTaskSource);
    }
}
