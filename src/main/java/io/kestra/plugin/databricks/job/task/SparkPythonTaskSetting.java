package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.Source;
import com.databricks.sdk.service.jobs.SparkPythonTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import jakarta.validation.constraints.NotNull;

@Builder
@Getter
public class SparkPythonTaskSetting {

    @NotNull
    private Property<String> pythonFile;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "List of task parameters.",
        description = "Can be a list of strings or a variable that binds to a JSON array of strings.",
        anyOf = {String.class, String[].class}
    )
    private Object parameters;

    @NotNull
    private Property<Source> sparkPythonTaskSource;

    public SparkPythonTask toSparkPythonTask(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> renderedParameters = ParametersUtils.listParameters(runContext, parameters);

        return new SparkPythonTask()
            .setPythonFile(runContext.render(pythonFile).as(String.class).orElseThrow())
            .setParameters(renderedParameters)
            .setSource(runContext.render(sparkPythonTaskSource).as(Source.class).orElseThrow());
    }
}
