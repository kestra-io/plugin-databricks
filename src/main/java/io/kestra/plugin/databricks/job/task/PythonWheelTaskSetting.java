package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.PythonWheelTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Builder
@Getter
public class PythonWheelTaskSetting {
    private Property<String> entryPoint;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "List of task parameters.",
        description = "Can be a list of strings or a variable that binds to a JSON array of strings.",
        anyOf = {String.class, String[].class}
    )
    private Object parameters;

    @PluginProperty(dynamic = true, additionalProperties = String.class)
    @Schema(
        title = "Map of task named parameters.",
        description = "Can be a map of string/string or a variable that binds to a JSON object.",
        anyOf = {String.class, Map.class}
    )
    private Object namedParameters;

    private Property<String> packageName;

    public PythonWheelTask toPythonWheelTask(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> renderedParameters = ParametersUtils.listParameters(runContext, parameters);
        Map<String, String> renderedNamedParameters = ParametersUtils.mapParameters(runContext, namedParameters);

        return new PythonWheelTask()
            .setEntryPoint(runContext.render(entryPoint).as(String.class).orElse(null))
            .setParameters(renderedParameters)
            .setNamedParameters(renderedNamedParameters)
            .setPackageName(runContext.render(packageName).as(String.class).orElse(null));
    }
}
