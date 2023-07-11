package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.PythonWheelTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

@Builder
@Getter
public class PythonWheelTaskSetting {
    @PluginProperty(dynamic = true)
    private String entryPoint;

    @PluginProperty(dynamic = true)
    private List<String> parameters;

    @PluginProperty
    private Map<String, String> namedParameters;

    @PluginProperty(dynamic = true)
    private String packageName;

    public PythonWheelTask toPythonWheelTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new PythonWheelTask()
            .setEntryPoint(runContext.render(entryPoint))
            .setParameters(parameters != null ? runContext.render(parameters) : null)
            .setNamedParameters(namedParameters)
            .setPackageName(runContext.render(packageName));
    }
}
