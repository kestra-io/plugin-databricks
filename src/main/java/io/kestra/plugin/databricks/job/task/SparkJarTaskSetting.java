package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.SparkJarTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class SparkJarTaskSetting {
    private Property<String> jarUri;

    private Property<String> mainClassName;

    @PluginProperty(dynamic = true)
    @Schema(
        title = "List of task parameters.",
        description = "Can be a list of strings or a variable that binds to a JSON array of strings.",
        anyOf = {String.class, String[].class}
    )
    private Object parameters;

    public SparkJarTask toSparkJarTask(RunContext runContext) throws IllegalVariableEvaluationException {
        List<String> renderedParameters = ParametersUtils.listParameters(runContext, parameters);

        return new SparkJarTask()
            .setJarUri(runContext.render(jarUri).as(String.class).orElse(null))
            .setMainClassName(runContext.render(mainClassName).as(String.class).orElse(null))
            .setParameters(renderedParameters);
    }
}
