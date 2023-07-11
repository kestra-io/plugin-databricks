package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.SparkJarTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class SparkJarTaskSetting {
    @PluginProperty(dynamic = true)
    private String jarUri;

    @PluginProperty(dynamic = true)
    private String mainClassName;

    @PluginProperty(dynamic = true)
    private List<String> parameters;

    public SparkJarTask toSparkJarTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new SparkJarTask()
            .setJarUri(runContext.render(jarUri))
            .setMainClassName(runContext.render(mainClassName))
            .setParameters(parameters != null ? runContext.render(parameters) : null);
    }
}
