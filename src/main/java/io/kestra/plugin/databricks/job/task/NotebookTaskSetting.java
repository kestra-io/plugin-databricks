package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.NotebookTask;
import com.databricks.sdk.service.jobs.Source;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Builder
@Getter
public class NotebookTaskSetting {
    @PluginProperty(dynamic = true)
    private String notebookPath;

    @PluginProperty
    private Source source;

    @PluginProperty(dynamic = true, additionalProperties = String.class)
    @Schema(
        title = "Map of task base parameters.",
        description = "Can be a map of string/string or a variable that binds to a JSON object.",
        anyOf = {String.class, Map.class}
    )
    private Object baseParameters;

    public NotebookTask toNotebookTask(RunContext runContext) throws IllegalVariableEvaluationException {
        Map<String, String> renderedParameters = ParametersUtils.mapParameters(runContext, baseParameters);

        return new NotebookTask()
            .setNotebookPath(runContext.render(notebookPath))
            .setSource(source)
            .setBaseParameters(renderedParameters);
    }
}
