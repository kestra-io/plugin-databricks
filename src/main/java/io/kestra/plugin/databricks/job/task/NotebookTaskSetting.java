package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.NotebookTask;
import com.databricks.sdk.service.jobs.Source;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
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

    @PluginProperty(dynamic = true)
    private Map<String, String> baseParameters;

    public NotebookTask toNotebookTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new NotebookTask()
            .setNotebookPath(runContext.render(notebookPath))
            .setSource(source)
            .setBaseParameters(baseParameters != null ? runContext.renderMap(baseParameters) : null);
    }
}
