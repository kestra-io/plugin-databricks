package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.PipelineTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class PipelineTaskSetting {
    @PluginProperty(dynamic = true)
    private String pipelineId;

    @PluginProperty
    private Boolean fullRefresh;

    public PipelineTask toPipelineTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new PipelineTask()
            .setPipelineId(runContext.render(pipelineId))
            .setFullRefresh(fullRefresh);
    }
}
