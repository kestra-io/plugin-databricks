package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.PipelineTask;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@Schema(title = "Delta Live Tables pipeline task settings")
public class PipelineTaskSetting {
    @Schema(title = "Pipeline ID", description = "ID of the Delta Live Tables pipeline to trigger.")
    private Property<String> pipelineId;

    @Schema(title = "Full refresh", description = "If true, the pipeline runs a full refresh, reprocessing all data.")
    private Property<Boolean> fullRefresh;

    public PipelineTask toPipelineTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new PipelineTask()
            .setPipelineId(runContext.render(pipelineId).as(String.class).orElse(null))
            .setFullRefresh(runContext.render(fullRefresh).as(Boolean.class).orElse(null));
    }
}
