package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.DbtTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class DbtTaskSetting {
    @PluginProperty(dynamic = true)
    private String catalog;

    @PluginProperty(dynamic = true)
    private String schema;

    @PluginProperty(dynamic = true)
    private String warehouseId;

    @PluginProperty(dynamic = true)
    private List<String> commands;

    public DbtTask toDbtTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new DbtTask()
            .setCatalog(runContext.render(catalog))
            .setSchema(runContext.render(schema))
            .setWarehouseId(runContext.render(warehouseId))
            .setCommands(commands != null ? runContext.render(commands) : null);
    }
}
