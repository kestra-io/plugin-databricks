package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.DbtTask;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Builder
@Getter
public class DbtTaskSetting {
    private Property<String> catalog;

    private Property<String> schema;

    private Property<String> warehouseId;

    private Property<List<String>> commands;

    public DbtTask toDbtTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new DbtTask()
            .setCatalog(runContext.render(catalog).as(String.class).orElse(null))
            .setSchema(runContext.render(schema).as(String.class).orElse(null))
            .setWarehouseId(runContext.render(warehouseId).as(String.class).orElse(null))
            .setCommands(runContext.render(commands).asList(String.class).isEmpty() ? null : runContext.render(commands).asList(String.class));
    }
}
