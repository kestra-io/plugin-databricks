package io.kestra.plugin.databricks.job.task;

import java.util.List;

import com.databricks.sdk.service.jobs.DbtTask;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
@Schema(title = "DBT task settings")
public class DbtTaskSetting {
    @Schema(title = "Catalog", description = "Unity Catalog to run the dbt commands against.")
    private Property<String> catalog;

    @Schema(title = "Schema", description = "Schema to run the dbt commands against.")
    private Property<String> schema;

    @Schema(title = "SQL warehouse ID", description = "ID of the SQL warehouse used to run the dbt commands.")
    private Property<String> warehouseId;

    @Schema(title = "DBT commands", description = "List of dbt commands to execute in order, for example `dbt deps` then `dbt run`.")
    private Property<List<String>> commands;

    public DbtTask toDbtTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new DbtTask()
            .setCatalog(runContext.render(catalog).as(String.class).orElse(null))
            .setSchema(runContext.render(schema).as(String.class).orElse(null))
            .setWarehouseId(runContext.render(warehouseId).as(String.class).orElse(null))
            .setCommands(runContext.render(commands).asList(String.class).isEmpty() ? null : runContext.render(commands).asList(String.class));
    }
}
