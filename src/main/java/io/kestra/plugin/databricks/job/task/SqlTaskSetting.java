package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.SqlTask;
import com.databricks.sdk.service.jobs.SqlTaskQuery;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Builder
@Getter
public class SqlTaskSetting {
    @PluginProperty(dynamic = true)
    private String warehouseId;

    @PluginProperty(dynamic = true)
    private String queryId;

    @PluginProperty(dynamic = true, additionalProperties = String.class)
    @Schema(
        title = "Map of task parameters.",
        description = "Can be a map of string/string or a variable that binds to a JSON object.",
        anyOf = {String.class, Map.class}
    )
    private Object parameters;

    public SqlTask toSqlTask(RunContext runContext) throws IllegalVariableEvaluationException {
        Map<String, String> renderedParameters = ParametersUtils.mapParameters(runContext, parameters);

        return new SqlTask()
            .setWarehouseId(runContext.render(warehouseId))
            .setParameters(renderedParameters)
            .setQuery(new SqlTaskQuery().setQueryId(runContext.render(queryId)));
    }
}
