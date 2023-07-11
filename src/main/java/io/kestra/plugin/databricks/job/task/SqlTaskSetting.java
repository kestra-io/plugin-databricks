package io.kestra.plugin.databricks.job.task;

import com.databricks.sdk.service.jobs.SqlTask;
import com.databricks.sdk.service.jobs.SqlTaskQuery;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.runners.RunContext;
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

    @PluginProperty
    private Map<String, String> parameters;

    public SqlTask toSqlTask(RunContext runContext) throws IllegalVariableEvaluationException {
        return new SqlTask()
            .setWarehouseId(runContext.render(warehouseId))
            .setParameters(parameters)
            .setQuery(new SqlTaskQuery().setQueryId(runContext.render(queryId)));
    }
}
