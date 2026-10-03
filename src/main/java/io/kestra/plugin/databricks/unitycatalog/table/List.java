package io.kestra.plugin.databricks.unitycatalog.table;

import java.util.Map;

import com.databricks.sdk.service.catalog.ListTablesRequest;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "List the tables of a schema",
            full = true,
            code = """
                id: databricks_uc_table_list
                namespace: company.team

                tasks:
                  - id: list_tables
                    type: io.kestra.plugin.databricks.unitycatalog.table.List
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                    schemaName: landing_zone
                """
        )
    }
)
@Schema(
    title = "List Unity Catalog tables",
    description = "Lists the tables of a schema that the caller can access."
)
public class List extends AbstractTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Catalog name",
        description = "Name of the parent catalog."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> catalogName;

    @Schema(
        title = "Schema name",
        description = "Name of the parent schema."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> schemaName;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        var rCatalogName = runContext.render(catalogName).as(String.class).orElseThrow();
        var rSchemaName = runContext.render(schemaName).as(String.class).orElseThrow();
        var request = new ListTablesRequest().setCatalogName(rCatalogName).setSchemaName(rSchemaName);

        var tables = UnityCatalogUtils.toMaps(workspaceClient(runContext).tables().list(request));
        runContext.logger().info("Found {} table(s) in schema '{}.{}'", tables.size(), rCatalogName, rSchemaName);

        return Output.builder()
            .tables(tables)
            .size(tables.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Tables as returned by Databricks")
        private final java.util.List<Map<String, Object>> tables;

        @Schema(title = "Number of tables")
        private final Integer size;
    }
}
