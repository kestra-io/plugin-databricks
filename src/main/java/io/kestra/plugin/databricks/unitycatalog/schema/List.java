package io.kestra.plugin.databricks.unitycatalog.schema;

import java.util.Map;

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
            title = "List the schemas of a catalog",
            full = true,
            code = """
                id: databricks_uc_schema_list
                namespace: company.team

                tasks:
                  - id: list_schemas
                    type: io.kestra.plugin.databricks.unitycatalog.schema.List
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                """
        )
    }
)
@Schema(
    title = "List Unity Catalog schemas",
    description = "Lists the schemas of a catalog that the caller can access."
)
public class List extends AbstractTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Catalog name",
        description = "Name of the parent catalog."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> catalogName;

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        var rCatalogName = runContext.render(catalogName).as(String.class).orElseThrow();
        var schemas = UnityCatalogUtils.toMaps(workspaceClient(runContext).schemas().list(rCatalogName));
        runContext.logger().info("Found {} schema(s) in catalog '{}'", schemas.size(), rCatalogName);

        return Output.builder()
            .schemas(schemas)
            .size(schemas.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Schemas as returned by Databricks")
        private final java.util.List<Map<String, Object>> schemas;

        @Schema(title = "Number of schemas")
        private final Integer size;
    }
}
