package io.kestra.plugin.databricks.unitycatalog.volume;

import java.util.Map;

import com.databricks.sdk.service.catalog.ListVolumesRequest;

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
            title = "List the volumes of a schema",
            full = true,
            code = """
                id: databricks_uc_volume_list
                namespace: company.team

                tasks:
                  - id: list_volumes
                    type: io.kestra.plugin.databricks.unitycatalog.volume.List
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
    title = "List Unity Catalog volumes",
    description = "Lists the volumes of a schema that the caller can access."
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
        var request = new ListVolumesRequest().setCatalogName(rCatalogName).setSchemaName(rSchemaName);

        var volumes = UnityCatalogUtils.toMaps(workspaceClient(runContext).volumes().list(request));
        runContext.logger().info("Found {} volume(s) in schema '{}.{}'", volumes.size(), rCatalogName, rSchemaName);

        return Output.builder()
            .volumes(volumes)
            .size(volumes.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Volumes as returned by Databricks")
        private final java.util.List<Map<String, Object>> volumes;

        @Schema(title = "Number of volumes")
        private final Integer size;
    }
}
