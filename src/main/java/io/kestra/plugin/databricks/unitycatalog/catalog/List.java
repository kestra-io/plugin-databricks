package io.kestra.plugin.databricks.unitycatalog.catalog;

import java.util.Map;

import com.databricks.sdk.service.catalog.ListCatalogsRequest;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogUtils;

import io.swagger.v3.oas.annotations.media.Schema;
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
            title = "List Unity Catalog catalogs",
            full = true,
            code = """
                id: databricks_uc_catalog_list
                namespace: company.team

                tasks:
                  - id: list_catalogs
                    type: io.kestra.plugin.databricks.unitycatalog.catalog.List
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                """
        )
    }
)
@Schema(
    title = "List Unity Catalog catalogs",
    description = "Lists all the catalogs of the metastore that the caller can access."
)
public class List extends AbstractTask implements RunnableTask<List.Output> {
    @Override
    public List.Output run(RunContext runContext) throws Exception {
        var catalogs = UnityCatalogUtils.toMaps(
            workspaceClient(runContext).catalogs().list(new ListCatalogsRequest())
        );
        runContext.logger().info("Found {} catalog(s)", catalogs.size());

        return Output.builder()
            .catalogs(catalogs)
            .size(catalogs.size())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Catalogs as returned by Databricks")
        private final java.util.List<Map<String, Object>> catalogs;

        @Schema(title = "Number of catalogs")
        private final Integer size;
    }
}
