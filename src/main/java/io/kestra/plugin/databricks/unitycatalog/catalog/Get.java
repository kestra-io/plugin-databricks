package io.kestra.plugin.databricks.unitycatalog.catalog;

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
            title = "Get a Unity Catalog catalog",
            full = true,
            code = """
                id: databricks_uc_catalog_get
                namespace: company.team

                tasks:
                  - id: get_catalog
                    type: io.kestra.plugin.databricks.unitycatalog.catalog.Get
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    name: analytics
                """
        )
    }
)
@Schema(
    title = "Get a Unity Catalog catalog",
    description = "Fetches the details of a catalog by name."
)
public class Get extends AbstractTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "Catalog name"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> name;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var info = workspaceClient(runContext).catalogs().get(rName);

        return Output.builder()
            .name(info.getName())
            .owner(info.getOwner())
            .catalog(UnityCatalogUtils.toMap(info))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Catalog name")
        private final String name;

        @Schema(title = "Catalog owner")
        private final String owner;

        @Schema(title = "Full catalog details as returned by Databricks")
        private final Map<String, Object> catalog;
    }
}
