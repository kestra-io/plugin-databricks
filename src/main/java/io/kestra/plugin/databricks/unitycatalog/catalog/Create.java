package io.kestra.plugin.databricks.unitycatalog.catalog;

import java.util.Map;

import com.databricks.sdk.service.catalog.CreateCatalog;

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
            title = "Create a Unity Catalog catalog",
            full = true,
            code = """
                id: databricks_uc_catalog_create
                namespace: company.team

                tasks:
                  - id: create_catalog
                    type: io.kestra.plugin.databricks.unitycatalog.catalog.Create
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    name: analytics
                    comment: Analytics data products
                """
        )
    }
)
@Schema(
    title = "Create a Unity Catalog catalog",
    description = "Creates a catalog in the Unity Catalog metastore. Fails if a catalog with the same name already exists."
)
public class Create extends AbstractTask implements RunnableTask<Create.Output> {
    @Schema(
        title = "Catalog name"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "Description of the catalog"
    )
    @PluginProperty(group = "main")
    private Property<String> comment;

    @Schema(
        title = "Managed storage root",
        description = "Cloud storage URL used as the default storage location for managed tables and volumes of this catalog. Defaults to the metastore storage root."
    )
    @PluginProperty(group = "main")
    private Property<String> storageRoot;

    @Schema(
        title = "Properties",
        description = "Key-value properties to attach to the object."
    )
    @PluginProperty(group = "main")
    private Property<Map<String, String>> properties;

    @Override
    public Create.Output run(RunContext runContext) throws Exception {
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var request = new CreateCatalog().setName(rName);
        runContext.render(comment).as(String.class).ifPresent(request::setComment);
        runContext.render(storageRoot).as(String.class).ifPresent(request::setStorageRoot);
        if (properties != null) {
            request.setProperties(runContext.render(properties).asMap(String.class, String.class));
        }

        var info = workspaceClient(runContext).catalogs().create(request);
        runContext.logger().info("Created catalog '{}'", info.getName());

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
