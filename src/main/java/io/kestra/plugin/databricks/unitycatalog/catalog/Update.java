package io.kestra.plugin.databricks.unitycatalog.catalog;

import java.util.Map;

import com.databricks.sdk.service.catalog.UpdateCatalog;

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
            title = "Change the owner of a catalog",
            full = true,
            code = """
                id: databricks_uc_catalog_update
                namespace: company.team

                tasks:
                  - id: update_catalog
                    type: io.kestra.plugin.databricks.unitycatalog.catalog.Update
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    name: analytics
                    owner: data-platform
                """
        )
    }
)
@Schema(
    title = "Update a Unity Catalog catalog",
    description = "Updates the comment, owner, properties or name of a catalog. Only the properties you set are changed."
)
public class Update extends AbstractTask implements RunnableTask<Update.Output> {
    @Schema(
        title = "Catalog name",
        description = "Current name of the catalog to update."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "New catalog name",
        description = "Rename the catalog to this value."
    )
    @PluginProperty(group = "main")
    private Property<String> newName;

    @Schema(
        title = "Description of the catalog"
    )
    @PluginProperty(group = "main")
    private Property<String> comment;

    @Schema(
        title = "Catalog owner",
        description = "Username or group name of the new owner."
    )
    @PluginProperty(group = "main")
    private Property<String> owner;

    @Schema(
        title = "Properties",
        description = "Key-value properties to attach to the object."
    )
    @PluginProperty(group = "main")
    private Property<Map<String, String>> properties;

    @Override
    public Update.Output run(RunContext runContext) throws Exception {
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var request = new UpdateCatalog().setName(rName);
        runContext.render(newName).as(String.class).ifPresent(request::setNewName);
        runContext.render(comment).as(String.class).ifPresent(request::setComment);
        runContext.render(owner).as(String.class).ifPresent(request::setOwner);
        if (properties != null) {
            request.setProperties(runContext.render(properties).asMap(String.class, String.class));
        }

        var info = workspaceClient(runContext).catalogs().update(request);
        runContext.logger().info("Updated catalog '{}'", info.getName());

        return Output.builder()
            .name(info.getName())
            .owner(info.getOwner())
            .catalog(UnityCatalogUtils.toMap(info))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Catalog name after the update")
        private final String name;

        @Schema(title = "Catalog owner")
        private final String owner;

        @Schema(title = "Full catalog details as returned by Databricks")
        private final Map<String, Object> catalog;
    }
}
