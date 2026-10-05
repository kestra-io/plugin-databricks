package io.kestra.plugin.databricks.unitycatalog.schema;

import java.util.Map;

import com.databricks.sdk.service.catalog.CreateSchema;

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
            title = "Create a schema in the main catalog",
            full = true,
            code = """
                id: databricks_uc_schema_create
                namespace: company.team

                tasks:
                  - id: create_schema
                    type: io.kestra.plugin.databricks.unitycatalog.schema.Create
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                    name: landing_zone
                    comment: Raw data landing area
                """
        )
    }
)
@Schema(
    title = "Create a Unity Catalog schema",
    description = "Creates a schema in an existing catalog. Fails if the schema already exists."
)
public class Create extends AbstractTask implements RunnableTask<Create.Output> {
    @Schema(
        title = "Catalog name",
        description = "Name of the parent catalog."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> catalogName;

    @Schema(
        title = "Schema name"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "Description of the schema"
    )
    @PluginProperty(group = "main")
    private Property<String> comment;

    @Schema(
        title = "Managed storage root",
        description = "Cloud storage URL used for managed tables and volumes of this schema. Defaults to the catalog or metastore storage root."
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
        var rCatalogName = runContext.render(catalogName).as(String.class).orElseThrow();
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var request = new CreateSchema().setCatalogName(rCatalogName).setName(rName);
        runContext.render(comment).as(String.class).ifPresent(request::setComment);
        runContext.render(storageRoot).as(String.class).ifPresent(request::setStorageRoot);
        if (properties != null) {
            request.setProperties(runContext.render(properties).asMap(String.class, String.class));
        }

        var info = workspaceClient(runContext).schemas().create(request);
        runContext.logger().info("Created schema '{}'", info.getFullName());

        return Output.builder()
            .name(info.getName())
            .fullName(info.getFullName())
            .owner(info.getOwner())
            .schema(UnityCatalogUtils.toMap(info))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Schema name")
        private final String name;

        @Schema(title = "Fully qualified schema name (`catalog.schema`)")
        private final String fullName;

        @Schema(title = "Schema owner")
        private final String owner;

        @Schema(title = "Full schema details as returned by Databricks")
        private final Map<String, Object> schema;
    }
}
