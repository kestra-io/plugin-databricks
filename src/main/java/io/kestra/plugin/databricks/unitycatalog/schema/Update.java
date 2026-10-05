package io.kestra.plugin.databricks.unitycatalog.schema;

import java.util.Map;

import com.databricks.sdk.service.catalog.UpdateSchema;

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
            title = "Change the owner of a schema",
            full = true,
            code = """
                id: databricks_uc_schema_update
                namespace: company.team

                tasks:
                  - id: update_schema
                    type: io.kestra.plugin.databricks.unitycatalog.schema.Update
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    fullName: main.landing_zone
                    owner: data-platform
                """
        )
    }
)
@Schema(
    title = "Update a Unity Catalog schema",
    description = "Updates the comment, owner, properties or name of a schema. Only the properties you set are changed."
)
public class Update extends AbstractTask implements RunnableTask<Update.Output> {
    @Schema(
        title = "Schema full name",
        description = "Fully qualified schema name, in the form `catalog.schema`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Schema(
        title = "New schema name",
        description = "Rename the schema to this value."
    )
    @PluginProperty(group = "main")
    private Property<String> newName;

    @Schema(
        title = "Description of the schema"
    )
    @PluginProperty(group = "main")
    private Property<String> comment;

    @Schema(
        title = "Schema owner",
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
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();
        var request = new UpdateSchema().setFullName(rFullName);
        runContext.render(newName).as(String.class).ifPresent(request::setNewName);
        runContext.render(comment).as(String.class).ifPresent(request::setComment);
        runContext.render(owner).as(String.class).ifPresent(request::setOwner);
        if (properties != null) {
            request.setProperties(runContext.render(properties).asMap(String.class, String.class));
        }

        var info = workspaceClient(runContext).schemas().update(request);
        runContext.logger().info("Updated schema '{}'", info.getFullName());

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
