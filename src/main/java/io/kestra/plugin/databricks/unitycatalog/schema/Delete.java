package io.kestra.plugin.databricks.unitycatalog.schema;

import com.databricks.sdk.service.catalog.DeleteSchemaRequest;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
            title = "Delete a Unity Catalog schema",
            full = true,
            code = """
                id: databricks_uc_schema_delete
                namespace: company.team

                tasks:
                  - id: delete_schema
                    type: io.kestra.plugin.databricks.unitycatalog.schema.Delete
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    fullName: main.landing_zone
                    force: true
                """
        )
    }
)
@Schema(
    title = "Delete a Unity Catalog schema",
    description = "Deletes a schema. Set `force` to delete a schema that still contains objects."
)
public class Delete extends AbstractTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Schema full name",
        description = "Fully qualified schema name, in the form `catalog.schema`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Schema(
        title = "Force deletion",
        description = "Delete the schema even if it is not empty. Defaults to `false`."
    )
    @PluginProperty(group = "main")
    private Property<Boolean> force;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();
        var request = new DeleteSchemaRequest().setFullName(rFullName);
        runContext.render(force).as(Boolean.class).ifPresent(request::setForce);

        workspaceClient(runContext).schemas().delete(request);
        runContext.logger().info("Deleted schema '{}'", rFullName);

        return null;
    }
}
