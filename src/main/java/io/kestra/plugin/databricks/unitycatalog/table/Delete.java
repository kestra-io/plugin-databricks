package io.kestra.plugin.databricks.unitycatalog.table;

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
            title = "Delete a Unity Catalog table",
            full = true,
            code = """
                id: databricks_uc_table_delete
                namespace: company.team

                tasks:
                  - id: delete_table
                    type: io.kestra.plugin.databricks.unitycatalog.table.Delete
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    fullName: main.landing_zone.events
                """
        )
    }
)
@Schema(
    title = "Delete a Unity Catalog table",
    description = "Deletes a table from Unity Catalog. The table must be owned by the caller or the caller must be a metastore admin. Tables are created with SQL, for example using the `io.kestra.plugin.databricks.sql.Query` task."
)
public class Delete extends AbstractTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Table full name",
        description = "Fully qualified table name, in the form `catalog.schema.table`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();

        workspaceClient(runContext).tables().delete(rFullName);
        runContext.logger().info("Deleted table '{}'", rFullName);

        return null;
    }
}
