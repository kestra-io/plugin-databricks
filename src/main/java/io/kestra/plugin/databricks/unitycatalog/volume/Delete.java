package io.kestra.plugin.databricks.unitycatalog.volume;

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
            title = "Delete a Unity Catalog volume",
            full = true,
            code = """
                id: databricks_uc_volume_delete
                namespace: company.team

                tasks:
                  - id: delete_volume
                    type: io.kestra.plugin.databricks.unitycatalog.volume.Delete
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    fullName: main.landing_zone.raw_files
                """
        )
    }
)
@Schema(
    title = "Delete a Unity Catalog volume",
    description = "Deletes a volume. Deleting a managed volume also deletes the files it contains."
)
public class Delete extends AbstractTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Volume full name",
        description = "Fully qualified volume name, in the form `catalog.schema.volume`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();

        workspaceClient(runContext).volumes().delete(rFullName);
        runContext.logger().info("Deleted volume '{}'", rFullName);

        return null;
    }
}
