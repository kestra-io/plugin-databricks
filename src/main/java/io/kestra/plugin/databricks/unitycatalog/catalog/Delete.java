package io.kestra.plugin.databricks.unitycatalog.catalog;

import com.databricks.sdk.service.catalog.DeleteCatalogRequest;

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
            title = "Delete a Unity Catalog catalog",
            full = true,
            code = """
                id: databricks_uc_catalog_delete
                namespace: company.team

                tasks:
                  - id: delete_catalog
                    type: io.kestra.plugin.databricks.unitycatalog.catalog.Delete
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    name: analytics
                    force: true
                """
        )
    }
)
@Schema(
    title = "Delete a Unity Catalog catalog",
    description = "Deletes a catalog. Set `force` to delete a catalog that still contains schemas."
)
public class Delete extends AbstractTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Catalog name"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "Force deletion",
        description = "Delete the catalog even if it is not empty. Defaults to `false`."
    )
    @PluginProperty(group = "main")
    private Property<Boolean> force;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var rName = runContext.render(name).as(String.class).orElseThrow();
        var request = new DeleteCatalogRequest().setName(rName);
        runContext.render(force).as(Boolean.class).ifPresent(request::setForce);

        workspaceClient(runContext).catalogs().delete(request);
        runContext.logger().info("Deleted catalog '{}'", rName);

        return null;
    }
}
