package io.kestra.plugin.databricks.cluster;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Delete a Databricks cluster.",
            full = true,
            code = """
                id: databricks_delete_cluster
                namespace: company.team

                tasks:
                  - id: delete_cluster
                    type: io.kestra.plugin.databricks.cluster.DeleteCluster
                    authentication:
                      token: <your-token>
                    host: <your-host>
                    clusterId: <your-cluster>
                """
        )
    }
)
@Schema(
    title = "Delete a Databricks cluster",
    description = "Terminates and deletes an existing Databricks cluster by clusterId."
)
public class DeleteCluster extends AbstractTask implements RunnableTask<VoidOutput> {
    @NotNull
    @Schema(title = "Cluster identifier", description = "ID of the cluster to delete")
    private Property<String> clusterId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var deleteCluster = new com.databricks.sdk.service.compute.DeleteCluster()
            .setClusterId(runContext.render(clusterId).as(String.class).orElseThrow());

        workspaceClient(runContext).clusters().delete(deleteCluster).get();

        return null;
    }
}
