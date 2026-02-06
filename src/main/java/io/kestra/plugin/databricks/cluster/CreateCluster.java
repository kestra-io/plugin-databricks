package io.kestra.plugin.databricks.cluster;

import com.databricks.sdk.service.compute.AutoScale;
import com.databricks.sdk.service.compute.State;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import jakarta.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            title = "Create a Databricks cluster with one worker.",
            full = true,
            code = """
                id: databricks_create_cluster
                namespace: company.team

                tasks:
                  - id: create_cluster
                    type: io.kestra.plugin.databricks.cluster.CreateCluster
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: <your-host>
                    clusterName: kestra-demo
                    nodeTypeId: n2-highmem-4
                    numWorkers: 1
                    sparkVersion: 13.0.x-scala2.12
                """
        )
    }
)
@Schema(
    title = "Create a Databricks cluster",
    description = """
        Provisions a new Databricks cluster via the Compute API.
        Set a fixed worker count with numWorkers or enable autoscaling with minWorkers/maxWorkers; auto termination is optional.
        """
)
public class CreateCluster extends AbstractTask implements RunnableTask<CreateCluster.Output> {
    @NotNull
    @Schema(title = "Cluster name")
    private Property<String> clusterName;

    @NotNull
    @Schema(title = "Spark version", description = "Runtime identifier, e.g. 13.0.x-scala2.12")
    private Property<String> sparkVersion;

    @Schema(title = "Node type", description = "Instance type; values depend on the workspace cloud provider")
    private Property<String> nodeTypeId;

    @Schema(title = "Auto-termination minutes", description = "Idle timeout; cluster is terminated after this duration if set")
    private Property<Long> autoTerminationMinutes;

    @Schema(
        title = "Fixed workers",
        description = "Required unless autoscaling is configured; sets numWorkers on the cluster"
    )
    private Property<Long> numWorkers;

    @Schema(
        title = "Minimum workers",
        description = "Use with maxWorkers to enable autoscaling; ignored when numWorkers is set"
    )
    private Property<Long> minWorkers;

    @Schema(
        title = "Maximum workers",
        description = "Use with minWorkers to enable autoscaling; ignored when numWorkers is set"
    )
    private Property<Long> maxWorkers;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var createCluster = new com.databricks.sdk.service.compute.CreateCluster()
                .setClusterName(runContext.render(clusterName).as(String.class).orElseThrow())
                .setSparkVersion(runContext.render(sparkVersion).as(String.class).orElseThrow())
                .setNodeTypeId(runContext.render(nodeTypeId).as(String.class).orElse(null))
                .setAutoterminationMinutes(runContext.render(autoTerminationMinutes).as(Long.class).orElse(null))
                .setNumWorkers(runContext.render(numWorkers).as(Long.class).orElse(null));
        if (minWorkers != null && maxWorkers != null) {
            createCluster.setAutoscale(new AutoScale()
                .setMinWorkers(runContext.render(minWorkers).as(Long.class).orElseThrow())
                .setMaxWorkers(runContext.render(maxWorkers).as(Long.class).orElseThrow())
            );
        }

        var workspaceClient = workspaceClient(runContext);
        var response = workspaceClient.clusters().create(createCluster).get();
        var clusterURI = URI.create(workspaceClient.config().getHost() + "/#setting/clusters/" + response.getClusterId() + "/configuration");
        runContext.logger().info("Cluster created: {}", clusterURI);

        return Output.builder()
            .clusterId(response.getClusterId())
            .clusterURI(clusterURI)
            .clusterState(response.getState())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Cluster identifier")
        private String clusterId;

        @Schema(title = "Cluster console URI")
        private URI clusterURI;

        @Schema(title = "Cluster state")
        private State clusterState;
    }
}
