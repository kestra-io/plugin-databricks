package io.kestra.plugin.databricks.cluster;

import com.databricks.sdk.service.compute.AutoScale;
import com.databricks.sdk.service.compute.State;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
                      token: <your-token>
                    host: <your-host>
                    clusterName: kestra-demo
                    nodeTypeId: n2-highmem-4
                    numWorkers: 1
                    sparkVersion: 13.0.x-scala2.12
                """
        )
    }
)
@Schema(title = "Create a Databricks cluster.")
public class CreateCluster extends AbstractTask implements RunnableTask<CreateCluster.Output> {
    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(title = "The name of the cluster.")
    private String clusterName;

    @NotNull
    @PluginProperty(dynamic = true)
    @Schema(title = "The Spark version.")
    private String sparkVersion;

    @PluginProperty(dynamic = true)
    @Schema(title = "The type of node, the value depends on the cloud provider.")
    private String nodeTypeId;

    @PluginProperty
    @Schema(title = "If set, the cluster will be terminated automatically after this time period.")
    private Long autoTerminationMinutes;

    @PluginProperty
    @Schema(
        title = "The fixed number of workers.",
        description = "You must set this property unless you use the `minWorkers` and `maxWorkers` properties for autoscaling."
    )
    private Long numWorkers;

    @PluginProperty
    @Schema(
        title = "The minimum number of workers.",
        description = "Use this property along with `maxWorkers` for autoscaling. Otherwise, set a fixed number of workers using `numWorkers`."
    )
    private Long minWorkers;

    @PluginProperty
    @Schema(
        title = "The maximum number of workers.",
        description = "Use this property along with `minWorkers` to use autoscaling. Otherwise, set a fixed number of workers using `numWorkers`."
    )
    private Long maxWorkers;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var createCluster = new com.databricks.sdk.service.compute.CreateCluster()
                .setClusterName(runContext.render(clusterName))
                .setSparkVersion(runContext.render(sparkVersion))
                .setNodeTypeId(runContext.render(nodeTypeId))
                .setAutoterminationMinutes(autoTerminationMinutes)
                .setNumWorkers(numWorkers);
        if (minWorkers != null && maxWorkers != null) {
            createCluster.setAutoscale(new AutoScale().setMinWorkers(minWorkers).setMaxWorkers(maxWorkers));
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
        @Schema(title = "The cluster identifier.")
        private String clusterId;

        @Schema(title = "The cluster URI on the Databricks console.")
        private URI clusterURI;

        @Schema(title = "The cluster state.")
        private State clusterState;
    }
}
