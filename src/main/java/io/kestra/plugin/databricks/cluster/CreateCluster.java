package io.kestra.plugin.databricks.cluster;

import com.databricks.sdk.service.compute.ClusterSource;
import com.databricks.sdk.service.compute.State;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.net.URI;
import javax.validation.constraints.NotNull;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = """
                id: createCluster
                type: io.kestra.plugin.databricks.cluster.CreateCluster
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
public class CreateCluster extends AbstractTask implements RunnableTask<CreateCluster.Output> {
    @NotNull
    @PluginProperty(dynamic = true)
    private String clusterName;

    @NotNull
    @PluginProperty
    private String sparkVersion;

    @PluginProperty
    private String nodeTypeId;

    @PluginProperty
    private Long autoTerminationMinutes;

    @PluginProperty
    private Long numWorkers;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var createCluster = new com.databricks.sdk.service.compute.CreateCluster()
                .setClusterName(runContext.render(clusterName))
                .setSparkVersion(sparkVersion)
                .setNodeTypeId(nodeTypeId)
                .setAutoterminationMinutes(autoTerminationMinutes)
                .setNumWorkers(numWorkers);

        var workspaceClient = workspaceClient(runContext);
        var response = workspaceClient.clusters().create(createCluster).get(); //TODO timeout
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
        private String clusterId;

        private URI clusterURI;

        private State clusterState;
    }
}
