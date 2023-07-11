package io.kestra.plugin.databricks.cluster;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

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
                id: deleteCluster
                type: io.kestra.plugin.databricks.cluster.DeleteCluster
                token: <your-token>
                host: <your-host>
                clusterId: <your-cluster>
                """
        )
    }
)
public class DeleteCluster extends AbstractTask implements RunnableTask<VoidOutput> {
    @PluginProperty(dynamic = true)
    @NotNull
    private String clusterId;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var deleteCluster = new com.databricks.sdk.service.compute.DeleteCluster()
            .setClusterId(runContext.render(clusterId));

        workspaceClient(runContext).clusters().delete(deleteCluster).get(); //TODO timeout

        return null;
    }
}
