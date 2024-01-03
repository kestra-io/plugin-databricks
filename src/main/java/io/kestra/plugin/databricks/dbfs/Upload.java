package io.kestra.plugin.databricks.dbfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;
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
            title = "Upload a file to the Databricks File System",
            code = """
                id: uploadFile
                type: io.kestra.plugin.databricks.dbfs.Upload
                authentication:
                  token: <your-token>
                host: <your-host>
                from: "{{inputs.someFile}}"
                to: /Share/myFile.txt"""
        )
    },
    metrics = {
        @Metric(
            name = "file.size",
            type = "counter",
            description = "The file size"
        )
    }
)
@Schema(
    title = "Upload a file",
    description = "The file can be of any size. The task will upload the file in chunks of 1MB."
)
public class Upload extends AbstractTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "The file to upload",
        description = "Must be a file from Kestra internal storage."
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Schema(
        title = "The destination path"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String to;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var path = runContext.render(to);
        var workspace = workspaceClient(runContext);

        try (InputStream in = runContext.uriToInputStream(URI.create(runContext.render(from)));
             OutputStream out = workspace.dbfs().getOutputStream(path)) {
            int size = IOUtils.copy(in, out);
            runContext.metric(Counter.of("file.size", size));
        }
        return null;
    }
}
