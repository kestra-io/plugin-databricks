package io.kestra.plugin.databricks.dbfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
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
            title = "Download a file from the Databricks File System",
            code = """
                id: downloadFile
                type: io.kestra.plugin.databricks.dbfs.Download
                authentication:
                  token: <your-token>
                host: <your-host>
                from: /Share/myFile.txt"""
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
    title = "Download a file",
    description = "The file can be of any size. The task will download the file in chunks of 1MB."
)
public class Download extends AbstractTask implements RunnableTask<Download.Output> {
    @Schema(
        title = "The file to download"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    private String from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var path = runContext.render(from);
        File tempFile = runContext.tempFile(runContext.fileExtension(path)).toFile();
        var workspace = workspaceClient(runContext);

        try (InputStream in = workspace.dbfs().open(path);
             OutputStream out = new FileOutputStream(tempFile)) {
            int size = IOUtils.copy(in, out);
            runContext.metric(Counter.of("file.size", size));
            var uri = runContext.putTempFile(tempFile);
            return Output.builder().uri(uri).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The URL of the file downloaded to Kestra's internal storage "
        )
        private final URI uri;
    }
}
