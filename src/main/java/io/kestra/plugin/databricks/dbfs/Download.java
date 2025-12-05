package io.kestra.plugin.databricks.dbfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.utils.FileUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
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
            title = "Download a file from the Databricks File System.",
            full = true,
            code = """
                id: databricks_dbfs_download
                namespace: company.team

                tasks:
                  - id: download_file
                    type: io.kestra.plugin.databricks.dbfs.Download
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: <your-host>
                    from: /Share/myFile.txt
                """
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
    title = "Download a file from Databricks File System.",
    description = "The file can be of any size. The task will download the file in chunks of 1MB."
)
public class Download extends AbstractTask implements RunnableTask<Download.Output> {
    @Schema(
        title = "The file to download."
    )
    @NotNull
    private Property<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String path = runContext.render(from).as(String.class).orElseThrow();
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(path)).toFile();
        var workspace = workspaceClient(runContext);

        try (InputStream in = workspace.dbfs().open(path);
             OutputStream out = new FileOutputStream(tempFile)) {
            int size = IOUtils.copy(in, out);
            runContext.metric(Counter.of("file.size", size));
            var uri = runContext.storage().putFile(tempFile);
            return Output.builder().uri(uri).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The URI of the file downloaded to Kestra's internal storage."
        )
        private final URI uri;
    }
}
