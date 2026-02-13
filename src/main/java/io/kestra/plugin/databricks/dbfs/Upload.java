package io.kestra.plugin.databricks.dbfs;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
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
            title = "Upload a file to the Databricks File System.",
            full = true,
            code = """
                id: databricks_dbfs_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE
                    description: File to be uploaded to DBFS

                tasks:
                  - id: upload_file
                    type: io.kestra.plugin.databricks.dbfs.Upload
                    authentication:
                    token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    from: "{{ inputs.file }}"
                    to: /Share/myFile.parquet
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
    title = "Upload a file to DBFS",
    description = "Streams a file from Kestra internal storage to DBFS using 1 MB chunks; suited for large files."
)
public class Upload extends AbstractTask implements RunnableTask<VoidOutput> {
    @Schema(
        title = "Source file URI",
        description = "Internal storage URI of the file to upload (kestra:// ...)"
    )
    @NotNull
    @PluginProperty(internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Destination DBFS path",
        description = "Absolute DBFS path such as /mnt/volume/file.parquet"
    )
    @NotNull
    private Property<String> to;

    @Override
    public VoidOutput run(RunContext runContext) throws Exception {
        var path = runContext.render(to).as(String.class).orElseThrow();
        var workspace = workspaceClient(runContext);

        try (InputStream in = runContext.storage().getFile(URI.create(runContext.render(from).as(String.class).orElseThrow()));
             OutputStream out = workspace.dbfs().getOutputStream(path)) {
            int size = IOUtils.copy(in, out);
            runContext.metric(Counter.of("file.size", size));
        }
        return null;
    }
}
