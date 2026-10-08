package io.kestra.plugin.databricks.unitycatalog.volume;

import java.net.URI;

import org.apache.commons.io.input.CountingInputStream;

import com.databricks.sdk.service.files.UploadRequest;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
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
            title = "Upload a file to a Unity Catalog volume",
            full = true,
            code = """
                id: databricks_uc_volume_upload
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE
                    description: File to upload to the volume

                tasks:
                  - id: upload_file
                    type: io.kestra.plugin.databricks.unitycatalog.volume.Upload
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    from: "{{ inputs.file }}"
                    volumePath: /Volumes/main/landing_zone/raw_files/data.csv
                    overwrite: true
                """
        )
    },
    metrics = {
        @Metric(
            name = "file.size",
            type = "counter",
            description = "The size of the uploaded file, in bytes"
        )
    }
)
@Schema(
    title = "Upload a file to a Unity Catalog volume",
    description = "Streams a file from Kestra internal storage to a Unity Catalog volume using the Databricks Files API. The volume must already exist. This is the replacement for the legacy DBFS upload."
)
public class Upload extends AbstractTask implements RunnableTask<Upload.Output> {
    @Schema(
        title = "Source file URI",
        description = "Internal storage URI of the file to upload (`kestra://...`)."
    )
    @NotNull
    @PluginProperty(group = "main", internalStorageURI = true)
    private Property<String> from;

    @Schema(
        title = "Volume file path",
        description = "Absolute path of the destination file, in the form `/Volumes/<catalog>/<schema>/<volume>/<path>`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> volumePath;

    @Schema(
        title = "Overwrite existing file",
        description = "Replace the file if it already exists. Defaults to `false`, in which case the upload fails when the file exists."
    )
    @PluginProperty(group = "main")
    private Property<Boolean> overwrite;

    @Override
    public Upload.Output run(RunContext runContext) throws Exception {
        var rFrom = runContext.render(from).as(String.class).orElseThrow();
        var rVolumePath = runContext.render(volumePath).as(String.class).orElseThrow();
        UnityCatalogUtils.validateVolumePath(rVolumePath);
        var rOverwrite = runContext.render(overwrite).as(Boolean.class).orElse(false);

        try (var in = new CountingInputStream(runContext.storage().getFile(URI.create(rFrom)))) {
            workspaceClient(runContext).files().upload(
                new UploadRequest().setFilePath(rVolumePath).setContents(in).setOverwrite(rOverwrite)
            );

            long size = in.getByteCount();
            runContext.metric(Counter.of("file.size", size));
            runContext.logger().info("Uploaded {} byte(s) to '{}'", size, rVolumePath);

            return Output.builder()
                .volumePath(rVolumePath)
                .size(size)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Path of the uploaded file in the volume")
        private final String volumePath;

        @Schema(title = "Size of the uploaded file, in bytes")
        private final Long size;
    }
}
