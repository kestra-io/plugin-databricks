package io.kestra.plugin.databricks.unitycatalog.volume;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;

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
            title = "Download a file from a Unity Catalog volume",
            full = true,
            code = """
                id: databricks_uc_volume_download
                namespace: company.team

                tasks:
                  - id: download_file
                    type: io.kestra.plugin.databricks.unitycatalog.volume.Download
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    volumePath: /Volumes/main/landing_zone/raw_files/data.csv
                """
        )
    },
    metrics = {
        @Metric(
            name = "file.size",
            type = "counter",
            description = "The size of the downloaded file, in bytes"
        )
    }
)
@Schema(
    title = "Download a file from a Unity Catalog volume",
    description = "Downloads a file from a Unity Catalog volume using the Databricks Files API and stores it in Kestra internal storage. This is the replacement for the legacy DBFS download."
)
public class Download extends AbstractTask implements RunnableTask<Download.Output> {
    @Schema(
        title = "Volume file path",
        description = "Absolute path of the file to download, in the form `/Volumes/<catalog>/<schema>/<volume>/<path>`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> volumePath;

    @Override
    public Download.Output run(RunContext runContext) throws Exception {
        var rVolumePath = runContext.render(volumePath).as(String.class).orElseThrow();
        UnityCatalogUtils.validateVolumePath(rVolumePath);
        File tempFile = runContext.workingDir().createTempFile(FileUtils.getExtension(rVolumePath)).toFile();

        try (
            InputStream in = workspaceClient(runContext).files().download(rVolumePath).getContents();
            OutputStream out = new FileOutputStream(tempFile)
        ) {
            long size = IOUtils.copyLarge(in, out);
            runContext.metric(Counter.of("file.size", size));
            runContext.logger().info("Downloaded {} byte(s) from '{}'", size, rVolumePath);

            return Output.builder()
                .uri(runContext.storage().putFile(tempFile))
                .size(size)
                .build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Internal storage URI of the downloaded file")
        private final URI uri;

        @Schema(title = "Size of the downloaded file, in bytes")
        private final Long size;
    }
}
