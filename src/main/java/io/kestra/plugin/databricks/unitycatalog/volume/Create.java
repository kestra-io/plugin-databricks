package io.kestra.plugin.databricks.unitycatalog.volume;

import java.util.Map;

import com.databricks.sdk.service.catalog.CreateVolumeRequestContent;
import com.databricks.sdk.service.catalog.VolumeType;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
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
            title = "Create a managed volume",
            full = true,
            code = """
                id: databricks_uc_volume_create
                namespace: company.team

                tasks:
                  - id: create_volume
                    type: io.kestra.plugin.databricks.unitycatalog.volume.Create
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                    schemaName: landing_zone
                    name: raw_files
                    volumeType: MANAGED
                """
        )
    }
)
@Schema(
    title = "Create a Unity Catalog volume",
    description = "Creates a managed or external volume in an existing schema. External volumes require a `storageLocation`."
)
public class Create extends AbstractTask implements RunnableTask<Create.Output> {
    @Schema(
        title = "Catalog name",
        description = "Name of the parent catalog."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> catalogName;

    @Schema(
        title = "Schema name",
        description = "Name of the parent schema."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> schemaName;

    @Schema(
        title = "Volume name"
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> name;

    @Schema(
        title = "Volume type",
        description = "`MANAGED` (default) or `EXTERNAL`."
    )
    @PluginProperty(group = "main")
    @Builder.Default
    private Property<VolumeType> volumeType = Property.ofValue(VolumeType.MANAGED);

    @Schema(
        title = "Storage location",
        description = "Cloud storage location of an `EXTERNAL` volume. Not allowed for `MANAGED` volumes."
    )
    @PluginProperty(group = "main")
    private Property<String> storageLocation;

    @Schema(
        title = "Description of the volume"
    )
    @PluginProperty(group = "main")
    private Property<String> comment;

    @Override
    public Create.Output run(RunContext runContext) throws Exception {
        var rVolumeType = runContext.render(volumeType).as(VolumeType.class).orElse(VolumeType.MANAGED);
        var rStorageLocation = runContext.render(storageLocation).as(String.class);
        if (rVolumeType == VolumeType.EXTERNAL && rStorageLocation.isEmpty()) {
            throw new IllegalArgumentException("`storageLocation` is required to create an EXTERNAL volume");
        }
        if (rVolumeType == VolumeType.MANAGED && rStorageLocation.isPresent()) {
            throw new IllegalArgumentException("`storageLocation` can only be set on an EXTERNAL volume");
        }

        var request = new CreateVolumeRequestContent()
            .setCatalogName(runContext.render(catalogName).as(String.class).orElseThrow())
            .setSchemaName(runContext.render(schemaName).as(String.class).orElseThrow())
            .setName(runContext.render(name).as(String.class).orElseThrow())
            .setVolumeType(rVolumeType);
        rStorageLocation.ifPresent(request::setStorageLocation);
        runContext.render(comment).as(String.class).ifPresent(request::setComment);

        var info = workspaceClient(runContext).volumes().create(request);
        runContext.logger().info("Created volume '{}'", info.getFullName());

        return Output.builder()
            .name(info.getName())
            .fullName(info.getFullName())
            .owner(info.getOwner())
            .volume(UnityCatalogUtils.toMap(info))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Volume name")
        private final String name;

        @Schema(title = "Fully qualified volume name (`catalog.schema.volume`)")
        private final String fullName;

        @Schema(title = "Volume owner")
        private final String owner;

        @Schema(title = "Full volume details as returned by Databricks")
        private final Map<String, Object> volume;
    }
}
