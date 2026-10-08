package io.kestra.plugin.databricks.unitycatalog.volume;

import java.util.Map;

import com.databricks.sdk.service.catalog.UpdateVolumeRequestContent;

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
            title = "Change the owner of a volume",
            full = true,
            code = """
                id: databricks_uc_volume_update
                namespace: company.team

                tasks:
                  - id: update_volume
                    type: io.kestra.plugin.databricks.unitycatalog.volume.Update
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    fullName: main.landing_zone.raw_files
                    owner: data-platform
                """
        )
    }
)
@Schema(
    title = "Update a Unity Catalog volume",
    description = "Updates the comment, owner or name of a volume. Only the properties you set are changed."
)
public class Update extends AbstractTask implements RunnableTask<Update.Output> {
    @Schema(
        title = "Volume full name",
        description = "Fully qualified volume name, in the form `catalog.schema.volume`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Schema(
        title = "New volume name",
        description = "Rename the volume to this value."
    )
    @PluginProperty(group = "main")
    private Property<String> newName;

    @Schema(
        title = "Description of the volume"
    )
    @PluginProperty(group = "main")
    private Property<String> comment;

    @Schema(
        title = "Volume owner",
        description = "Username or group name of the new owner."
    )
    @PluginProperty(group = "main")
    private Property<String> owner;

    @Override
    public Update.Output run(RunContext runContext) throws Exception {
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();
        var request = new UpdateVolumeRequestContent().setName(rFullName);
        runContext.render(newName).as(String.class).ifPresent(request::setNewName);
        runContext.render(comment).as(String.class).ifPresent(request::setComment);
        runContext.render(owner).as(String.class).ifPresent(request::setOwner);

        var info = workspaceClient(runContext).volumes().update(request);
        runContext.logger().info("Updated volume '{}'", info.getFullName());

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
