package io.kestra.plugin.databricks.unitycatalog.volume;

import java.util.Map;

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
            title = "Get a Unity Catalog volume",
            full = true,
            code = """
                id: databricks_uc_volume_get
                namespace: company.team

                tasks:
                  - id: get_volume
                    type: io.kestra.plugin.databricks.unitycatalog.volume.Get
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    fullName: main.landing_zone.raw_files
                """
        )
    }
)
@Schema(
    title = "Get a Unity Catalog volume",
    description = "Fetches the details of a volume by its full name."
)
public class Get extends AbstractTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "Volume full name",
        description = "Fully qualified volume name, in the form `catalog.schema.volume`."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();
        var info = workspaceClient(runContext).volumes().read(rFullName);

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
