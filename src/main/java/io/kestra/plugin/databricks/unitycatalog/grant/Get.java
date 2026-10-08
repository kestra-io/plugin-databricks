package io.kestra.plugin.databricks.unitycatalog.grant;

import java.util.List;
import java.util.Map;

import com.databricks.sdk.service.catalog.GetGrantRequest;

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
            title = "Get the permissions of a schema",
            full = true,
            code = """
                id: databricks_uc_grant_get
                namespace: company.team

                tasks:
                  - id: get_grants
                    type: io.kestra.plugin.databricks.unitycatalog.grant.Get
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    securableType: SCHEMA
                    fullName: main.landing_zone
                """
        )
    }
)
@Schema(
    title = "Get Unity Catalog permissions",
    description = "Gets the permissions granted directly on a securable object, optionally restricted to one principal. Inherited permissions are not included."
)
public class Get extends AbstractTask implements RunnableTask<Get.Output> {
    @Schema(
        title = "Securable type",
        description = "Type of the object the permissions apply to, for example `CATALOG`, `SCHEMA`, `TABLE`, `VOLUME`, `EXTERNAL_LOCATION` or `STORAGE_CREDENTIAL`. Case-insensitive."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> securableType;

    @Schema(
        title = "Securable full name",
        description = "Full name of the object: `catalog`, `catalog.schema`, `catalog.schema.table` or `catalog.schema.volume` depending on the securable type."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> fullName;

    @Schema(
        title = "Principal",
        description = "Only return the privileges of this user, group or service principal."
    )
    @PluginProperty(group = "main")
    private Property<String> principal;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        var request = new GetGrantRequest()
            .setSecurableType(UnityCatalogUtils.securableType(runContext.render(securableType).as(String.class).orElseThrow()))
            .setFullName(runContext.render(fullName).as(String.class).orElseThrow());
        runContext.render(principal).as(String.class).ifPresent(request::setPrincipal);

        var response = workspaceClient(runContext).grants().get(request);

        return Output.builder()
            .privilegeAssignments(UnityCatalogUtils.toMaps(response.getPrivilegeAssignments()))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Privileges granted on the object, grouped by principal")
        private final List<Map<String, Object>> privilegeAssignments;
    }
}
