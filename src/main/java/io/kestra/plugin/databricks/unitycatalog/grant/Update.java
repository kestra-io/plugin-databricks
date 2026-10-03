package io.kestra.plugin.databricks.unitycatalog.grant;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.databricks.sdk.service.catalog.PermissionsChange;
import com.databricks.sdk.service.catalog.Privilege;
import com.databricks.sdk.service.catalog.UpdatePermissions;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogUtils;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotEmpty;
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
            title = "Grant SELECT on a schema to a group",
            full = true,
            code = """
                id: databricks_uc_grant_update
                namespace: company.team

                tasks:
                  - id: grant_select
                    type: io.kestra.plugin.databricks.unitycatalog.grant.Update
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    securableType: SCHEMA
                    fullName: main.landing_zone
                    changes:
                      - principal: data-analysts
                        add:
                          - USE_SCHEMA
                          - SELECT
                """
        )
    }
)
@Schema(
    title = "Update Unity Catalog permissions",
    description = "Grants and revokes privileges on a securable object. Each change targets one principal and can add and remove privileges in the same call."
)
public class Update extends AbstractTask implements RunnableTask<Update.Output> {
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
        title = "Permission changes",
        description = "List of changes to apply."
    )
    @NotNull
    @NotEmpty
    @PluginProperty(group = "main")
    private List<Change> changes;

    @Override
    public Update.Output run(RunContext runContext) throws Exception {
        var rChanges = new ArrayList<PermissionsChange>();
        for (var change : changes) {
            rChanges.add(
                new PermissionsChange()
                    .setPrincipal(runContext.render(change.getPrincipal()).as(String.class).orElseThrow())
                    .setAdd(privileges(runContext, change.getAdd()))
                    .setRemove(privileges(runContext, change.getRemove()))
            );
        }

        var rSecurableType = UnityCatalogUtils.securableType(runContext.render(securableType).as(String.class).orElseThrow());
        var rFullName = runContext.render(fullName).as(String.class).orElseThrow();
        var response = workspaceClient(runContext).grants().update(
            new UpdatePermissions().setSecurableType(rSecurableType).setFullName(rFullName).setChanges(rChanges)
        );
        runContext.logger().info("Updated permissions on {} '{}'", rSecurableType, rFullName);

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

    @Builder
    @Getter
    public static class Change {
        @Schema(
            title = "Principal",
            description = "User, group or service principal the change applies to."
        )
        @NotNull
        private Property<String> principal;

        @Schema(
            title = "Privileges to grant",
            description = "For example `SELECT`, `MODIFY`, `USE_SCHEMA`, `READ_VOLUME` or `ALL_PRIVILEGES`."
        )
        private Property<List<String>> add;

        @Schema(
            title = "Privileges to revoke",
            description = "Same values as `add`."
        )
        private Property<List<String>> remove;
    }

    private static Collection<Privilege> privileges(RunContext runContext, Property<List<String>> property) throws Exception {
        var values = runContext.render(property).asList(String.class);
        if (values.isEmpty()) {
            return null;
        }

        var privileges = new ArrayList<Privilege>();
        for (var value : values) {
            try {
                privileges.add(Privilege.valueOf(value.trim().toUpperCase(Locale.ROOT).replace(' ', '_')));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Unknown Unity Catalog privilege '" + value + "'", e);
            }
        }
        return privileges;
    }
}
