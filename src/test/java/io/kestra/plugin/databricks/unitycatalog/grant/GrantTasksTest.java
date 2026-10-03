package io.kestra.plugin.databricks.unitycatalog.grant;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.GetGrantRequest;
import com.databricks.sdk.service.catalog.GetPermissionsResponse;
import com.databricks.sdk.service.catalog.GrantsAPI;
import com.databricks.sdk.service.catalog.Privilege;
import com.databricks.sdk.service.catalog.PrivilegeAssignment;
import com.databricks.sdk.service.catalog.UpdatePermissions;
import com.databricks.sdk.service.catalog.UpdatePermissionsResponse;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogTestSupport;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class GrantTasksTest {
    @Inject
    private RunContextFactory runContextFactory;

    private GrantsAPI api;

    private WorkspaceClient client;

    @BeforeEach
    void setUp() {
        api = mock(GrantsAPI.class);
        client = mock(WorkspaceClient.class);
        when(client.grants()).thenReturn(api);
    }

    @Test
    void get() throws Exception {
        when(api.get(any(GetGrantRequest.class))).thenReturn(
            new GetPermissionsResponse().setPrivilegeAssignments(List.of(new PrivilegeAssignment().setPrincipal("data-analysts").setPrivileges(List.of(Privilege.SELECT))))
        );

        var task = Get.builder()
            .id(IdUtils.create())
            .type(Get.class.getName())
            .securableType(Property.ofValue("SCHEMA"))
            .fullName(Property.ofValue("main.landing_zone"))
            .principal(Property.ofValue("data-analysts"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(GetGrantRequest.class);
        verify(api).get(request.capture());
        assertThat(request.getValue().getSecurableType(), is("schema"));
        assertThat(request.getValue().getFullName(), is("main.landing_zone"));
        assertThat(request.getValue().getPrincipal(), is("data-analysts"));
        assertThat(output.getPrivilegeAssignments().size(), is(1));
        assertThat(output.getPrivilegeAssignments().get(0).get("principal"), is("data-analysts"));
    }

    @Test
    void update() throws Exception {
        when(api.update(any(UpdatePermissions.class))).thenReturn(new UpdatePermissionsResponse());

        var task = Update.builder()
            .id(IdUtils.create())
            .type(Update.class.getName())
            .securableType(Property.ofValue("schema"))
            .fullName(Property.ofValue("main.landing_zone"))
            .changes(
                List.of(
                    Update.Change.builder()
                        .principal(Property.ofValue("data-analysts"))
                        .add(Property.ofValue(List.of("use_schema", "SELECT")))
                        .remove(Property.ofValue(List.of("MODIFY")))
                        .build(),
                    Update.Change.builder().principal(Property.ofValue("interns")).remove(Property.ofValue(List.of("ALL_PRIVILEGES"))).build()
                )
            )
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(UpdatePermissions.class);
        verify(api).update(request.capture());
        assertThat(request.getValue().getSecurableType(), is("schema"));
        assertThat(request.getValue().getFullName(), is("main.landing_zone"));

        var changes = List.copyOf(request.getValue().getChanges());
        assertThat(changes.get(0).getPrincipal(), is("data-analysts"));
        assertThat(changes.get(0).getAdd(), contains(Privilege.USE_SCHEMA, Privilege.SELECT));
        assertThat(changes.get(0).getRemove(), contains(Privilege.MODIFY));
        assertThat(changes.get(1).getAdd(), nullValue());
        assertThat(changes.get(1).getRemove(), contains(Privilege.ALL_PRIVILEGES));
        assertThat(output.getPrivilegeAssignments().isEmpty(), is(true));
    }

    @Test
    void updateRejectsUnknownPrivilege() throws Exception {
        var task = Update.builder()
            .id(IdUtils.create())
            .type(Update.class.getName())
            .securableType(Property.ofValue("SCHEMA"))
            .fullName(Property.ofValue("main.landing_zone"))
            .changes(List.of(Update.Change.builder().principal(Property.ofValue("data-analysts")).add(Property.ofValue(List.of("NOT_A_PRIVILEGE"))).build()))
            .build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(exception.getMessage(), containsString("NOT_A_PRIVILEGE"));
        verify(api, never()).update(any(UpdatePermissions.class));
    }
}
