package io.kestra.plugin.databricks.unitycatalog.table;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogTestSupport;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class TableTasksTest {
    @Inject
    private RunContextFactory runContextFactory;

    private TablesAPI api;

    private WorkspaceClient client;

    @BeforeEach
    void setUp() {
        api = mock(TablesAPI.class);
        client = mock(WorkspaceClient.class);
        when(client.tables()).thenReturn(api);
    }

    private io.kestra.plugin.databricks.unitycatalog.table.List.ListBuilder<?, ?> listTask() {
        return io.kestra.plugin.databricks.unitycatalog.table.List.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.databricks.unitycatalog.table.List.class.getName())
            .catalogName(Property.ofValue("main"))
            .schemaName(Property.ofValue("landing_zone"));
    }

    private void twoTables() {
        when(api.list(any(ListTablesRequest.class))).thenReturn(
            java.util.List.of(
                new TableInfo().setName("events").setFullName("main.landing_zone.events"),
                new TableInfo().setName("users").setFullName("main.landing_zone.users")
            )
        );
    }

    @Test
    void list() throws Exception {
        twoTables();

        var task = listTask().build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(ListTablesRequest.class);
        verify(api).list(request.capture());
        assertThat(request.getValue().getCatalogName(), is("main"));
        assertThat(request.getValue().getSchemaName(), is("landing_zone"));
        assertThat(request.getValue().getOmitColumns(), nullValue());
        assertThat(output.getSize(), is(2));
        assertThat(output.getTables().get(0).get("full_name"), is("main.landing_zone.events"));
        assertThat(output.getTable(), nullValue());
        assertThat(output.getUri(), nullValue());
    }

    @Test
    void listOmitsColumns() throws Exception {
        twoTables();

        var task = listTask().omitColumns(Property.ofValue(true)).build();
        UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(ListTablesRequest.class);
        verify(api).list(request.capture());
        assertThat(request.getValue().getOmitColumns(), is(true));
    }

    @Test
    void listFetchOne() throws Exception {
        twoTables();

        var task = listTask().fetchType(Property.ofValue(FetchType.FETCH_ONE)).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getSize(), is(1));
        assertThat(output.getTable().get("full_name"), is("main.landing_zone.events"));
        assertThat(output.getTables(), nullValue());
    }

    @Test
    void listStore() throws Exception {
        twoTables();

        var task = listTask().fetchType(Property.ofValue(FetchType.STORE)).build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = UnityCatalogTestSupport.withClient(task, client).run(runContext);

        assertThat(output.getSize(), is(2));
        assertThat(output.getTables(), nullValue());
        assertThat(output.getUri(), notNullValue());
        try (var in = runContext.storage().getFile(output.getUri())) {
            var stored = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(stored, containsString("main.landing_zone.events"));
            assertThat(stored, containsString("main.landing_zone.users"));
        }
    }

    @Test
    void listNone() throws Exception {
        twoTables();

        var task = listTask().fetchType(Property.ofValue(FetchType.NONE)).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getSize(), is(2));
        assertThat(output.getTables(), nullValue());
        assertThat(output.getTable(), nullValue());
        assertThat(output.getUri(), nullValue());
    }

    @Test
    void get() throws Exception {
        when(api.get("main.landing_zone.events")).thenReturn(new TableInfo().setName("events").setFullName("main.landing_zone.events"));

        var task = Get.builder().id(IdUtils.create()).type(Get.class.getName()).fullName(Property.ofValue("main.landing_zone.events")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getName(), is("events"));
        assertThat(output.getFullName(), is("main.landing_zone.events"));
    }

    @Test
    void delete() throws Exception {
        var task = Delete.builder().id(IdUtils.create()).type(Delete.class.getName()).fullName(Property.ofValue("main.landing_zone.events")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        verify(api).delete("main.landing_zone.events");
        assertThat(output, nullValue());
    }
}
