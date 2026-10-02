package io.kestra.plugin.databricks.unitycatalog.schema;

import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CreateSchema;
import com.databricks.sdk.service.catalog.DeleteSchemaRequest;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.UpdateSchema;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogTestSupport;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class SchemaTasksTest {
    @Inject
    private RunContextFactory runContextFactory;

    private SchemasAPI api;

    private WorkspaceClient client;

    @BeforeEach
    void setUp() {
        api = mock(SchemasAPI.class);
        client = mock(WorkspaceClient.class);
        when(client.schemas()).thenReturn(api);
    }

    @Test
    void create() throws Exception {
        when(api.create(any(CreateSchema.class))).thenReturn(new SchemaInfo().setName("landing_zone").setFullName("main.landing_zone").setOwner("me"));

        var task = Create.builder()
            .id(IdUtils.create())
            .type(Create.class.getName())
            .catalogName(Property.ofValue("main"))
            .name(Property.ofValue("landing_zone"))
            .comment(Property.ofValue("Raw data"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(CreateSchema.class);
        verify(api).create(request.capture());
        assertThat(request.getValue().getCatalogName(), is("main"));
        assertThat(request.getValue().getName(), is("landing_zone"));
        assertThat(request.getValue().getComment(), is("Raw data"));
        assertThat(output.getFullName(), is("main.landing_zone"));
    }

    @Test
    void get() throws Exception {
        when(api.get("main.landing_zone")).thenReturn(new SchemaInfo().setName("landing_zone").setFullName("main.landing_zone"));

        var task = Get.builder().id(IdUtils.create()).type(Get.class.getName()).fullName(Property.ofValue("main.landing_zone")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getName(), is("landing_zone"));
        assertThat(output.getSchema().get("full_name"), is("main.landing_zone"));
    }

    @Test
    void update() throws Exception {
        when(api.update(any(UpdateSchema.class))).thenReturn(new SchemaInfo().setName("landing_zone").setFullName("main.landing_zone").setOwner("data-platform"));

        var task = Update.builder()
            .id(IdUtils.create())
            .type(Update.class.getName())
            .fullName(Property.ofValue("main.landing_zone"))
            .owner(Property.ofValue("data-platform"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(UpdateSchema.class);
        verify(api).update(request.capture());
        assertThat(request.getValue().getFullName(), is("main.landing_zone"));
        assertThat(request.getValue().getOwner(), is("data-platform"));
        assertThat(request.getValue().getNewName(), nullValue());
        assertThat(output.getOwner(), is("data-platform"));
    }

    @Test
    void delete() throws Exception {
        var task = Delete.builder().id(IdUtils.create()).type(Delete.class.getName()).fullName(Property.ofValue("main.landing_zone")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(DeleteSchemaRequest.class);
        verify(api).delete(request.capture());
        assertThat(request.getValue().getFullName(), is("main.landing_zone"));
        assertThat(request.getValue().getForce(), nullValue());
        assertThat(output, nullValue());
    }

    @Test
    void list() throws Exception {
        when(api.list("main")).thenReturn(java.util.List.of(new SchemaInfo().setName("landing_zone"), new SchemaInfo().setName("default")));

        var task = io.kestra.plugin.databricks.unitycatalog.schema.List.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.databricks.unitycatalog.schema.List.class.getName())
            .catalogName(Property.ofValue("main"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getSize(), is(2));
    }
}
