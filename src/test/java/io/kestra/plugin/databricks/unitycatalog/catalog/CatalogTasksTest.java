package io.kestra.plugin.databricks.unitycatalog.catalog;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.CreateCatalog;
import com.databricks.sdk.service.catalog.DeleteCatalogRequest;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.UpdateCatalog;

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
class CatalogTasksTest {
    @Inject
    private RunContextFactory runContextFactory;

    private CatalogsAPI api;

    private WorkspaceClient client;

    @BeforeEach
    void setUp() {
        api = mock(CatalogsAPI.class);
        client = mock(WorkspaceClient.class);
        when(client.catalogs()).thenReturn(api);
    }

    @Test
    void create() throws Exception {
        when(api.create(any(CreateCatalog.class))).thenReturn(new CatalogInfo().setName("analytics").setOwner("me"));

        var task = Create.builder()
            .id(IdUtils.create())
            .type(Create.class.getName())
            .name(Property.ofValue("analytics"))
            .comment(Property.ofValue("Analytics data products"))
            .properties(Property.ofValue(Map.of("team", "data")))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(CreateCatalog.class);
        verify(api).create(request.capture());
        assertThat(request.getValue().getName(), is("analytics"));
        assertThat(request.getValue().getComment(), is("Analytics data products"));
        assertThat(request.getValue().getProperties(), is(Map.of("team", "data")));
        assertThat(request.getValue().getStorageRoot(), nullValue());
        assertThat(output.getName(), is("analytics"));
        assertThat(output.getOwner(), is("me"));
        assertThat(output.getCatalog().get("name"), is("analytics"));
    }

    @Test
    void get() throws Exception {
        when(api.get("analytics")).thenReturn(new CatalogInfo().setName("analytics").setOwner("me"));

        var task = Get.builder().id(IdUtils.create()).type(Get.class.getName()).name(Property.ofValue("analytics")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getName(), is("analytics"));
        assertThat(output.getOwner(), is("me"));
    }

    @Test
    void update() throws Exception {
        when(api.update(any(UpdateCatalog.class))).thenReturn(new CatalogInfo().setName("analytics").setOwner("data-platform"));

        var task = Update.builder()
            .id(IdUtils.create())
            .type(Update.class.getName())
            .name(Property.ofValue("analytics"))
            .owner(Property.ofValue("data-platform"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(UpdateCatalog.class);
        verify(api).update(request.capture());
        assertThat(request.getValue().getName(), is("analytics"));
        assertThat(request.getValue().getOwner(), is("data-platform"));
        assertThat(request.getValue().getComment(), nullValue());
        assertThat(output.getOwner(), is("data-platform"));
    }

    @Test
    void delete() throws Exception {
        var task = Delete.builder()
            .id(IdUtils.create())
            .type(Delete.class.getName())
            .name(Property.ofValue("analytics"))
            .force(Property.ofValue(true))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(DeleteCatalogRequest.class);
        verify(api).delete(request.capture());
        assertThat(request.getValue().getName(), is("analytics"));
        assertThat(request.getValue().getForce(), is(true));
        assertThat(output, nullValue());
    }

    @Test
    void list() throws Exception {
        when(api.list(any(ListCatalogsRequest.class))).thenReturn(
            List.of(new CatalogInfo().setName("main"), new CatalogInfo().setName("analytics"))
        );

        var task = io.kestra.plugin.databricks.unitycatalog.catalog.List.builder().id(IdUtils.create()).type(io.kestra.plugin.databricks.unitycatalog.catalog.List.class.getName()).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getSize(), is(2));
        assertThat(output.getCatalogs().get(1).get("name"), is("analytics"));
    }
}
