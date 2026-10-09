package io.kestra.plugin.databricks.unitycatalog.volume;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CreateVolumeRequestContent;
import com.databricks.sdk.service.catalog.UpdateVolumeRequestContent;
import com.databricks.sdk.service.catalog.VolumeInfo;
import com.databricks.sdk.service.catalog.VolumeType;
import com.databricks.sdk.service.catalog.VolumesAPI;
import com.databricks.sdk.service.files.DownloadResponse;
import com.databricks.sdk.service.files.FilesAPI;
import com.databricks.sdk.service.files.UploadRequest;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.unitycatalog.UnityCatalogTestSupport;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@KestraTest
class VolumeTasksTest {
    private static final String VOLUME_FILE = "/Volumes/main/landing_zone/raw_files/data.csv";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private VolumesAPI volumes;

    private FilesAPI files;

    private WorkspaceClient client;

    @BeforeEach
    void setUp() {
        volumes = mock(VolumesAPI.class);
        files = mock(FilesAPI.class);
        client = mock(WorkspaceClient.class);
        when(client.volumes()).thenReturn(volumes);
        when(client.files()).thenReturn(files);
    }

    @Test
    void createManagedByDefault() throws Exception {
        when(volumes.create(any(CreateVolumeRequestContent.class))).thenReturn(
            new VolumeInfo().setName("raw_files").setFullName("main.landing_zone.raw_files").setVolumeType(VolumeType.MANAGED)
        );

        var task = Create.builder()
            .id(IdUtils.create())
            .type(Create.class.getName())
            .catalogName(Property.ofValue("main"))
            .schemaName(Property.ofValue("landing_zone"))
            .name(Property.ofValue("raw_files"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(CreateVolumeRequestContent.class);
        verify(volumes).create(request.capture());
        assertThat(request.getValue().getVolumeType(), is(VolumeType.MANAGED));
        assertThat(request.getValue().getCatalogName(), is("main"));
        assertThat(request.getValue().getSchemaName(), is("landing_zone"));
        assertThat(request.getValue().getName(), is("raw_files"));
        assertThat(output.getFullName(), is("main.landing_zone.raw_files"));
    }

    @Test
    void createExternalRequiresStorageLocation() throws Exception {
        var task = Create.builder()
            .id(IdUtils.create())
            .type(Create.class.getName())
            .catalogName(Property.ofValue("main"))
            .schemaName(Property.ofValue("landing_zone"))
            .name(Property.ofValue("raw_files"))
            .volumeType(Property.ofValue(VolumeType.EXTERNAL))
            .build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        assertThat(exception.getMessage(), containsString("storageLocation"));
        verify(volumes, never()).create(any(CreateVolumeRequestContent.class));
    }

    @Test
    void createManagedRejectsStorageLocation() throws Exception {
        var task = Create.builder()
            .id(IdUtils.create())
            .type(Create.class.getName())
            .catalogName(Property.ofValue("main"))
            .schemaName(Property.ofValue("landing_zone"))
            .name(Property.ofValue("raw_files"))
            .storageLocation(Property.ofValue("s3://bucket/path"))
            .build();

        assertThrows(
            IllegalArgumentException.class,
            () -> UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
    }

    @Test
    void get() throws Exception {
        when(volumes.read("main.landing_zone.raw_files")).thenReturn(new VolumeInfo().setName("raw_files").setFullName("main.landing_zone.raw_files"));

        var task = Get.builder().id(IdUtils.create()).type(Get.class.getName()).fullName(Property.ofValue("main.landing_zone.raw_files")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        assertThat(output.getName(), is("raw_files"));
    }

    @Test
    void update() throws Exception {
        when(volumes.update(any(UpdateVolumeRequestContent.class))).thenReturn(new VolumeInfo().setName("raw").setFullName("main.landing_zone.raw"));

        var task = Update.builder()
            .id(IdUtils.create())
            .type(Update.class.getName())
            .fullName(Property.ofValue("main.landing_zone.raw_files"))
            .newName(Property.ofValue("raw"))
            .build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        var request = ArgumentCaptor.forClass(UpdateVolumeRequestContent.class);
        verify(volumes).update(request.capture());
        assertThat(request.getValue().getName(), is("main.landing_zone.raw_files"));
        assertThat(request.getValue().getNewName(), is("raw"));
        assertThat(output.getFullName(), is("main.landing_zone.raw"));
    }

    @Test
    void delete() throws Exception {
        var task = Delete.builder().id(IdUtils.create()).type(Delete.class.getName()).fullName(Property.ofValue("main.landing_zone.raw_files")).build();
        var output = UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));

        verify(volumes).delete("main.landing_zone.raw_files");
        assertThat(output, nullValue());
    }

    @Test
    void upload() throws Exception {
        var content = "id,name\n1,kestra\n";
        var source = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + IdUtils.create()),
            new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
        );

        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .volumePath(Property.ofValue(VOLUME_FILE))
            .overwrite(Property.ofValue(true))
            .from(Property.ofValue(source.toString()))
            .build();

        // the SDK consumes the stream while uploading
        doAnswer(invocation ->
        {
            ((UploadRequest) invocation.getArgument(0)).getContents().readAllBytes();
            return null;
        }).when(files).upload(any(UploadRequest.class));

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = UnityCatalogTestSupport.withClient(task, client).run(runContext);

        var request = ArgumentCaptor.forClass(UploadRequest.class);
        verify(files).upload(request.capture());
        assertThat(request.getValue().getFilePath(), is(VOLUME_FILE));
        assertThat(request.getValue().getOverwrite(), is(true));
        assertThat(output.getVolumePath(), is(VOLUME_FILE));
        assertThat(output.getSize(), is((long) content.getBytes(StandardCharsets.UTF_8).length));
    }

    @Test
    void uploadRejectsPathOutsideVolumes() throws Exception {
        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .volumePath(Property.ofValue("/Share/data.csv"))
            .from(Property.ofValue("kestra:///unused"))
            .build();

        assertThrows(
            IllegalArgumentException.class,
            () -> UnityCatalogTestSupport.withClient(task, client).run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()))
        );
        verify(files, never()).upload(any(UploadRequest.class));
    }

    @Test
    void download() throws Exception {
        var bytes = "id,name\n1,kestra\n".getBytes(StandardCharsets.UTF_8);
        when(files.download(anyString())).thenReturn(new DownloadResponse().setContents(new ByteArrayInputStream(bytes)));

        var task = Download.builder().id(IdUtils.create()).type(Download.class.getName()).volumePath(Property.ofValue(VOLUME_FILE)).build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, Map.of());
        var output = UnityCatalogTestSupport.withClient(task, client).run(runContext);

        verify(files).download(VOLUME_FILE);
        assertThat(output.getUri(), notNullValue());
        assertThat(output.getSize(), is((long) bytes.length));
        try (var in = runContext.storage().getFile(output.getUri())) {
            assertThat(in.readAllBytes(), is(bytes));
        }
    }
}
