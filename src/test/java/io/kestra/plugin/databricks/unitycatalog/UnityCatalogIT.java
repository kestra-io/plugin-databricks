package io.kestra.plugin.databricks.unitycatalog;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

import com.google.api.client.util.Strings;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Runs the Unity Catalog tasks against a real Databricks workspace.
 * <p>
 * Required environment variables:
 * <ul>
 * <li>{@code DATABRICKS_HOST}: workspace URL, e.g. {@code https://dbc-xxxx.cloud.databricks.com}</li>
 * <li>{@code DATABRICKS_TOKEN}: personal access token</li>
 * <li>{@code DATABRICKS_UC_CATALOG}: an <b>existing</b> catalog in which the test creates (and removes) a temporary schema</li>
 * </ul>
 * Optional: {@code DATABRICKS_UC_CREATE_CATALOG=true} also tests catalog create/update/delete (needs a metastore with a root storage or
 * {@code DATABRICKS_UC_CATALOG_STORAGE_ROOT}).
 */
@KestraTest
@DisabledIf(
    value = "canNotBeEnabled",
    disabledReason = "Disabled because it requires Databricks secrets: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_UC_CATALOG"
)
class UnityCatalogIT {
    private static final String HOST = System.getenv("DATABRICKS_HOST");
    private static final String TOKEN = System.getenv("DATABRICKS_TOKEN");
    private static final String CATALOG = System.getenv("DATABRICKS_UC_CATALOG");
    private static final String CREATE_CATALOG = System.getenv("DATABRICKS_UC_CREATE_CATALOG");
    private static final String CATALOG_STORAGE_ROOT = System.getenv("DATABRICKS_UC_CATALOG_STORAGE_ROOT");

    // built-in group that exists in every workspace
    private static final String PRINCIPAL = "account users";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void schemaVolumeFileAndGrantLifecycle() throws Exception {
        var schemaName = "kestra_it_" + IdUtils.create().toLowerCase(Locale.ROOT);
        var schemaFullName = CATALOG + "." + schemaName;
        var volumeName = "raw_files";
        var volumeFullName = schemaFullName + "." + volumeName;
        var volumePath = "/Volumes/" + CATALOG + "/" + schemaName + "/" + volumeName + "/data.csv";
        var content = "id,name\n1,kestra\n2,databricks\n";

        try {
            // schema
            var createdSchema = run(
                io.kestra.plugin.databricks.unitycatalog.schema.Create.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.schema.Create.class.getName())
                    .host(host())
                    .authentication(auth())
                    .catalogName(Property.ofValue(CATALOG))
                    .name(Property.ofValue(schemaName))
                    .comment(Property.ofValue("Created by the Kestra integration tests"))
                    .build()
            );
            assertThat(createdSchema.getFullName(), is(schemaFullName));

            var schemas = run(
                io.kestra.plugin.databricks.unitycatalog.schema.List.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.schema.List.class.getName())
                    .host(host())
                    .authentication(auth())
                    .catalogName(Property.ofValue(CATALOG))
                    .build()
            );
            assertThat(schemas.getSchemas().stream().map(s -> s.get("name")).toList(), hasItem(schemaName));

            var updatedSchema = run(
                io.kestra.plugin.databricks.unitycatalog.schema.Update.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.schema.Update.class.getName())
                    .host(host())
                    .authentication(auth())
                    .fullName(Property.ofValue(schemaFullName))
                    .comment(Property.ofValue("Updated by the Kestra integration tests"))
                    .build()
            );
            assertThat(updatedSchema.getSchema().get("comment"), is("Updated by the Kestra integration tests"));

            var fetchedSchema = run(
                io.kestra.plugin.databricks.unitycatalog.schema.Get.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.schema.Get.class.getName())
                    .host(host())
                    .authentication(auth())
                    .fullName(Property.ofValue(schemaFullName))
                    .build()
            );
            assertThat(fetchedSchema.getName(), is(schemaName));

            // volume
            var createdVolume = run(
                io.kestra.plugin.databricks.unitycatalog.volume.Create.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.volume.Create.class.getName())
                    .host(host())
                    .authentication(auth())
                    .catalogName(Property.ofValue(CATALOG))
                    .schemaName(Property.ofValue(schemaName))
                    .name(Property.ofValue(volumeName))
                    .build()
            );
            assertThat(createdVolume.getFullName(), is(volumeFullName));

            var volumes = run(
                io.kestra.plugin.databricks.unitycatalog.volume.List.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.volume.List.class.getName())
                    .host(host())
                    .authentication(auth())
                    .catalogName(Property.ofValue(CATALOG))
                    .schemaName(Property.ofValue(schemaName))
                    .build()
            );
            assertThat(volumes.getVolumes().stream().map(v -> v.get("name")).toList(), hasItem(volumeName));

            var fetchedVolume = run(
                io.kestra.plugin.databricks.unitycatalog.volume.Get.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.volume.Get.class.getName())
                    .host(host())
                    .authentication(auth())
                    .fullName(Property.ofValue(volumeFullName))
                    .build()
            );
            assertThat(fetchedVolume.getName(), is(volumeName));

            // file upload / download through the Files API
            URI source = storageInterface.put(
                TenantService.MAIN_TENANT,
                null,
                new URI("/" + IdUtils.create()),
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8))
            );

            var uploaded = run(
                io.kestra.plugin.databricks.unitycatalog.volume.Upload.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.volume.Upload.class.getName())
                    .host(host())
                    .authentication(auth())
                    .from(Property.ofValue(source.toString()))
                    .volumePath(Property.ofValue(volumePath))
                    .overwrite(Property.ofValue(true))
                    .build()
            );
            assertThat(uploaded.getSize(), is((long) content.getBytes(StandardCharsets.UTF_8).length));

            var downloadTask = io.kestra.plugin.databricks.unitycatalog.volume.Download.builder()
                .id(IdUtils.create())
                .type(io.kestra.plugin.databricks.unitycatalog.volume.Download.class.getName())
                .host(host())
                .authentication(auth())
                .volumePath(Property.ofValue(volumePath))
                .build();
            var downloadContext = TestsUtils.mockRunContext(runContextFactory, downloadTask, Map.of());
            var downloaded = downloadTask.run(downloadContext);
            assertThat(downloaded.getSize(), is((long) content.getBytes(StandardCharsets.UTF_8).length));
            try (var in = downloadContext.storage().getFile(downloaded.getUri())) {
                assertThat(new String(in.readAllBytes(), StandardCharsets.UTF_8), is(content));
            }

            // grants
            var granted = run(
                io.kestra.plugin.databricks.unitycatalog.grant.Update.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.grant.Update.class.getName())
                    .host(host())
                    .authentication(auth())
                    .securableType(Property.ofValue("SCHEMA"))
                    .fullName(Property.ofValue(schemaFullName))
                    .changes(
                        java.util.List.of(
                            io.kestra.plugin.databricks.unitycatalog.grant.Update.Change.builder()
                                .principal(Property.ofValue(PRINCIPAL))
                                .add(Property.ofValue(java.util.List.of("USE_SCHEMA")))
                                .build()
                        )
                    )
                    .build()
            );
            assertThat(granted.getPrivilegeAssignments().stream().map(a -> a.get("principal")).toList(), hasItem(PRINCIPAL));

            var grants = run(
                io.kestra.plugin.databricks.unitycatalog.grant.Get.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.grant.Get.class.getName())
                    .host(host())
                    .authentication(auth())
                    .securableType(Property.ofValue("SCHEMA"))
                    .fullName(Property.ofValue(schemaFullName))
                    .principal(Property.ofValue(PRINCIPAL))
                    .build()
            );
            assertThat(grants.getPrivilegeAssignments().size(), is(1));
            assertThat(grants.getPrivilegeAssignments().get(0).get("privileges"), is(java.util.List.of("USE_SCHEMA")));

            var revoked = run(
                io.kestra.plugin.databricks.unitycatalog.grant.Update.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.grant.Update.class.getName())
                    .host(host())
                    .authentication(auth())
                    .securableType(Property.ofValue("SCHEMA"))
                    .fullName(Property.ofValue(schemaFullName))
                    .changes(
                        java.util.List.of(
                            io.kestra.plugin.databricks.unitycatalog.grant.Update.Change.builder()
                                .principal(Property.ofValue(PRINCIPAL))
                                .remove(Property.ofValue(java.util.List.of("USE_SCHEMA")))
                                .build()
                        )
                    )
                    .build()
            );
            assertThat(revoked.getPrivilegeAssignments().stream().map(a -> a.get("principal")).toList().contains(PRINCIPAL), is(false));

            // volume deletion
            run(
                io.kestra.plugin.databricks.unitycatalog.volume.Delete.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.volume.Delete.class.getName())
                    .host(host())
                    .authentication(auth())
                    .fullName(Property.ofValue(volumeFullName))
                    .build()
            );
        } finally {
            // always remove the temporary schema, including whatever is left in it
            run(
                io.kestra.plugin.databricks.unitycatalog.schema.Delete.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.schema.Delete.class.getName())
                    .host(host())
                    .authentication(auth())
                    .fullName(Property.ofValue(schemaFullName))
                    .force(Property.ofValue(true))
                    .build()
            );
        }
    }

    @Test
    void catalogReadAndTables() throws Exception {
        var catalogs = run(
            io.kestra.plugin.databricks.unitycatalog.catalog.List.builder()
                .id(IdUtils.create())
                .type(io.kestra.plugin.databricks.unitycatalog.catalog.List.class.getName())
                .host(host())
                .authentication(auth())
                .build()
        );
        assertThat(catalogs.getCatalogs().stream().map(c -> c.get("name")).toList(), hasItem(CATALOG));

        var catalog = run(
            io.kestra.plugin.databricks.unitycatalog.catalog.Get.builder()
                .id(IdUtils.create())
                .type(io.kestra.plugin.databricks.unitycatalog.catalog.Get.class.getName())
                .host(host())
                .authentication(auth())
                .name(Property.ofValue(CATALOG))
                .build()
        );
        assertThat(catalog.getName(), is(CATALOG));

        // every catalog has an `information_schema` schema, which gives us tables to read without needing a SQL warehouse
        var tables = run(
            io.kestra.plugin.databricks.unitycatalog.table.List.builder()
                .id(IdUtils.create())
                .type(io.kestra.plugin.databricks.unitycatalog.table.List.class.getName())
                .host(host())
                .authentication(auth())
                .catalogName(Property.ofValue(CATALOG))
                .schemaName(Property.ofValue("information_schema"))
                .build()
        );
        assertThat(tables.getSize(), greaterThan(0));

        var fullName = (String) tables.getTables().get(0).get("full_name");
        var table = run(
            io.kestra.plugin.databricks.unitycatalog.table.Get.builder()
                .id(IdUtils.create())
                .type(io.kestra.plugin.databricks.unitycatalog.table.Get.class.getName())
                .host(host())
                .authentication(auth())
                .fullName(Property.ofValue(fullName))
                .build()
        );
        assertThat(table.getFullName(), is(fullName));
    }

    @Test
    void tableTrigger() throws Exception {
        var trigger = io.kestra.plugin.databricks.unitycatalog.table.Trigger.builder()
            .id(IdUtils.create())
            .type(io.kestra.plugin.databricks.unitycatalog.table.Trigger.class.getName())
            .host(host())
            .authentication(auth())
            .catalogName(Property.ofValue(CATALOG))
            .schemaName(Property.ofValue("information_schema"))
            .on(Property.ofValue(StatefulTriggerInterface.On.CREATE))
            .stateKey(Property.ofValue("uc_it_" + IdUtils.create()))
            .build();
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        // first poll reports the existing tables as new
        var first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));
        assertThat(first.get().getTrigger(), notNullValue());

        // nothing new on the second poll
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void catalogLifecycle() throws Exception {
        if (!"true".equalsIgnoreCase(CREATE_CATALOG)) {
            org.junit.jupiter.api.Assumptions.abort("Set DATABRICKS_UC_CREATE_CATALOG=true to test catalog creation");
        }

        var name = "kestra_it_" + IdUtils.create().toLowerCase(Locale.ROOT);
        try {
            var created = run(
                io.kestra.plugin.databricks.unitycatalog.catalog.Create.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.catalog.Create.class.getName())
                    .host(host())
                    .authentication(auth())
                    .name(Property.ofValue(name))
                    .comment(Property.ofValue("Created by the Kestra integration tests"))
                    .storageRoot(Strings.isNullOrEmpty(CATALOG_STORAGE_ROOT) ? null : Property.ofValue(CATALOG_STORAGE_ROOT))
                    .build()
            );
            assertThat(created.getName(), is(name));

            var updated = run(
                io.kestra.plugin.databricks.unitycatalog.catalog.Update.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.catalog.Update.class.getName())
                    .host(host())
                    .authentication(auth())
                    .name(Property.ofValue(name))
                    .comment(Property.ofValue("Updated by the Kestra integration tests"))
                    .build()
            );
            assertThat(updated.getCatalog().get("comment"), is("Updated by the Kestra integration tests"));
        } finally {
            run(
                io.kestra.plugin.databricks.unitycatalog.catalog.Delete.builder()
                    .id(IdUtils.create())
                    .type(io.kestra.plugin.databricks.unitycatalog.catalog.Delete.class.getName())
                    .host(host())
                    .authentication(auth())
                    .name(Property.ofValue(name))
                    .force(Property.ofValue(true))
                    .build()
            );
        }
    }

    private <O extends Output, T extends Task & RunnableTask<O>> O run(T task) throws Exception {
        return task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of()));
    }

    private static Property<String> host() {
        return Property.ofValue(HOST);
    }

    private static AbstractTask.AuthenticationConfig auth() {
        return AbstractTask.AuthenticationConfig.builder().token(Property.ofValue(TOKEN)).build();
    }

    protected static boolean canNotBeEnabled() {
        return Strings.isNullOrEmpty(HOST) || Strings.isNullOrEmpty(TOKEN) || Strings.isNullOrEmpty(CATALOG);
    }
}
