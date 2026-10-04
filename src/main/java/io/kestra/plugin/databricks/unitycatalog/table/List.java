package io.kestra.plugin.databricks.unitycatalog.table;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.net.URI;
import java.util.Map;

import com.databricks.sdk.service.catalog.ListTablesRequest;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
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
            title = "List the tables of a schema",
            full = true,
            code = """
                id: databricks_uc_table_list
                namespace: company.team

                tasks:
                  - id: list_tables
                    type: io.kestra.plugin.databricks.unitycatalog.table.List
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                    schemaName: landing_zone
                """
        ),
        @Example(
            title = "Store the table names of a large schema in internal storage, without the column definitions.",
            full = true,
            code = """
                id: databricks_uc_table_list_store
                namespace: company.team

                tasks:
                  - id: list_tables
                    type: io.kestra.plugin.databricks.unitycatalog.table.List
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    catalogName: main
                    schemaName: landing_zone
                    omitColumns: true
                    fetchType: STORE
                """
        )
    }
)
@Schema(
    title = "List Unity Catalog tables",
    description = """
        Lists the tables of a schema that the caller can access.
        On schemas with many tables, set `omitColumns: true` and/or `fetchType: STORE` to keep the task output small."""
)
public class List extends AbstractTask implements RunnableTask<List.Output> {
    @Schema(
        title = "Catalog name",
        description = "Name of the parent catalog."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> catalogName;

    @Schema(
        title = "Schema name",
        description = "Name of the parent schema."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> schemaName;

    @Schema(
        title = "Omit columns",
        description = "Do not return the column definitions of each table. Recommended when only the table names are needed. Defaults to `false`."
    )
    @PluginProperty(group = "advanced")
    private Property<Boolean> omitColumns;

    @Schema(
        title = "Fetch type",
        description = """
            How the tables are returned:
            - `FETCH`: all the tables are available in the `tables` output (default).
            - `FETCH_ONE`: only the first table is available in the `table` output.
            - `STORE`: the tables are written to Kestra internal storage and exposed through the `uri` output.
            - `NONE`: nothing is returned except `size`."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Override
    public List.Output run(RunContext runContext) throws Exception {
        var rCatalogName = runContext.render(catalogName).as(String.class).orElseThrow();
        var rSchemaName = runContext.render(schemaName).as(String.class).orElseThrow();
        var rFetchType = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);

        var request = new ListTablesRequest().setCatalogName(rCatalogName).setSchemaName(rSchemaName);
        runContext.render(omitColumns).as(Boolean.class).ifPresent(request::setOmitColumns);

        var tables = workspaceClient(runContext).tables().list(request);
        var output = Output.builder();
        int size = 0;

        switch (rFetchType) {
            case FETCH_ONE -> {
                var iterator = tables.iterator();
                if (iterator.hasNext()) {
                    output.table(UnityCatalogUtils.toMap(iterator.next()));
                    size = 1;
                }
            }
            case FETCH -> {
                var fetched = UnityCatalogUtils.toMaps(tables);
                output.tables(fetched);
                size = fetched.size();
            }
            case STORE -> {
                var tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                try (var out = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                    for (var table : tables) {
                        FileSerde.write(out, UnityCatalogUtils.toMap(table));
                        size++;
                    }
                }
                output.uri(runContext.storage().putFile(tempFile));
            }
            case NONE -> {
                for (var ignored : tables) {
                    size++;
                }
            }
        }

        runContext.logger().info("Found {} table(s) in schema '{}.{}'", size, rCatalogName, rSchemaName);

        return output.size(size).build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Tables as returned by Databricks",
            description = "Only populated if using `fetchType=FETCH`."
        )
        private final java.util.List<Map<String, Object>> tables;

        @Schema(
            title = "First table as returned by Databricks",
            description = "Only populated if using `fetchType=FETCH_ONE`."
        )
        private final Map<String, Object> table;

        @Schema(
            title = "Kestra internal storage URI of the stored tables",
            description = "Only populated if using `fetchType=STORE`. The file is in Ion format, one table per row."
        )
        private final URI uri;

        @Schema(title = "Number of tables")
        private final Integer size;
    }
}
