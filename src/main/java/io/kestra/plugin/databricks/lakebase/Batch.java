package io.kestra.plugin.databricks.lakebase;

import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

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
@Schema(
    title = "Run a parameterized batch against Databricks Lakebase",
    description = """
        Mints a short-lived OAuth database credential, then executes a prepared SQL statement once per
        parameter group or once per row from an Ion file in internal storage. Intended for bulk insert
        and update workloads.
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Batch-insert rows produced by an upstream task",
            code = """
                id: lakebase_batch_insert
                namespace: company.team

                inputs:
                  - id: rows
                    type: ARRAY
                    itemType: STRING

                tasks:
                  - id: insert_rows
                    type: io.kestra.plugin.databricks.lakebase.Batch
                    workspaceHost: "{{ secret('DATABRICKS_HOST') }}"
                    clientId: "{{ secret('DATABRICKS_CLIENT_ID') }}"
                    clientSecret: "{{ secret('DATABRICKS_CLIENT_SECRET') }}"
                    endpoint: "{{ secret('LAKEBASE_ENDPOINT_NAME') }}"
                    database: "orders_db"
                    sql: "INSERT INTO orders_audit (payload) VALUES (?)"
                    parameterGroups:
                      - parameters: "{{ inputs.rows }}"
                """
        )
    },
    metrics = {
        @Metric(name = "records", type = Counter.TYPE, description = "The number of records processed"),
        @Metric(name = "updated", type = Counter.TYPE, description = "The number of records updated"),
        @Metric(name = "query", type = Counter.TYPE, description = "The number of batch statements executed")
    }
)
public class Batch extends AbstractLakebaseTask implements RunnableTask<Batch.Output> {
    @NotNull
    @Schema(title = "SQL statement to execute", description = "Prepared statement with JDBC ? placeholders, rendered with flow variables")
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Schema(
        title = "Parameter groups for the prepared statement",
        description = """
            Each group supplies values for one or more executions. A group's `parameters` may be a single
            row (a list of values), a list of rows, or — when the SQL has a single placeholder — a list
            of scalars that each become their own row.
            """
    )
    @PluginProperty(group = "main")
    private Property<List<ParameterGroup>> parameterGroups;

    @Schema(
        title = "Ion file of rows to bind",
        description = "Internal storage URI of an Ion file produced by a previous task (for example Query with fetchType STORE). Each row is a map or a list bound to the SQL placeholders."
    )
    @PluginProperty(group = "main")
    private Property<String> from;

    @Builder.Default
    @Schema(title = "JDBC batch size", description = "Number of rows added to the prepared statement before executeBatch. Default: 1000")
    @PluginProperty(group = "execution")
    private Property<Integer> batchSize = Property.ofValue(1000);

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedSql = runContext.render(sql).as(String.class).orElseThrow();
        int placeholders = LakebaseService.placeholderCount(renderedSql);
        int chunk = runContext.render(batchSize).as(Integer.class).orElse(1000);

        Counters counters = new Counters();
        List<List<Object>> buffer = new ArrayList<>(Math.max(chunk, 1));

        try (
            Connection connection = LakebaseService.connect(runContext, this);
            PreparedStatement stmt = connection.prepareStatement(renderedSql)
        ) {
            streamParameterGroups(runContext, stmt, buffer, placeholders, chunk, counters);
            streamFile(runContext, stmt, buffer, placeholders, chunk, counters);

            if (counters.records == 0) {
                throw new IllegalArgumentException("Batch requires parameterGroups or from with at least one row");
            }
            if (!buffer.isEmpty()) {
                counters.updated += flush(stmt, buffer, placeholders);
                counters.queries++;
            }
        }

        runContext.logger().debug("Finished Lakebase batch of {} row(s): {}", counters.records, renderedSql);
        runContext.metric(Counter.of("records", counters.records));
        runContext.metric(Counter.of("updated", counters.updated));
        runContext.metric(Counter.of("query", counters.queries));

        return Output.builder()
            .rowCount(counters.records)
            .updatedCount(counters.updated)
            .build();
    }

    private static void append(
        PreparedStatement stmt,
        List<List<Object>> buffer,
        List<Object> row,
        int placeholders,
        int chunk,
        Counters counters) throws Exception {
        buffer.add(row);
        counters.records++;
        if (buffer.size() >= chunk) {
            counters.updated += flush(stmt, buffer, placeholders);
            counters.queries++;
        }
    }

    private static long flush(PreparedStatement stmt, List<List<Object>> buffer, int placeholders) throws Exception {
        for (List<Object> row : buffer) {
            bind(stmt, row, placeholders);
            stmt.addBatch();
        }
        long updated = sum(stmt.executeBatch());
        buffer.clear();
        return updated;
    }

    private void streamParameterGroups(
        RunContext runContext,
        PreparedStatement stmt,
        List<List<Object>> buffer,
        int placeholders,
        int chunk,
        Counters counters) throws Exception {
        for (ParameterGroup group : runContext.render(parameterGroups).asList(ParameterGroup.class)) {
            for (List<Object> row : LakebaseService.expandParameterGroup(renderParameters(runContext, group), placeholders)) {
                append(stmt, buffer, row, placeholders, chunk, counters);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Object renderParameters(RunContext runContext, ParameterGroup group) throws Exception {
        if (group == null || group.getParameters() == null) {
            return List.of();
        }
        Object raw = group.getParameters();
        if (raw instanceof Property<?> property) {
            @SuppressWarnings("unchecked")
            Property<List<Object>> typed = (Property<List<Object>>) property;
            List<Object> rendered = runContext.render(typed).asList(Object.class);
            if (!rendered.isEmpty()) {
                return rendered;
            }
            return List.of();
        }
        if (raw instanceof String string) {
            String rendered = runContext.render(string).trim();
            if (rendered.startsWith("[")) {
                return JacksonMapper.ofJson().readValue(rendered, List.class);
            }
            return rendered;
        }
        if (raw instanceof List<?> list) {
            List<Object> rendered = new ArrayList<>(list.size());
            for (Object item : list) {
                rendered.add(item instanceof String string ? runContext.render(string) : item);
            }
            return rendered;
        }
        return raw;
    }

    private void streamFile(
        RunContext runContext,
        PreparedStatement stmt,
        List<List<Object>> buffer,
        int placeholders,
        int chunk,
        Counters counters) throws Exception {
        String fromValue = runContext.render(from).as(String.class).orElse(null);
        if (fromValue == null || fromValue.isBlank()) {
            return;
        }

        var failure = new AtomicReference<Exception>();
        try (InputStream input = runContext.storage().getFile(URI.create(fromValue))) {
            FileSerde.read(input, item ->
            {
                if (failure.get() != null) {
                    return;
                }
                try {
                    for (List<Object> row : LakebaseService.expandParameterGroup(normalizeFileRow(item), placeholders)) {
                        append(stmt, buffer, row, placeholders, chunk, counters);
                    }
                } catch (Exception e) {
                    failure.set(e);
                }
            });
        }
        if (failure.get() != null) {
            throw failure.get();
        }
    }

    @SuppressWarnings("unchecked")
    private static Object normalizeFileRow(Object item) {
        if (item instanceof Map<?, ?> map) {
            return new ArrayList<>(map.values());
        }
        if (item instanceof Collection<?> collection) {
            return new ArrayList<>((Collection<Object>) collection);
        }
        return item;
    }

    private static void bind(PreparedStatement stmt, List<Object> row, int placeholders) throws Exception {
        if (placeholders > 0 && row.size() != placeholders) {
            throw new IllegalArgumentException(
                "Parameter row has " + row.size() + " value(s) but SQL has " + placeholders + " placeholder(s)"
            );
        }
        for (int i = 0; i < row.size(); i++) {
            stmt.setObject(i + 1, LakebaseService.jdbcBindValue(row.get(i)));
        }
    }

    private static final class Counters {
        private long records;
        private long updated;
        private long queries;
    }

    private static long sum(int[] counts) {
        long total = 0L;
        for (int count : counts) {
            if (count >= 0) {
                total += count;
            }
        }
        return total;
    }

    @SuperBuilder
    @Getter
    @NoArgsConstructor
    public static class ParameterGroup {
        @Schema(
            title = "Parameter values",
            description = "A single row (list of values), a list of rows, or — when the SQL has one placeholder — a list of scalars"
        )
        private Object parameters;
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of rows processed")
        private final Long rowCount;

        @Schema(title = "Number of rows reported updated by the driver")
        private final Long updatedCount;
    }
}
