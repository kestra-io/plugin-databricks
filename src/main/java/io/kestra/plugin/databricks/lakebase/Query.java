package io.kestra.plugin.databricks.lakebase;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;

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
    title = "Run a SQL query on Databricks Lakebase",
    description = """
        Mints a short-lived OAuth database credential via the Databricks SDK, then executes a single SQL
        statement against Lakebase over the Postgres wire protocol. Use fetchType to control how rows are
        returned: FETCH (all rows in memory), FETCH_ONE (first row), STORE (Ion file in internal storage),
        or NONE (execute only).
        """
)
@Plugin(
    examples = {
        @Example(
            full = true,
            title = "Run a query against a Lakebase database",
            code = """
                id: lakebase_query
                namespace: company.team

                tasks:
                  - id: query_orders
                    type: io.kestra.plugin.databricks.lakebase.Query
                    workspaceHost: "{{ secret('DATABRICKS_HOST') }}"
                    clientId: "{{ secret('DATABRICKS_CLIENT_ID') }}"
                    clientSecret: "{{ secret('DATABRICKS_CLIENT_SECRET') }}"
                    endpoint: "{{ secret('LAKEBASE_ENDPOINT_NAME') }}"
                    database: "orders_db"
                    sql: "SELECT id, status, updated_at FROM orders WHERE status = 'pending'"
                    fetchType: FETCH
                """
        )
    },
    metrics = {
        @Metric(name = "fetch.size", type = Counter.TYPE, description = "Query result size")
    }
)
public class Query extends AbstractLakebaseTask implements RunnableTask<Query.Output> {
    @NotNull
    @Schema(title = "SQL query to execute", description = "Single SQL statement rendered with flow variables before execution")
    @PluginProperty(group = "main")
    private Property<String> sql;

    @Schema(
        title = "SQL to execute after the main query in the same transaction",
        description = "Optional single statement, useful for marking processed rows. Committed together with the main query."
    )
    @PluginProperty(group = "advanced")
    private Property<String> afterSQL;

    @NotNull
    @Builder.Default
    @Schema(
        title = "Result fetching mode",
        description = "FETCH returns all rows, FETCH_ONE returns the first row only, STORE streams rows to internal storage (Ion), NONE returns no data. Default: FETCH"
    )
    @PluginProperty(group = "main")
    private Property<FetchType> fetchType = Property.ofValue(FetchType.FETCH);

    @Builder.Default
    @Schema(
        title = "JDBC fetch size for STORE mode",
        description = "Number of rows fetched per database round trip when fetchType is STORE. Default: 10000"
    )
    @PluginProperty(group = "execution")
    private Property<Integer> fetchSize = Property.ofValue(10000);

    @Schema(
        title = "Named parameter bindings",
        description = "Map of parameter names to values. Use :name placeholders in SQL; they are replaced with JDBC ? bindings."
    )
    @PluginProperty(group = "advanced")
    private Property<Map<String, Object>> parameters;

    @Schema(
        title = "Time zone for temporal values",
        description = "Timezone used when converting date/time columns; defaults to the worker JVM time zone"
    )
    @PluginProperty(group = "execution")
    private Property<String> timeZoneId;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String query = runContext.render(sql).as(String.class).orElseThrow();
        FetchType type = runContext.render(fetchType).as(FetchType.class).orElse(FetchType.FETCH);
        LakebaseCellConverter cellConverter = new LakebaseCellConverter(zoneId(runContext));

        runContext.logger().debug("Starting Lakebase query: {}", query);

        try (Connection connection = LakebaseService.connect(runContext, this)) {
            boolean supportsTx = connection.getMetaData().supportsTransactions();
            if (supportsTx && afterSQL != null) {
                connection.setAutoCommit(false);
            }

            Output.OutputBuilder<?, ?> output = Output.builder();
            long size = 0L;

            try (Statement stmt = createStatement(runContext, connection, query)) {
                if (type == FetchType.STORE) {
                    stmt.setFetchSize(runContext.render(fetchSize).as(Integer.class).orElse(10000));
                }

                boolean isResult = execute(stmt, query);

                if (isResult && type != FetchType.NONE) {
                    try (ResultSet rs = stmt.getResultSet()) {
                        switch (type) {
                            case FETCH_ONE -> {
                                Map<String, Object> row = fetchOne(rs, cellConverter, connection);
                                size = row == null ? 0L : 1L;
                                output.row(row).size(size);
                            }
                            case STORE -> {
                                File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                                try (var fileOutput = new BufferedOutputStream(new FileOutputStream(tempFile), FileSerde.BUFFER_SIZE)) {
                                    size = fetch(stmt, rs, Rethrow.throwConsumer(map -> FileSerde.write(fileOutput, map)), cellConverter, connection);
                                }
                                output.uri(runContext.storage().putFile(tempFile)).size(size);
                            }
                            case FETCH -> {
                                List<Map<String, Object>> rows = new ArrayList<>();
                                size = fetch(stmt, rs, rows::add, cellConverter, connection);
                                output.rows(rows).size(size);
                            }
                            default -> {
                            }
                        }
                    }
                } else {
                    output.size(size);
                }
            }

            executeAfterSql(runContext, connection);
            if (supportsTx && afterSQL != null) {
                connection.commit();
            }

            runContext.metric(Counter.of("fetch.size", size));
            return output.build();
        }
    }

    private void executeAfterSql(RunContext runContext, Connection connection) throws Exception {
        if (afterSQL == null) {
            return;
        }
        String rendered = runContext.render(afterSQL).as(String.class).orElse(null);
        if (rendered == null || rendered.isBlank()) {
            return;
        }
        runContext.logger().debug("Executing Lakebase afterSQL: {}", rendered);
        try (Statement stmt = createStatement(runContext, connection, rendered)) {
            execute(stmt, rendered);
        }
    }

    private Statement createStatement(RunContext runContext, Connection connection, String renderedSql) throws Exception {
        Map<String, Object> namedParams = runContext.render(parameters).asMap(String.class, Object.class);
        if (namedParams.isEmpty()) {
            return connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        }

        String preparedSql = renderedSql;
        List<String> names = new ArrayList<>();
        StringBuilder rewritten = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        for (int i = 0; i < preparedSql.length(); i++) {
            char c = preparedSql.charAt(i);
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
                rewritten.append(c);
            } else if (c == '"' && !inSingle) {
                inDouble = !inDouble;
                rewritten.append(c);
            } else if (c == ':' && !inSingle && !inDouble && i + 1 < preparedSql.length() && preparedSql.charAt(i + 1) == ':') {
                rewritten.append("::");
                i++;
            } else if (c == ':' && !inSingle && !inDouble && i + 1 < preparedSql.length() && Character.isJavaIdentifierStart(preparedSql.charAt(i + 1))) {
                int start = i + 1;
                int end = start;
                while (end < preparedSql.length() && Character.isJavaIdentifierPart(preparedSql.charAt(end))) {
                    end++;
                }
                names.add(preparedSql.substring(start, end));
                rewritten.append('?');
                i = end - 1;
            } else {
                rewritten.append(c);
            }
        }

        PreparedStatement stmt = connection.prepareStatement(rewritten.toString(), ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        for (int i = 0; i < names.size(); i++) {
            stmt.setObject(i + 1, LakebaseService.jdbcBindValue(namedParams.get(names.get(i))));
        }
        return stmt;
    }

    private boolean execute(Statement stmt, String query) throws SQLException {
        if (stmt instanceof PreparedStatement preparedStatement) {
            return preparedStatement.execute();
        }
        return stmt.execute(query);
    }

    private Map<String, Object> fetchOne(ResultSet rs, LakebaseCellConverter cellConverter, Connection connection) throws SQLException {
        if (!rs.next()) {
            return null;
        }
        return mapRow(rs, cellConverter, connection);
    }

    private long fetch(
        Statement stmt,
        ResultSet rs,
        Consumer<Map<String, Object>> consumer,
        LakebaseCellConverter cellConverter,
        Connection connection) throws SQLException {
        long count = 0;
        boolean more;
        do {
            while (rs.next()) {
                consumer.accept(mapRow(rs, cellConverter, connection));
                count++;
            }
            more = stmt.getMoreResults();
            if (more) {
                rs = stmt.getResultSet();
            }
        } while (more);
        return count;
    }

    private Map<String, Object> mapRow(ResultSet rs, LakebaseCellConverter cellConverter, Connection connection) throws SQLException {
        int columns = rs.getMetaData().getColumnCount();
        Map<String, Object> row = new LinkedHashMap<>(columns * 2);
        for (int i = 1; i <= columns; i++) {
            row.put(rs.getMetaData().getColumnLabel(i), cellConverter.convertCell(i, rs, connection));
        }
        return row;
    }

    private ZoneId zoneId(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.timeZoneId != null) {
            return ZoneId.of(runContext.render(this.timeZoneId).as(String.class).orElseThrow());
        }
        return TimeZone.getDefault().toZoneId();
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Fetched rows", description = "Present when fetchType is FETCH")
        private final List<Map<String, Object>> rows;

        @Schema(title = "First fetched row", description = "Present when fetchType is FETCH_ONE")
        private final Map<String, Object> row;

        @Schema(title = "Result file URI", description = "Internal storage URI of the Ion file when fetchType is STORE")
        private final URI uri;

        @Schema(title = "Number of fetched rows")
        private final Long size;
    }
}
