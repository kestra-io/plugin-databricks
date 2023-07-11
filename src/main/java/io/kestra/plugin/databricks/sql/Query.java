package io.kestra.plugin.databricks.sql;

import com.databricks.client.jdbc.Driver;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.Rethrow;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Consumer;
import javax.validation.constraints.NotNull;

/**
 * See https://docs.databricks.com/integrations/jdbc-odbc-bi.html#jdbc-driver
 *
 * FIXME some part are copied from the plugin-jdbc, maybe we need to find a way to avoid copying and share more stuff
 */
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Plugin(
    examples = {
        @Example(
            code = """
                id: sqlQuery
                type: io.kestra.plugin.databricks.sql.Query
                accessToken: <your-accessToken>
                host: <your-host>
                httpPath: <your-httpPath>
                sql: SELECT 1
                """
        )
    },
    metrics = {
        @Metric(name = "fetch.size", type = "counter", description = "Query result size")
    }
)
public class Query extends Task implements RunnableTask<Query.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon();

    @NotNull
    @PluginProperty(dynamic = true)
    private String host;

    @NotNull
    @PluginProperty(dynamic = true)
    private String httpPath;

    @PluginProperty(dynamic = true)
    private String catalog;

    @PluginProperty(dynamic = true)
    private String schema;

    @PluginProperty(dynamic = true)
    private String accessToken;

    @PluginProperty
    private Map<String, String> properties;

    @NotNull
    @PluginProperty(dynamic = true)
    private String sql;

    @Schema(
        title = "The time zone id to use for date/time manipulation. Default value is the worker default zone id."
    )
    @PluginProperty
    private String timeZoneId;

    //TODO should we allow to fetch or do we design only for big data ?

    @Override
    public Output run(RunContext runContext) throws Exception {
        var url = "jdbc:databricks://" + runContext.render(host) + ":443;HttpPath=" + runContext.render(httpPath);
        if (catalog != null) {
            url += ";ConnCatalog=" + runContext.render(catalog);
        }
        if (schema != null) {
            url += ";ConnSchema=" + runContext.render(schema);
        }
        var props = new java.util.Properties();
        if (accessToken != null) {
            props.put("PWD", runContext.render(accessToken));
        }
        if (properties != null) {
            props.putAll(properties);
        }
        runContext.logger().debug("Using JDBC URL: {}", url);

        DriverManager.registerDriver(new Driver());
        try (var connection = DriverManager.getConnection(url, props);
             var stmt = connection.createStatement()) {
            var query = runContext.render(sql);
            runContext.logger().debug("Starting query: {}", query);

            if (stmt.execute(query)) {
                try (ResultSet rs = stmt.getResultSet()) {
                    File tempFile = runContext.tempFile(".ion").toFile();
                    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
                    long size = fetchToFile(stmt, rs, fileWriter, new DatabricksCellConverter(zoneId()), connection);
                    fileWriter.flush();
                    fileWriter.close();

                    runContext.metric(Counter.of("fetch.size",  size));

                    return Output.builder()
                        .uri(runContext.putTempFile(tempFile))
                        .size(size)
                        .build();
                }
            }
        }
        return null;
    }

    //FIXME duplicated with io.kestra.plugin.jdbc.AbstractJdbcQuery
    private ZoneId zoneId() {
        if (this.getTimeZoneId() != null) {
            return ZoneId.of(this.getTimeZoneId());
        }

        return TimeZone.getDefault().toZoneId();
    }

    //FIXME duplicated with io.kestra.plugin.jdbc.AbstractJdbcQuery
    private long fetchToFile(Statement stmt, ResultSet rs, BufferedWriter writer, AbstractCellConverter cellConverter, Connection connection) throws SQLException, IOException {
        return fetch(
            stmt,
            rs,
            Rethrow.throwConsumer(map -> {
                final String s = MAPPER.writeValueAsString(map);
                writer.write(s);
                writer.write("\n");
            }),
            cellConverter,
            connection
        );
    }

    //FIXME duplicated with io.kestra.plugin.jdbc.AbstractJdbcQuery
    private long fetch(Statement stmt, ResultSet rs, Consumer<Map<String, Object>> c, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        boolean isResult;
        long count = 0;

        do {
            while (rs.next()) {
                Map<String, Object> map = mapResultSetToMap(rs, cellConverter, connection);
                c.accept(map);
                count++;
            }
            isResult = stmt.getMoreResults();
        } while (isResult);

        return count;
    }

    //FIXME duplicated with io.kestra.plugin.jdbc.AbstractJdbcQuery
    private Map<String, Object> mapResultSetToMap(ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        int columnsCount = rs.getMetaData().getColumnCount();
        Map<String, Object> map = new LinkedHashMap<>();

        for (int i = 1; i <= columnsCount; i++) {
            map.put(rs.getMetaData().getColumnName(i), convertCell(i, rs, cellConverter, connection));
        }

        return map;
    }

    //FIXME duplicated with io.kestra.plugin.jdbc.AbstractJdbcQuery
    private Object convertCell(int columnIndex, ResultSet rs, AbstractCellConverter cellConverter, Connection connection) throws SQLException {
        return cellConverter.convertCell(columnIndex, rs, connection);
    }

    @SuperBuilder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "The url of the result file on kestra storage (.ion file / Amazon Ion text format)",
            description = "Only populated if 'store' is set to true."
        )
        private final URI uri;

        @Schema(
            title = "The size of the fetched rows",
            description = "Only populated if 'store' or 'fetch' parameter is set to true."
        )
        private final Long size;
    }
}
