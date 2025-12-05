package io.kestra.plugin.databricks.sql;

import com.databricks.client.jdbc.Driver;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
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
import jakarta.validation.constraints.NotNull;

/**
 * For more information on the JDBC drivers see <a href="https://docs.databricks.com/integrations/jdbc-odbc-bi.html#jdbc-driver">JDBC Driver</a>.
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
            full = true,
            code = """
                id: databricks_sql_query
                namespace: company.team

                tasks:
                  - id: sql_query
                    type: io.kestra.plugin.databricks.sql.Query
                    accessToken: "{{ secret('DATABRICKS_TOKEN') }}"
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
@Schema(
    title = "Execute a SQL query on a Databricks cluster.",
    description = """
        See [Retrieve the connection details](https://docs.databricks.com/integrations/jdbc-odbc-bi.html#retrieve-the-connection-details) in the Databricks documentation to discover how to retrieve the needed configuration properties.
        We're using the Databricks JDBC driver to execute a Query, see [https://docs.databricks.com/integrations/jdbc-odbc-bi.html#jdbc-driver-capabilities](https://docs.databricks.com/integrations/jdbc-odbc-bi.html#jdbc-driver-capabilities) for its capabilities.

        Due to current limitation of the JDBC driver with Java 21, Arrow is disabled, performance may be impacted, see [here](https://community.databricks.com/t5/data-engineering/what-s-the-eta-for-supporting-java-21-in-the-jdbc-driver/td-p/57370) and [here](https://community.databricks.com/t5/data-engineering/java-21-support-with-databricks-jdbc-driver/m-p/49297) from Databricks status on Java 21 support.
        """
)
public class Query extends Task implements RunnableTask<Query.Output> {
    private static final ObjectMapper MAPPER = JacksonMapper.ofIon();

    @NotNull
    @Schema(title = "Databricks host.")
    private Property<String> host;

    @NotNull
    @Schema(
        title = "Databricks cluster HTTP Path.",
        description = "To retrieve the HTTP Path, go to your Databricks cluster, click on Advanced options then, click on JDBC/ODBC. See [Retrieve the connection details](https://docs.databricks.com/integrations/jdbc-odbc-bi.html#get-server-hostname-port-http-path-and-jdbc-url) for more details."
    )
    private Property<String> httpPath;

    private Property<String> catalog;

    private Property<String> schema;

    @Schema(title = "Databricks access token.")
    private Property<String> accessToken;

    private Property<Map<String, String>> properties;

    @NotNull
    @Schema(title = "SQL query to be executed.")
    private Property<String> sql;

    @Schema(
        title = "The time zone id to use for date/time manipulation. Default value is the worker default zone id."
    )
    private Property<String> timeZoneId;

    //TODO should we allow to fetch or do we design only for big data ?

    @Override
    public Output run(RunContext runContext) throws Exception {
        var url = "jdbc:databricks://" + runContext.render(host).as(String.class).orElseThrow() + ":443;HttpPath=" + runContext.render(httpPath).as(String.class).orElseThrow();
        if (catalog != null) {
            url += ";ConnCatalog=" + runContext.render(catalog).as(String.class).orElseThrow();
        }
        if (schema != null) {
            url += ";ConnSchema=" + runContext.render(schema).as(String.class).orElseThrow();
        }
        var props = new java.util.Properties();
        if (accessToken != null) {
            props.put("PWD", runContext.render(accessToken).as(String.class).orElseThrow());
        }
        var propertiesValue = runContext.render(properties).asMap(String.class, String.class);
        if (!propertiesValue.isEmpty()) {
            props.putAll(propertiesValue);
        }

        runContext.logger().debug("Using JDBC URL: {}", url);

        DriverManager.registerDriver(new Driver());
        try (var connection = DriverManager.getConnection(url, props);
             var stmt = connection.createStatement()) {
            String query = runContext.render(sql).as(String.class).orElseThrow();
            runContext.logger().debug("Starting query: {}", query);

            if (stmt.execute(query)) {
                try (ResultSet rs = stmt.getResultSet()) {
                    File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
                    BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
                    long size = fetchToFile(stmt, rs, fileWriter, new DatabricksCellConverter(zoneId(runContext)), connection);
                    fileWriter.flush();
                    fileWriter.close();

                    runContext.metric(Counter.of("fetch.size",  size));

                    return Output.builder()
                        .uri(runContext.storage().putFile(tempFile))
                        .size(size)
                        .build();
                }
            }
        }
        return null;
    }

    //FIXME duplicated with io.kestra.plugin.jdbc.AbstractJdbcQuery
    private ZoneId zoneId(RunContext runContext) throws IllegalVariableEvaluationException {
        if (this.getTimeZoneId() != null) {
            return ZoneId.of(runContext.render(this.getTimeZoneId()).as(String.class).orElseThrow());
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
            title = "The URI of the result file in Kestra's internal storage (`.ion` file i.e. Amazon Ion text format)."
        )
        private final URI uri;

        @Schema(
            title = "The number of fetched rows."
        )
        private final Long size;
    }
}
