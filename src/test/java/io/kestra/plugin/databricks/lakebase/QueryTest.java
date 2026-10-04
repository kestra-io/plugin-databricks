package io.kestra.plugin.databricks.lakebase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class QueryTest {
    private String jdbcUrl;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = "jdbc:h2:mem:lakebase-query-" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (
            Connection connection = DriverManager.getConnection(jdbcUrl, "sa", "");
            Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(32))");
            statement.execute("INSERT INTO orders VALUES (1, 'pending'), (2, 'done')");
        }
        LakebaseService.overrideConnection((ctx, conn) -> DriverManager.getConnection(jdbcUrl, "sa", ""));
    }

    @AfterEach
    void tearDown() {
        LakebaseService.resetConnectionOverride();
    }

    @Test
    void fetchReturnsAllRows() throws Exception {
        var task = baseQuery()
            .sql(Property.ofValue("SELECT id, status FROM orders ORDER BY id"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getSize(), is(2L));
        assertThat(output.getRows(), hasSize(2));
        assertThat(output.getRows().getFirst().get("ID"), is(1));
        assertThat(output.getRows().get(1).get("STATUS"), is("done"));
        assertThat(output.getRow(), nullValue());
        assertThat(output.getUri(), nullValue());
    }

    @Test
    void fetchOneReturnsTheFirstRow() throws Exception {
        var task = baseQuery()
            .sql(Property.ofValue("SELECT id, status FROM orders ORDER BY id"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRow().get("ID"), is(1));
        assertThat(output.getRows(), nullValue());
    }

    @Test
    void storeWritesRowsToInternalStorage() throws Exception {
        var task = baseQuery()
            .sql(Property.ofValue("SELECT id, status FROM orders ORDER BY id"))
            .fetchType(Property.ofValue(FetchType.STORE))
            .build();

        var runContext = runContext(task);
        var output = task.run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getUri(), notNullValue());

        try (
            var input = runContext.storage().getFile(output.getUri());
            var reader = new java.io.InputStreamReader(input, java.nio.charset.StandardCharsets.UTF_8)
        ) {
            List<Object> stored = FileSerde.readAll(reader).collectList().block();
            assertThat(stored, hasSize(2));
        }
    }

    @Test
    void noneDoesNotFetchRows() throws Exception {
        var task = baseQuery()
            .sql(Property.ofValue("UPDATE orders SET status = 'closed' WHERE id = 2"))
            .fetchType(Property.ofValue(FetchType.NONE))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getSize(), is(0L));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getRow(), nullValue());
        assertThat(output.getUri(), nullValue());
    }

    @Test
    void namedParametersAreBound() throws Exception {
        var task = baseQuery()
            .sql(Property.ofValue("SELECT id FROM orders WHERE status = :status"))
            .parameters(Property.ofValue(Map.of("status", "pending")))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows().getFirst().get("ID"), is(1));
    }

    @Test
    void afterSqlRunsInTheSameSession() throws Exception {
        var task = baseQuery()
            .sql(Property.ofValue("SELECT id FROM orders WHERE status = 'pending'"))
            .afterSQL(Property.ofValue("UPDATE orders SET status = 'processed' WHERE status = 'pending'"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var output = task.run(runContext(task));
        assertThat(output.getSize(), is(1L));

        var verify = baseQuery()
            .sql(Property.ofValue("SELECT status FROM orders WHERE id = 1"))
            .fetchType(Property.ofValue(FetchType.FETCH_ONE))
            .build();
        var updated = verify.run(runContext(verify));
        assertThat(updated.getRow().get("STATUS"), is("processed"));
    }

    private Query.QueryBuilder<?, ?> baseQuery() {
        return Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue("00000000-0000-0000-0000-000000000001"))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .database(Property.ofValue("orders_db"));
    }

    private io.kestra.core.runners.RunContext runContext(Query task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }
}
