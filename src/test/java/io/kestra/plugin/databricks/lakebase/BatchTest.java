package io.kestra.plugin.databricks.lakebase;

import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
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
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class BatchTest {
    private String jdbcUrl;

    @Inject
    private RunContextFactory runContextFactory;

    @BeforeEach
    void setUp() throws Exception {
        jdbcUrl = "jdbc:h2:mem:lakebase-batch-" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1";
        try (
            Connection connection = DriverManager.getConnection(jdbcUrl, "sa", "");
            Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE TABLE orders_audit (payload VARCHAR(255))");
        }
        LakebaseService.overrideConnection((ctx, conn) -> DriverManager.getConnection(jdbcUrl, "sa", ""));
    }

    @AfterEach
    void tearDown() {
        LakebaseService.resetConnectionOverride();
    }

    @Test
    void insertsOneRowPerScalarWhenSqlHasASinglePlaceholder() throws Exception {
        var task = baseBatch()
            .sql(Property.ofValue("INSERT INTO orders_audit (payload) VALUES (?)"))
            .parameterGroups(
                Property.ofValue(
                    List.of(
                        Batch.ParameterGroup.builder().parameters(List.of("alpha", "beta", "gamma")).build()
                    )
                )
            )
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getRowCount(), is(3L));
        assertThat(countRows(), is(3));
    }

    @Test
    void insertsExplicitRows() throws Exception {
        var task = baseBatch()
            .sql(Property.ofValue("INSERT INTO orders_audit (payload) VALUES (?)"))
            .parameterGroups(
                Property.ofValue(
                    List.of(
                        Batch.ParameterGroup.builder().parameters(List.of("one")).build(),
                        Batch.ParameterGroup.builder().parameters(List.of("two")).build()
                    )
                )
            )
            .build();

        var output = task.run(runContext(task));

        assertThat(output.getRowCount(), is(2L));
        assertThat(countRows(), is(2));
    }

    @Test
    void insertsFromIonFile() throws Exception {
        var task = baseBatch()
            .sql(Property.ofValue("INSERT INTO orders_audit (payload) VALUES (?)"))
            .build();
        var runContext = runContext(task);

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (var output = new FileOutputStream(tempFile)) {
            FileSerde.write(output, Map.of("payload", "from-file-1"));
            FileSerde.write(output, Map.of("payload", "from-file-2"));
        }
        var uri = runContext.storage().putFile(tempFile);

        var batch = baseBatch()
            .sql(Property.ofValue("INSERT INTO orders_audit (payload) VALUES (?)"))
            .from(Property.ofValue(uri.toString()))
            .build();

        var output = batch.run(runContext(batch));

        assertThat(output.getRowCount(), is(2L));
        assertThat(countRows(), is(2));
    }

    @Test
    void rejectsEmptyBatch() {
        var task = baseBatch()
            .sql(Property.ofValue("INSERT INTO orders_audit (payload) VALUES (?)"))
            .build();

        assertThrows(IllegalArgumentException.class, () -> task.run(runContext(task)));
    }

    private Batch.BatchBuilder<?, ?> baseBatch() {
        return Batch.builder()
            .id(IdUtils.create())
            .type(Batch.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue("00000000-0000-0000-0000-000000000001"))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .database(Property.ofValue("orders_db"));
    }

    private int countRows() throws Exception {
        try (
            Connection connection = DriverManager.getConnection(jdbcUrl, "sa", "");
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM orders_audit")
        ) {
            rs.next();
            return rs.getInt(1);
        }
    }

    private io.kestra.core.runners.RunContext runContext(Batch task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }
}
