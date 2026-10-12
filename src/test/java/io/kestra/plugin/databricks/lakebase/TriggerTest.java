package io.kestra.plugin.databricks.lakebase;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.utils.IdUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

class TriggerTest {
    @Test
    void shouldFireWhenQueryReturnedRows() {
        assertThat(Trigger.shouldFire(Query.Output.builder().size(2L).rows(List.of(Map.of("id", 1))).build()), is(true));
        assertThat(Trigger.shouldFire(Query.Output.builder().size(1L).row(Map.of("id", 1)).build()), is(true));
    }

    @Test
    void shouldNotFireWhenQueryReturnedNoRows() {
        assertThat(Trigger.shouldFire(Query.Output.builder().size(0L).build()), is(false));
        assertThat(Trigger.shouldFire(Query.Output.builder().build()), is(false));
        assertThat(Trigger.shouldFire(null), is(false));
    }

    @Test
    void queryCopiesConnectionAndSqlProperties() {
        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .interval(Duration.ofMinutes(1))
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .host(Property.ofValue("pg.example.com"))
            .port(Property.ofValue(15432))
            .database(Property.ofValue("orders_db"))
            .ssl(Property.ofValue(true))
            .sslMode(Property.ofValue(LakebaseConnectionInterface.SslMode.REQUIRE))
            .sql(Property.ofValue("SELECT * FROM orders WHERE status = 'pending'"))
            .afterSQL(Property.ofValue("UPDATE orders SET status = 'seen' WHERE status = 'pending'"))
            .fetchType(Property.ofValue(FetchType.FETCH))
            .build();

        var query = trigger.query();

        assertThat(query.getWorkspaceHost(), is(trigger.getWorkspaceHost()));
        assertThat(query.getClientId(), is(trigger.getClientId()));
        assertThat(query.getClientSecret(), is(trigger.getClientSecret()));
        assertThat(query.getEndpoint(), is(trigger.getEndpoint()));
        assertThat(query.getHost(), is(trigger.getHost()));
        assertThat(query.getPort(), is(trigger.getPort()));
        assertThat(query.getDatabase(), is(trigger.getDatabase()));
        assertThat(query.getSql(), is(trigger.getSql()));
        assertThat(query.getAfterSQL(), is(trigger.getAfterSQL()));
        assertThat(query.getFetchType(), is(trigger.getFetchType()));
        assertThat(query.getType(), notNullValue());
    }

    @Test
    void defaultIntervalIsOneMinute() {
        var trigger = Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .database(Property.ofValue("orders_db"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        assertThat(trigger.getInterval(), is(Duration.ofMinutes(1)));
    }
}
