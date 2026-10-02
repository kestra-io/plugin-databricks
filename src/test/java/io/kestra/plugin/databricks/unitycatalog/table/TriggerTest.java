package io.kestra.plugin.databricks.unitycatalog.table;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@KestraTest
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static TableInfo table(String name, long updatedAt) {
        return new TableInfo().setName(name).setFullName("main.landing_zone." + name).setUpdatedAt(updatedAt);
    }

    private static Trigger.TriggerBuilder<?, ?> trigger(StatefulTriggerInterface.On on) {
        return Trigger.builder()
            .id(IdUtils.create())
            .type(Trigger.class.getName())
            .catalogName(Property.ofValue("main"))
            .schemaName(Property.ofValue("landing_zone"))
            .stateKey(Property.ofValue(IdUtils.create()))
            .stateTtl(Property.ofValue(Duration.ofHours(1)))
            .on(Property.ofValue(on));
    }

    @SafeVarargs
    private static Trigger spyWithTables(Trigger trigger, Iterable<TableInfo>... polls) throws Exception {
        var api = mock(TablesAPI.class);
        var first = polls[0];
        var rest = Arrays.copyOfRange(polls, 1, polls.length);
        when(api.list(eq("main"), eq("landing_zone"))).thenReturn(first, rest);

        var client = mock(WorkspaceClient.class);
        when(client.tables()).thenReturn(api);

        var spied = spy(trigger);
        doReturn(client).when(spied).workspaceClient(any());
        return spied;
    }

    @Test
    void firesOnlyForNewTables() throws Exception {
        var trigger = trigger(StatefulTriggerInterface.On.CREATE).build();
        var spied = spyWithTables(
            trigger,
            List.of(table("events", 1L)),
            List.of(table("events", 1L)),
            List.of(table("events", 1L), table("users", 1L)),
            List.of(table("events", 2L), table("users", 1L))
        );
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        // first poll: every existing table is new
        var first = spied.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));
        assertThat(first.get().getTrigger().getVariables().get("size"), is(1));

        // nothing changed
        assertThat(spied.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        // a new table appears
        var third = spied.evaluate(context.getKey(), context.getValue());
        assertThat(third.isPresent(), is(true));
        assertThat(third.get().getTrigger().getVariables().get("fullNames"), is(List.of("main.landing_zone.users")));

        // an existing table is modified, which is ignored with `on: CREATE`
        assertThat(spied.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void firesOnUpdatedTables() throws Exception {
        var trigger = trigger(StatefulTriggerInterface.On.CREATE_OR_UPDATE).build();
        var spied = spyWithTables(
            trigger,
            List.of(table("events", 1L)),
            List.of(table("events", 2L))
        );
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(spied.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));

        var second = spied.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(true));
        assertThat(second.get().getTrigger(), notNullValue());
        assertThat(second.get().getTrigger().getVariables().get("fullNames"), is(List.of("main.landing_zone.events")));
    }
}
