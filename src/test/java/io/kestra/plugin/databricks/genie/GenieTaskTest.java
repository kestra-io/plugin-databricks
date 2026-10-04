package io.kestra.plugin.databricks.genie;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.service.dashboards.GenieAPI;
import com.databricks.sdk.service.dashboards.GenieAttachment;
import com.databricks.sdk.service.dashboards.GenieMessage;
import com.databricks.sdk.service.dashboards.GenieStartConversationResponse;
import com.databricks.sdk.service.dashboards.MessageStatus;
import com.databricks.sdk.service.dashboards.TextAttachment;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class GenieTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private ModelValidator modelValidator;

    @Test
    void askQuestionRendersSpaceAndQuestionAndMapsText() throws Exception {
        var fake = textFake();
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofExpression("{{ inputs.spaceId }}"))
            .question(Property.ofExpression("{{ inputs.question }}"))
            .build();
        var runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            ImmutableMap.of("spaceId", "space-9", "question", "revenue by region")
        );

        var output = task.answer(runContext, new GenieAPI(fake.service()));

        assertThat(fake.lastSpaceId, is("space-9"));
        assertThat(fake.lastQuestion, is("revenue by region"));
        assertThat(output.getText(), is("ok"));
        assertThat(output.getQuery(), nullValue());
        assertThat(output.getResult(), nullValue());
        assertThat(output.getConversationId(), is("conv-1"));
        assertThat(output.getMessageId(), is("msg-1"));
    }

    @Test
    void askQuestionMapsQueryAndResult() throws Exception {
        var fake = GenieConversationTest.sqlFake("SELECT region, revenue FROM sales");
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.ofValue("Revenue by region?"))
            .maxRows(Property.ofValue(10))
            .build();

        var output = task.answer(runContext(task), new GenieAPI(fake.service()));

        assertThat(output.getText(), nullValue());
        assertThat(output.getQuery(), is("SELECT region, revenue FROM sales"));
        assertThat(output.getResult().size(), is(1));
        assertThat(output.getResult().getFirst().get("region"), is("emea"));
        assertThat(output.getResult().getFirst().get("revenue"), is("10"));
    }

    @Test
    void continueRendersConversationIdAndMapsText() throws Exception {
        var fake = textFake();
        fake.created = new GenieMessage().setConversationId("conv-9").setMessageId("msg-9");
        var task = Continue.builder()
            .id(IdUtils.create())
            .type(Continue.class.getName())
            .spaceId(Property.ofExpression("{{ inputs.spaceId }}"))
            .conversationId(Property.ofExpression("{{ inputs.conversationId }}"))
            .question(Property.ofExpression("{{ inputs.question }}"))
            .build();
        var runContext = TestsUtils.mockRunContext(
            runContextFactory,
            task,
            ImmutableMap.of("spaceId", "space-9", "conversationId", "conv-9", "question", "break it down")
        );

        var output = task.answer(runContext, new GenieAPI(fake.service()));

        assertThat(fake.lastSpaceId, is("space-9"));
        assertThat(fake.lastConversationId, is("conv-9"));
        assertThat(fake.lastQuestion, is("break it down"));
        assertThat(output.getText(), is("ok"));
        assertThat(output.getQuery(), nullValue());
        assertThat(output.getResult(), nullValue());
    }

    @Test
    void continueMapsQueryAndResult() throws Exception {
        var fake = GenieConversationTest.sqlFake("SELECT 1");
        fake.created = new GenieMessage().setConversationId("conv-1").setMessageId("msg-1");
        var task = Continue.builder()
            .id(IdUtils.create())
            .type(Continue.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .conversationId(Property.ofValue("conv-1"))
            .question(Property.ofValue("count"))
            .build();

        var output = task.answer(runContext(task), new GenieAPI(fake.service()));

        assertThat(output.getText(), nullValue());
        assertThat(output.getQuery(), is("SELECT 1"));
        assertThat(output.getResult().getFirst().get("region"), is("emea"));
    }

    @Test
    void missingQuestionIsRejectedAtValidation() {
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .build();

        var validation = modelValidator.isValid(task);
        assertThat(validation.isPresent(), is(true));
        assertThat(validation.get().getMessage(), containsString("question"));
    }

    @Test
    void missingConversationIdIsRejectedAtValidation() {
        var task = Continue.builder()
            .id(IdUtils.create())
            .type(Continue.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.ofValue("break it down"))
            .build();

        var validation = modelValidator.isValid(task);
        assertThat(validation.isPresent(), is(true));
        assertThat(validation.get().getMessage(), containsString("conversationId"));
    }

    @Test
    void unsetSpaceIdNamesTheProperty() {
        var fake = textFake();
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.<String> builder().build())
            .question(Property.ofValue("revenue"))
            .build();

        var failure = assertThrows(
            IllegalArgumentException.class,
            () -> task.answer(runContext(task), new GenieAPI(fake.service()))
        );
        assertThat(failure.getMessage(), containsString("spaceId"));
        assertThat(fake.lastQuestion, nullValue());
    }

    @Test
    void unsetQuestionNamesTheProperty() {
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.<String> builder().build())
            .build();

        var failure = assertThrows(
            IllegalArgumentException.class,
            () -> task.answer(runContext(task), new GenieAPI(textFake().service()))
        );
        assertThat(failure.getMessage(), containsString("question"));
    }

    @Test
    void unsetConversationIdNamesTheProperty() {
        var task = Continue.builder()
            .id(IdUtils.create())
            .type(Continue.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .conversationId(Property.<String> builder().build())
            .question(Property.ofValue("break it down"))
            .build();

        var failure = assertThrows(
            IllegalArgumentException.class,
            () -> task.answer(runContext(task), new GenieAPI(textFake().service()))
        );
        assertThat(failure.getMessage(), containsString("conversationId"));
    }

    @Test
    void zeroTimeoutIsRejected() {
        var fake = textFake();
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.ofValue("revenue"))
            .timeout(Property.ofValue(Duration.ZERO))
            .build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.answer(runContext(task), new GenieAPI(fake.service())));
        assertThat(failure.getMessage(), containsString("timeout"));
        assertThat(fake.lastQuestion, nullValue());
    }

    @Test
    void negativeTimeoutIsRejected() {
        var fake = textFake();
        var task = Continue.builder()
            .id(IdUtils.create())
            .type(Continue.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .conversationId(Property.ofValue("conv-1"))
            .question(Property.ofValue("break it down"))
            .timeout(Property.ofValue(Duration.ofSeconds(-1)))
            .build();

        var failure = assertThrows(IllegalArgumentException.class, () -> task.answer(runContext(task), new GenieAPI(fake.service())));
        assertThat(failure.getMessage(), containsString("timeout"));
        assertThat(fake.lastQuestion, nullValue());
    }

    @Test
    void askQuestionFailsWhenRowsExceedMaxRows() {
        var fake = GenieConversationTest.sqlFake("SELECT 1");
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.ofValue("revenue"))
            .maxRows(Property.ofValue(0))
            .build();

        var failure = assertThrows(
            IllegalStateException.class,
            () -> task.answer(runContext(task), new GenieAPI(fake.service()))
        );
        assertThat(failure.getMessage(), containsString("maxRows"));
        assertThat(failure.getMessage(), containsString("1"));
    }

    @Test
    void timeoutDefaultsToTwentyMinutes() throws Exception {
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.ofValue("revenue"))
            .build();

        var rendered = runContext(task).render(task.getTimeout()).as(Duration.class).orElseThrow();
        assertThat(rendered, is(Duration.ofMinutes(20)));
        assertThat(runContext(task).render(task.getMaxRows()).as(Integer.class).orElseThrow(), is(1000));
    }

    private io.kestra.core.runners.RunContext runContext(AskQuestion task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }

    private io.kestra.core.runners.RunContext runContext(Continue task) {
        return TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
    }

    private static GenieConversationTest.FakeGenie textFake() {
        var fake = new GenieConversationTest.FakeGenie();
        fake.started = new GenieStartConversationResponse().setConversationId("conv-1").setMessageId("msg-1");
        fake.created = new GenieMessage().setConversationId("conv-1").setMessageId("msg-1");
        fake.completed = new GenieMessage()
            .setStatus(MessageStatus.COMPLETED)
            .setConversationId("conv-1")
            .setMessageId("msg-1")
            .setAttachments(List.of(new GenieAttachment().setText(new TextAttachment().setContent("ok"))));
        return fake;
    }
}
