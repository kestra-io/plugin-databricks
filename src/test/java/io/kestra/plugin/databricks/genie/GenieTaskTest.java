package io.kestra.plugin.databricks.genie;

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
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@KestraTest
class GenieTaskTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void askQuestionRendersTheQuestion() throws Exception {
        var fake = textFake();
        var task = AskQuestion.builder()
            .id(IdUtils.create())
            .type(AskQuestion.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .question(Property.ofExpression("{{ inputs.question }}"))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of("question", "revenue by region"));

        var output = task.answer(runContext, new GenieAPI(fake.service()));

        assertThat(fake.lastQuestion, is("revenue by region"));
        assertThat(output.getText(), is("ok"));
        assertThat(output.getQuery(), nullValue());
        assertThat(output.getResult(), nullValue());
        assertThat(output.getConversationId(), is("conv-1"));
        assertThat(output.getMessageId(), is("msg-1"));
    }

    @Test
    void continueRendersTheConversationId() throws Exception {
        var fake = textFake();
        fake.created = new GenieMessage().setConversationId("conv-9").setMessageId("msg-9");
        var task = Continue.builder()
            .id(IdUtils.create())
            .type(Continue.class.getName())
            .spaceId(Property.ofValue("space-1"))
            .conversationId(Property.ofExpression("{{ inputs.conversationId }}"))
            .question(Property.ofValue("break it down"))
            .build();
        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of("conversationId", "conv-9"));

        var output = task.answer(runContext, new GenieAPI(fake.service()));

        assertThat(fake.lastConversationId, is("conv-9"));
        assertThat(fake.lastQuestion, is("break it down"));
        assertThat(output.getMessageId(), is("msg-1"));
    }

    private static FakeGenie textFake() {
        var fake = new FakeGenie();
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
