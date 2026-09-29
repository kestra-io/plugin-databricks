package io.kestra.plugin.databricks.genie;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.service.dashboards.GenieAPI;
import com.databricks.sdk.service.dashboards.GenieAttachment;
import com.databricks.sdk.service.dashboards.GenieCreateConversationMessageRequest;
import com.databricks.sdk.service.dashboards.GenieGetMessageAttachmentQueryResultRequest;
import com.databricks.sdk.service.dashboards.GenieGetMessageQueryResultResponse;
import com.databricks.sdk.service.dashboards.GenieMessage;
import com.databricks.sdk.service.dashboards.GenieQueryAttachment;
import com.databricks.sdk.service.dashboards.GenieService;
import com.databricks.sdk.service.dashboards.GenieStartConversationMessageRequest;
import com.databricks.sdk.service.dashboards.GenieStartConversationResponse;
import com.databricks.sdk.service.dashboards.MessageStatus;
import com.databricks.sdk.service.dashboards.TextAttachment;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ResultData;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.ResultSchema;
import com.databricks.sdk.service.sql.StatementResponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GenieConversationTest {
    @Test
    void textAnswerStaysOutOfQueryAndResult() throws Exception {
        var fake = new FakeGenie();
        fake.started = new GenieStartConversationResponse().setConversationId("conv-1").setMessageId("msg-1");
        fake.completed = new GenieMessage()
            .setStatus(MessageStatus.COMPLETED)
            .setAttachments(
                List.of(
                    new GenieAttachment().setText(new TextAttachment().setContent("Revenue was flat."))
                )
            );

        var output = GenieConversation.ask(new GenieAPI(fake.service()), "space-1", "How was revenue?", null);

        assertThat(output.getConversationId(), is("conv-1"));
        assertThat(output.getMessageId(), is("msg-1"));
        assertThat(output.getText(), is("Revenue was flat."));
        assertThat(output.getQuery(), nullValue());
        assertThat(output.getResult(), nullValue());
        assertThat(fake.queryResultCalls, is(0));
    }

    @Test
    void sqlAnswerIsSeparateFromText() throws Exception {
        var fake = sqlFake("SELECT region, revenue FROM sales");
        fake.completed = fake.completed.setAttachments(
            List.of(
                new GenieAttachment().setText(new TextAttachment().setContent("Here is the breakdown.")),
                new GenieAttachment()
                    .setAttachmentId("att-q")
                    .setQuery(new GenieQueryAttachment().setQuery("SELECT region, revenue FROM sales"))
            )
        );

        var output = GenieConversation.ask(new GenieAPI(fake.service()), "space-1", "Revenue by region?", Duration.ofMinutes(2));

        assertThat(output.getText(), is("Here is the breakdown."));
        assertThat(output.getQuery(), is("SELECT region, revenue FROM sales"));
        assertThat(output.getResult().size(), is(1));
        assertThat(output.getResult().getFirst().get("region"), is("emea"));
        assertThat(output.getResult().getFirst().get("revenue"), is("10"));
        assertThat(fake.queryResultCalls, is(1));
        assertThat(fake.lastQueryRequest.getAttachmentId(), is("att-q"));
        assertThat(fake.lastQueryRequest.getConversationId(), is("conv-1"));
        assertThat(fake.lastQueryRequest.getMessageId(), is("msg-1"));
    }

    @Test
    void sqlOnlyAnswerLeavesTextUnset() throws Exception {
        var fake = sqlFake("SELECT 1");

        var output = GenieConversation.ask(new GenieAPI(fake.service()), "space-1", "Count rows", null);

        assertThat(output.getText(), nullValue());
        assertThat(output.getQuery(), is("SELECT 1"));
        assertThat(output.getResult().getFirst().get("region"), is("emea"));
    }

    @Test
    void continueSendsFollowUpOnConversation() throws Exception {
        var fake = new FakeGenie();
        fake.created = new GenieMessage().setMessageId("msg-1").setConversationId("conv-1");
        fake.completed = new GenieMessage()
            .setStatus(MessageStatus.COMPLETED)
            .setConversationId("conv-1")
            .setMessageId("msg-2")
            .setAttachments(List.of(new GenieAttachment().setText(new TextAttachment().setContent("By month next."))));

        var output = GenieConversation.followUp(
            new GenieAPI(fake.service()),
            "space-1",
            "conv-1",
            "Break that down by month",
            null
        );

        assertThat(fake.lastConversationId, is("conv-1"));
        assertThat(fake.lastQuestion, is("Break that down by month"));
        assertThat(output.getConversationId(), is("conv-1"));
        assertThat(output.getMessageId(), is("msg-2"));
        assertThat(output.getText(), is("By month next."));
        assertThat(output.getQuery(), nullValue());
    }

    @Test
    void failedMessageThrows() {
        var fake = new FakeGenie();
        fake.started = new GenieStartConversationResponse().setConversationId("conv-1").setMessageId("msg-1");
        fake.completed = new GenieMessage().setStatus(MessageStatus.FAILED);

        assertThrows(
            IllegalStateException.class,
            () -> GenieConversation.ask(new GenieAPI(fake.service()), "space-1", "question", Duration.ofSeconds(5))
        );
    }

    @Test
    void timeoutIsPassedToTheWait() {
        var fake = new FakeGenie();
        fake.started = new GenieStartConversationResponse().setConversationId("conv-1").setMessageId("msg-1");
        fake.completed = new GenieMessage().setStatus(MessageStatus.SUBMITTED);

        assertThrows(
            TimeoutException.class,
            () -> GenieConversation.ask(new GenieAPI(fake.service()), "space-1", "question", Duration.ZERO)
        );
    }

    @Test
    void extraResultChunkFailsInsteadOfDroppingRows() {
        var fake = sqlFake("SELECT 1");
        fake.queryResult = new GenieGetMessageQueryResultResponse()
            .setStatementResponse(
                fake.queryResult.getStatementResponse().setResult(
                    new ResultData().setDataArray(List.of(List.of("emea", "10"))).setNextChunkIndex(1L)
                )
            );

        assertThrows(
            IllegalStateException.class,
            () -> GenieConversation.ask(new GenieAPI(fake.service()), "space-1", "question", null)
        );
    }

    private static FakeGenie sqlFake(String sql) {
        var fake = new FakeGenie();
        fake.started = new GenieStartConversationResponse().setConversationId("conv-1").setMessageId("msg-1");
        fake.completed = new GenieMessage()
            .setStatus(MessageStatus.COMPLETED)
            .setConversationId("conv-1")
            .setMessageId("msg-1")
            .setAttachments(
                List.of(
                    new GenieAttachment()
                        .setAttachmentId("att-q")
                        .setQuery(new GenieQueryAttachment().setQuery(sql))
                )
            );
        fake.queryResult = new GenieGetMessageQueryResultResponse().setStatementResponse(
            new StatementResponse()
                .setManifest(
                    new ResultManifest().setSchema(
                        new ResultSchema().setColumns(
                            List.of(
                                new ColumnInfo().setName("revenue").setPosition(1L),
                                new ColumnInfo().setName("region").setPosition(0L)
                            )
                        )
                    )
                )
                .setResult(new ResultData().setDataArray(List.of(List.of("emea", "10"))))
        );
        return fake;
    }

    static final class FakeGenie {
        GenieStartConversationResponse started;
        GenieMessage created;
        GenieMessage completed;
        GenieGetMessageQueryResultResponse queryResult;
        String lastQuestion;
        String lastConversationId;
        GenieGetMessageAttachmentQueryResultRequest lastQueryRequest;
        int queryResultCalls;

        GenieService service() {
            return (GenieService) Proxy.newProxyInstance(
                GenieService.class.getClassLoader(),
                new Class<?>[] { GenieService.class },
                (proxy, method, args) ->
                {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "toString" -> "FakeGenie";
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "equals" -> proxy == args[0];
                            default -> null;
                        };
                    }
                    return switch (method.getName()) {
                        case "startConversation" -> {
                            var request = (GenieStartConversationMessageRequest) args[0];
                            lastQuestion = request.getContent();
                            yield started;
                        }
                        case "createMessage" -> {
                            var request = (GenieCreateConversationMessageRequest) args[0];
                            lastQuestion = request.getContent();
                            lastConversationId = request.getConversationId();
                            yield created;
                        }
                        case "getMessage" -> completed;
                        case "getMessageAttachmentQueryResult" -> {
                            queryResultCalls++;
                            lastQueryRequest = (GenieGetMessageAttachmentQueryResultRequest) args[0];
                            yield queryResult;
                        }
                        default -> throw new UnsupportedOperationException(method.getName());
                    };
                }
            );
        }
    }
}
