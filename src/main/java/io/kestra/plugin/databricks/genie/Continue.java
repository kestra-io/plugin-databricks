package io.kestra.plugin.databricks.genie;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.databricks.sdk.service.dashboards.GenieAPI;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;

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
@Plugin(
    examples = {
        @Example(
            title = "Send a follow-up in the same Genie conversation.",
            full = true,
            code = """
                id: databricks_genie_follow_up
                namespace: company.team

                tasks:
                  - id: ask_question
                    type: io.kestra.plugin.databricks.genie.AskQuestion
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: https://adb-1234567890123456.7.azuredatabricks.net
                    spaceId: <your-space>
                    question: What was total revenue by region last quarter?

                  - id: follow_up
                    type: io.kestra.plugin.databricks.genie.Continue
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: https://adb-1234567890123456.7.azuredatabricks.net
                    spaceId: <your-space>
                    conversationId: "{{ outputs.ask_question.conversationId }}"
                    question: Now break that down by month.
                """
        )
    }
)
@Schema(
    title = "Continue a Genie conversation",
    description = """
        Sends a follow-up on an existing Genie conversation and waits until Genie answers.
        Use the conversationId from AskQuestion. Outputs match AskQuestion.
        """
)
public class Continue extends AbstractTask implements RunnableTask<Continue.Output> {
    @NotNull
    @Schema(title = "Genie space", description = "ID of an existing Genie space")
    @PluginProperty(group = "main")
    private Property<String> spaceId;

    @NotNull
    @Schema(title = "Conversation identifier", description = "conversationId from AskQuestion or a previous Continue")
    @PluginProperty(group = "main")
    private Property<String> conversationId;

    @NotNull
    @Schema(title = "Question")
    @PluginProperty(group = "main")
    private Property<String> question;

    @Builder.Default
    @Schema(
        title = "Timeout",
        description = "How long to wait for Genie, as an ISO-8601 duration. Defaults to 20 minutes. Must be greater than zero."
    )
    @PluginProperty(group = "main")
    private Property<Duration> timeout = Property.ofValue(GenieConversation.DEFAULT_TIMEOUT);

    @Builder.Default
    @Schema(
        title = "Maximum rows",
        description = "Maximum number of query rows included on result. Defaults to 1000. The task fails when Genie returns more rows than this; the error names maxRows and the row count."
    )
    @PluginProperty(group = "main")
    private Property<Integer> maxRows = Property.ofValue(GenieConversation.DEFAULT_MAX_ROWS);

    @Override
    public Output run(RunContext runContext) throws Exception {
        return answer(runContext, genieApi(runContext));
    }

    protected GenieAPI genieApi(RunContext runContext) throws IllegalVariableEvaluationException {
        return workspaceClient(runContext).genie();
    }

    Output answer(RunContext runContext, GenieAPI genie) throws Exception {
        var space = runContext.render(spaceId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The `spaceId` property is required, set it to the identifier of an existing Genie space"));
        var conversation = runContext.render(conversationId).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The `conversationId` property is required, set it to the conversationId from AskQuestion"));
        var asked = runContext.render(question).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("The `question` property is required, set it to the question to ask Genie"));
        var wait = runContext.render(timeout).as(Duration.class).orElse(null);
        var limit = runContext.render(maxRows).as(Integer.class).orElse(GenieConversation.DEFAULT_MAX_ROWS);

        var reply = GenieConversation.followUp(genie, space, conversation, asked, wait, limit);
        runContext.logger().info("Genie conversation {} message {} answered", reply.getConversationId(), reply.getMessageId());

        return Output.builder()
            .conversationId(reply.getConversationId())
            .messageId(reply.getMessageId())
            .text(reply.getText())
            .query(reply.getQuery())
            .result(reply.getResult())
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Conversation identifier", description = "Pass this to Continue for a follow-up")
        private String conversationId;

        @Schema(title = "Message identifier")
        private String messageId;

        @Schema(title = "Text answer", description = "Set when Genie replies with text. Unset when the answer is SQL only.")
        private String text;

        @Schema(title = "Generated SQL", description = "SQL Genie produced. Unset for a text-only answer.")
        private String query;

        @Schema(
            title = "Query result",
            description = "Rows for the generated SQL, keyed by column name. Unset for a text-only answer. Limited to maxRows; the task fails when Genie returns more."
        )
        private List<Map<String, String>> result;
    }
}
