package io.kestra.plugin.databricks.genie;

import java.time.Duration;
import java.util.List;
import java.util.Map;

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
                    host: <your-host>
                    spaceId: <your-space>
                    question: What was total revenue by region last quarter?

                  - id: follow_up
                    type: io.kestra.plugin.databricks.genie.Continue
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: <your-host>
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

    @Schema(title = "Timeout", description = "How long to wait for Genie, as an ISO-8601 duration. Defaults to 20 minutes.")
    @PluginProperty(group = "main")
    private Property<Duration> timeout;

    @Override
    public Output run(RunContext runContext) throws Exception {
        var reply = GenieConversation.followUp(
            workspaceClient(runContext).genie(),
            runContext.render(spaceId).as(String.class).orElseThrow(),
            runContext.render(conversationId).as(String.class).orElseThrow(),
            runContext.render(question).as(String.class).orElseThrow(),
            timeout == null ? null : runContext.render(timeout).as(Duration.class).orElse(null)
        );
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

        @Schema(title = "Query result", description = "Rows for the generated SQL, keyed by column name. Unset for a text-only answer.")
        private List<Map<String, String>> result;
    }
}
