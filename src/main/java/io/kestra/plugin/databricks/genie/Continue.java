package io.kestra.plugin.databricks.genie;

import java.time.Duration;

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
            title = "Ask a follow-up in the same Genie conversation.",
            full = true,
            code = """
                id: genie_follow_up
                namespace: company.team

                tasks:
                  - id: ask_initial
                    type: io.kestra.plugin.databricks.genie.AskQuestion
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    spaceId: 01ef8b2f3a9c1a2d9b7e5f6a1b2c3d4e
                    question: What was total revenue by region last quarter?

                  - id: ask_followup
                    type: io.kestra.plugin.databricks.genie.Continue
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    spaceId: 01ef8b2f3a9c1a2d9b7e5f6a1b2c3d4e
                    conversationId: "{{ outputs.ask_initial.conversationId }}"
                    question: Now break that down by month.
                """
        )
    }
)
@Schema(
    title = "Continue a Genie conversation",
    description = """
        Sends a follow-up question on an existing Genie conversation and blocks until Genie answers.
        Use the conversationId from AskQuestion. Outputs match AskQuestion: text stays separate from generated SQL and its result.
        """
)
public class Continue extends AbstractTask implements RunnableTask<GenieOutput> {
    @NotNull
    @Schema(
        title = "Genie space identifier",
        description = "Identifier of the existing Genie space that owns the conversation."
    )
    @PluginProperty(group = "main")
    private Property<String> spaceId;

    @NotNull
    @Schema(title = "Conversation identifier", description = "conversationId returned by AskQuestion or a previous Continue.")
    @PluginProperty(group = "main")
    private Property<String> conversationId;

    @NotNull
    @Schema(title = "Question", description = "Follow-up question sent on the existing conversation.")
    @PluginProperty(group = "main")
    private Property<String> question;

    @Schema(
        title = "Time to wait for Genie",
        description = "ISO-8601 duration. The task polls until Genie completes or this elapses. Defaults to 20 minutes."
    )
    @PluginProperty(group = "main")
    private Property<Duration> timeout;

    @Override
    public GenieOutput run(RunContext runContext) throws Exception {
        return answer(runContext, genieApi(runContext));
    }

    GenieOutput answer(RunContext runContext, GenieAPI genie) throws Exception {
        var output = GenieConversation.continueConversation(
            genie,
            runContext.render(spaceId).as(String.class).orElseThrow(),
            runContext.render(conversationId).as(String.class).orElseThrow(),
            runContext.render(question).as(String.class).orElseThrow(),
            timeout == null ? null : runContext.render(timeout).as(Duration.class).orElse(null)
        );
        runContext.logger().info("Genie conversation {} message {} answered", output.getConversationId(), output.getMessageId());
        return output;
    }

    GenieAPI genieApi(RunContext runContext) throws IllegalVariableEvaluationException {
        return workspaceClient(runContext).genie();
    }
}
