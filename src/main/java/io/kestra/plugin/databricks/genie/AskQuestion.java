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
            title = "Ask Genie a question and log the generated SQL and result.",
            full = true,
            code = """
                id: genie_ask_question
                namespace: company.team

                tasks:
                  - id: ask_revenue_question
                    type: io.kestra.plugin.databricks.genie.AskQuestion
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    spaceId: 01ef8b2f3a9c1a2d9b7e5f6a1b2c3d4e
                    question: What was total revenue by region last quarter?
                    timeout: PT2M

                  - id: log_answer
                    type: io.kestra.plugin.core.log.Log
                    message: "Genie SQL: {{ outputs.ask_revenue_question.query }} — Result: {{ outputs.ask_revenue_question.result }}"
                """
        )
    }
)
@Schema(
    title = "Ask a Genie space a question",
    description = """
        Starts a Genie conversation and blocks until Genie answers.
        The Genie space must already exist in the workspace, with a SQL warehouse and the tables it is allowed to query.
        A text answer is returned on `text`. Generated SQL is returned on `query` with rows on `result`. A text-only answer leaves `query` and `result` unset.
        """
)
public class AskQuestion extends AbstractTask implements RunnableTask<GenieOutput> {
    @NotNull
    @Schema(
        title = "Genie space identifier",
        description = "Identifier of an existing Genie space. This task does not create the space."
    )
    @PluginProperty(group = "main")
    private Property<String> spaceId;

    @NotNull
    @Schema(title = "Question", description = "Plain-language question sent as the first message in a new conversation.")
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
        var output = GenieConversation.ask(
            genie,
            runContext.render(spaceId).as(String.class).orElseThrow(),
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
