package io.kestra.plugin.databricks.genie;

import java.util.List;
import java.util.Map;

import io.kestra.core.models.tasks.Output;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class GenieOutput implements Output {
    @Schema(title = "Conversation identifier", description = "Pass this to Continue to send a follow-up in the same conversation.")
    private final String conversationId;

    @Schema(title = "Message identifier")
    private final String messageId;

    @Schema(
        title = "Text answer",
        description = "Genie text when the reply includes a text attachment. Unset for a SQL-only answer, and never a copy of the generated SQL."
    )
    private final String text;

    @Schema(
        title = "Generated SQL",
        description = "SQL Genie generated for the question. Unset for a text-only answer."
    )
    private final String query;

    @Schema(
        title = "Query result",
        description = "Rows for the generated SQL, one map per row keyed by column name. Unset for a text-only answer, so a text reply is not returned as a result set."
    )
    private final List<Map<String, String>> result;
}
