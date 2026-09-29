package io.kestra.plugin.databricks.genie;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.databricks.sdk.service.dashboards.GenieAPI;
import com.databricks.sdk.service.dashboards.GenieAttachment;
import com.databricks.sdk.service.dashboards.GenieCreateConversationMessageRequest;
import com.databricks.sdk.service.dashboards.GenieGetMessageAttachmentQueryResultRequest;
import com.databricks.sdk.service.dashboards.GenieMessage;
import com.databricks.sdk.service.dashboards.GenieStartConversationMessageRequest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.StatementResponse;

/**
 * Blocks on the Genie SDK wait until a message completes, then splits a text answer from generated SQL.
 */
final class GenieConversation {
    static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(20);

    private GenieConversation() {
    }

    static GenieOutput ask(GenieAPI genie, String spaceId, String question, Duration timeout) throws TimeoutException {
        var request = new GenieStartConversationMessageRequest()
            .setSpaceId(spaceId)
            .setContent(question);
        var wait = genie.startConversation(request);
        var started = wait.getResponse();
        var message = wait.get(timeout == null ? DEFAULT_TIMEOUT : timeout);
        return fromMessage(genie, spaceId, message, started.getConversationId(), started.getMessageId());
    }

    static GenieOutput continueConversation(
        GenieAPI genie,
        String spaceId,
        String conversationId,
        String question,
        Duration timeout) throws TimeoutException {
        var request = new GenieCreateConversationMessageRequest()
            .setSpaceId(spaceId)
            .setConversationId(conversationId)
            .setContent(question);
        var wait = genie.createMessage(request);
        var created = wait.getResponse();
        var message = wait.get(timeout == null ? DEFAULT_TIMEOUT : timeout);
        return fromMessage(
            genie,
            spaceId,
            message,
            firstNonBlank(created.getConversationId(), conversationId),
            firstNonBlank(created.getMessageId(), created.getId())
        );
    }

    static GenieOutput fromMessage(
        GenieAPI genie,
        String spaceId,
        GenieMessage message,
        String fallbackConversationId,
        String fallbackMessageId) {
        if (message == null) {
            throw new IllegalStateException("Genie returned no message");
        }

        var conversationId = firstNonBlank(message.getConversationId(), fallbackConversationId);
        var messageId = firstNonBlank(message.getMessageId(), message.getId(), fallbackMessageId);
        String text = null;
        GenieAttachment queryAttachment = null;
        if (message.getAttachments() != null) {
            var textParts = new StringBuilder();
            for (GenieAttachment attachment : message.getAttachments()) {
                if (attachment.getText() != null && !isBlank(attachment.getText().getContent())) {
                    if (!textParts.isEmpty()) {
                        textParts.append('\n');
                    }
                    textParts.append(attachment.getText().getContent());
                }
                // ponytail: first query attachment only. Upgrade path: return one output per attachment.
                if (queryAttachment == null && attachment.getQuery() != null) {
                    queryAttachment = attachment;
                }
            }
            if (!textParts.isEmpty()) {
                text = textParts.toString();
            }
        }

        String query = null;
        List<Map<String, String>> result = null;
        if (queryAttachment != null) {
            query = queryAttachment.getQuery().getQuery();
            result = queryResult(genie, spaceId, conversationId, messageId, queryAttachment);
        }

        return GenieOutput.builder()
            .conversationId(conversationId)
            .messageId(messageId)
            .text(text)
            .query(query)
            .result(result)
            .build();
    }

    private static List<Map<String, String>> queryResult(
        GenieAPI genie,
        String spaceId,
        String conversationId,
        String messageId,
        GenieAttachment attachment) {
        var response = genie.getMessageAttachmentQueryResult(
            new GenieGetMessageAttachmentQueryResultRequest()
                .setSpaceId(spaceId)
                .setConversationId(conversationId)
                .setMessageId(messageId)
                .setAttachmentId(firstNonBlank(attachment.getAttachmentId(), attachment.getQuery().getId()))
        );
        if (response == null || response.getStatementResponse() == null) {
            return List.of();
        }
        return rows(response.getStatementResponse());
    }

    private static List<Map<String, String>> rows(StatementResponse statement) {
        var data = statement.getResult() == null ? null : statement.getResult().getDataArray();
        // ponytail: one inline chunk. Upgrade path: follow ResultData.nextChunkIndex.
        if (statement.getResult() != null && statement.getResult().getNextChunkIndex() != null) {
            throw new IllegalStateException("Genie query result has more chunks than this task reads");
        }
        if (statement.getManifest() != null && Boolean.TRUE.equals(statement.getManifest().getTruncated())) {
            throw new IllegalStateException("Genie query result was truncated");
        }
        if (data == null || data.isEmpty()) {
            return List.of();
        }

        var columns = columnNames(statement.getManifest());
        var rows = new ArrayList<Map<String, String>>();
        for (Collection<String> row : data) {
            var mapped = new LinkedHashMap<String, String>();
            var index = 0;
            if (row != null) {
                for (String value : row) {
                    var name = index < columns.size() ? columns.get(index) : "column_" + index;
                    mapped.put(name, value);
                    index++;
                }
            }
            rows.add(mapped);
        }
        return rows;
    }

    private static List<String> columnNames(ResultManifest manifest) {
        if (manifest == null || manifest.getSchema() == null || manifest.getSchema().getColumns() == null) {
            return List.of();
        }
        var columns = new ArrayList<>(manifest.getSchema().getColumns());
        columns.sort(Comparator.comparing(ColumnInfo::getPosition, Comparator.nullsLast(Long::compareTo)));
        var names = new ArrayList<String>();
        for (ColumnInfo column : columns) {
            names.add(isBlank(column.getName()) ? "column_" + names.size() : column.getName());
        }
        return names;
    }

    private static String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (!isBlank(value)) {
                return value;
            }
        }
        return null;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
