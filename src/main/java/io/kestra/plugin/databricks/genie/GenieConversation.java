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
import com.databricks.sdk.service.dashboards.GenieGetConversationMessageRequest;
import com.databricks.sdk.service.dashboards.GenieGetMessageAttachmentQueryResultRequest;
import com.databricks.sdk.service.dashboards.GenieMessage;
import com.databricks.sdk.service.dashboards.GenieStartConversationMessageRequest;
import com.databricks.sdk.service.dashboards.MessageStatus;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ResultManifest;
import com.databricks.sdk.service.sql.StatementResponse;

import lombok.Builder;
import lombok.Getter;

final class GenieConversation {
    static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(20);
    static final int DEFAULT_MAX_ROWS = 1000;

    private GenieConversation() {
    }

    static Reply ask(GenieAPI genie, String spaceId, String question, Duration timeout, int maxRows) throws TimeoutException {
        var wait = requireTimeout(timeout);
        var limit = requireMaxRows(maxRows);
        var request = new GenieStartConversationMessageRequest()
            .setSpaceId(spaceId)
            .setContent(question);
        var started = genie.startConversation(request).getResponse();
        var message = await(genie, spaceId, started.getConversationId(), started.getMessageId(), wait);
        return fromMessage(genie, spaceId, message, started.getConversationId(), started.getMessageId(), limit);
    }

    static Reply followUp(GenieAPI genie, String spaceId, String conversationId, String question, Duration timeout, int maxRows) throws TimeoutException {
        var wait = requireTimeout(timeout);
        var limit = requireMaxRows(maxRows);
        var request = new GenieCreateConversationMessageRequest()
            .setSpaceId(spaceId)
            .setConversationId(conversationId)
            .setContent(question);
        var created = genie.createMessage(request).getResponse();
        var resolvedConversationId = firstNonBlank(created.getConversationId(), conversationId);
        var resolvedMessageId = firstNonBlank(created.getMessageId(), created.getId());
        var message = await(genie, spaceId, resolvedConversationId, resolvedMessageId, wait);
        return fromMessage(genie, spaceId, message, resolvedConversationId, resolvedMessageId, limit);
    }

    static Duration requireTimeout(Duration timeout) {
        var wait = timeout == null ? DEFAULT_TIMEOUT : timeout;
        if (wait.isZero() || wait.isNegative()) {
            throw new IllegalArgumentException("The `timeout` property must be greater than zero");
        }
        return wait;
    }

    static int requireMaxRows(int maxRows) {
        if (maxRows < 0) {
            throw new IllegalArgumentException("The `maxRows` property must be zero or greater");
        }
        return maxRows;
    }

    private static GenieMessage await(GenieAPI genie, String spaceId, String conversationId, String messageId, Duration timeout) throws TimeoutException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            var message = genie.getMessage(
                new GenieGetConversationMessageRequest()
                    .setSpaceId(spaceId)
                    .setConversationId(conversationId)
                    .setMessageId(messageId)
            );
            if (message == null) {
                throw new IllegalStateException("Genie returned no message");
            }
            var status = message.getStatus();
            if (status == MessageStatus.COMPLETED) {
                return message;
            }
            if (status == MessageStatus.FAILED || status == MessageStatus.CANCELLED) {
                throw failed(message);
            }
            if (System.nanoTime() >= deadline) {
                throw timedOut(timeout);
            }
            var remainingMs = Duration.ofNanos(deadline - System.nanoTime()).toMillis();
            if (remainingMs <= 0) {
                throw timedOut(timeout);
            }
            try {
                Thread.sleep(Math.min(1000L, remainingMs));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for Genie", e);
            }
        }
    }

    private static IllegalStateException failed(GenieMessage message) {
        var detail = message.getError() == null ? null : message.getError().getError();
        if (isBlank(detail)) {
            return new IllegalStateException("Genie message " + message.getStatus());
        }
        return new IllegalStateException("Genie message " + message.getStatus() + ": " + detail);
    }

    private static TimeoutException timedOut(Duration timeout) {
        return new TimeoutException("Genie did not answer within " + timeout + ". Raise timeout or simplify the question.");
    }

    private static Reply fromMessage(GenieAPI genie, String spaceId, GenieMessage message, String fallbackConversationId, String fallbackMessageId, int maxRows) {
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
            result = queryResult(genie, spaceId, conversationId, messageId, queryAttachment, maxRows);
        }

        return Reply.builder()
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
        GenieAttachment attachment,
        int maxRows) {
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
        return rows(response.getStatementResponse(), maxRows);
    }

    private static List<Map<String, String>> rows(StatementResponse statement, int maxRows) {
        var data = statement.getResult() == null ? null : statement.getResult().getDataArray();
        // A further chunk or a truncated manifest would drop rows, so fail instead of returning a partial result.
        if (statement.getResult() != null && statement.getResult().getNextChunkIndex() != null) {
            throw new IllegalStateException("Genie query result has more chunks than this task reads");
        }
        if (statement.getManifest() != null && Boolean.TRUE.equals(statement.getManifest().getTruncated())) {
            throw new IllegalStateException("Genie query result was truncated");
        }
        if (data == null || data.isEmpty()) {
            return List.of();
        }
        if (data.size() > maxRows) {
            throw new IllegalStateException(
                "Genie query returned " + data.size() + " rows, which exceeds maxRows (" + maxRows + "). Raise maxRows or narrow the question."
            );
        }

        var columns = columnNames(statement.getManifest());
        var mappedRows = new ArrayList<Map<String, String>>();
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
            mappedRows.add(mapped);
        }
        return mappedRows;
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

    @Builder
    @Getter
    static class Reply {
        private String conversationId;
        private String messageId;
        private String text;
        private String query;
        private List<Map<String, String>> result;
    }
}
