package io.kestra.plugin.databricks.genie;

import java.lang.reflect.Proxy;

import com.databricks.sdk.service.dashboards.GenieCreateConversationMessageRequest;
import com.databricks.sdk.service.dashboards.GenieGetMessageAttachmentQueryResultRequest;
import com.databricks.sdk.service.dashboards.GenieGetMessageQueryResultResponse;
import com.databricks.sdk.service.dashboards.GenieMessage;
import com.databricks.sdk.service.dashboards.GenieService;
import com.databricks.sdk.service.dashboards.GenieStartConversationMessageRequest;
import com.databricks.sdk.service.dashboards.GenieStartConversationResponse;

final class FakeGenie {
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
