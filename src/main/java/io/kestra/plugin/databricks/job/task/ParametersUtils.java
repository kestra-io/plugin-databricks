package io.kestra.plugin.databricks.job.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.fasterxml.jackson.databind.type.MapType;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

final class ParametersUtils {
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapper.ofJson();
    private static final CollectionType LIST_OF_STRING = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class);
    private static final MapType MAP_OF_STRING_STRING = OBJECT_MAPPER.getTypeFactory().constructMapType(Map.class, String.class, String.class);

    private ParametersUtils() {
        // prevent instantiation
    }

    static List<String> listParameters(RunContext runContext, Object parameters) throws IllegalVariableEvaluationException {
        if (parameters == null) {
            return null;
        }

        if (parameters instanceof String parametersString) {
            String rendered = runContext.render(parametersString);
            try {
                return OBJECT_MAPPER.readValue(rendered, LIST_OF_STRING);
            } catch (JsonProcessingException e) {
                throw new IllegalVariableEvaluationException(e);
            }
        } else if (parameters instanceof Collection) {
            Collection<String> rendered = (Collection<String>) parameters;
            return rendered.stream().map(throwFunction(param -> runContext.render(param))).toList();
        } else {
            throw new IllegalArgumentException("Invalid parameters type '" + parameters.getClass() + "'");
        }
    }

    static Map<String, String> mapParameters(RunContext runContext, Object parameters) throws IllegalVariableEvaluationException {
        if (parameters == null) {
            return null;
        }

        if (parameters instanceof String parametersString) {
            String rendered = runContext.render(parametersString);
            try {
                return OBJECT_MAPPER.readValue(rendered, MAP_OF_STRING_STRING);
            } catch (JsonProcessingException e) {
                throw new IllegalVariableEvaluationException(e);
            }
        } else if (parameters instanceof Map) {
            Map<String, String> rendered = (Map<String, String>) parameters;
            return runContext.renderMap(rendered);
        } else {
            throw new IllegalArgumentException("Invalid parameters type '" + parameters.getClass() + "'");
        }
    }
}
