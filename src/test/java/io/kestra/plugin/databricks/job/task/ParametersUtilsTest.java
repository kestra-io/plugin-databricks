package io.kestra.plugin.databricks.job.task;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

@MicronautTest
class ParametersUtilsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void listParameters() throws IllegalVariableEvaluationException {
        var runContext = runContextFactory.of(Map.of(
            "var1", "value1",
            "var2", """
                ["element1", "element2"]"""));

        var listParameters = ParametersUtils.listParameters(runContext, List.of("param1", "param2"));
        assertThat(listParameters, containsInAnyOrder("param1", "param2"));

        listParameters = ParametersUtils.listParameters(runContext, List.of("param1", "{{var1}}"));
        assertThat(listParameters, containsInAnyOrder("param1", "value1"));

        listParameters = ParametersUtils.listParameters(runContext, "{{var2}}");
        assertThat(listParameters, containsInAnyOrder("element1", "element2"));
    }

    @Test
    void mapParameters() throws IllegalVariableEvaluationException {
        var runContext = runContextFactory.of(Map.of(
            "var1", "value1",
            "var2", """
                {
                    "key1": "value1",
                    "key2": "value2"
                }"""));

        var mapParameters = ParametersUtils.mapParameters(runContext, Map.of("key1", "value1", "key2", "value2"));
        assertThat(mapParameters.get("key1"), is("value1"));
        assertThat(mapParameters.get("key2"), is("value2"));

        mapParameters = ParametersUtils.mapParameters(runContext, Map.of("key1", "value1", "key2", "{{var1}}"));
        assertThat(mapParameters.get("key1"), is("value1"));
        assertThat(mapParameters.get("key2"), is("value1"));

        mapParameters = ParametersUtils.mapParameters(runContext, "{{var2}}");
        assertThat(mapParameters.get("key1"), is("value1"));
        assertThat(mapParameters.get("key2"), is("value2"));
    }

}