package io.kestra.plugin.databricks.job.task;

import com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
class RunJobTaskSettingTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void missingJobId() {
        var setting = RunJobTaskSetting.builder().build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> setting.toRunJobTask(runContextFactory.of(ImmutableMap.of()))
        );
        assertThat(exception.getMessage(), containsString("`jobId`"));
    }

    @Test
    void nonNumericJobId() {
        var setting = RunJobTaskSetting.builder().jobId(Property.ofValue("not-a-number")).build();

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> setting.toRunJobTask(runContextFactory.of(ImmutableMap.of()))
        );
        assertThat(exception.getMessage(), containsString("not-a-number"));
    }
}
