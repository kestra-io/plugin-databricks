package io.kestra.plugin.databricks.job;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.service.jobs.Source;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.utils.IdUtils;
import io.kestra.plugin.databricks.job.task.SparkPythonTaskSetting;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@KestraTest
class CreateJobValidationTest {

    @Inject
    private ModelValidator modelValidator;

    @Test
    void missingJobNameIsRejectedAtValidation() {
        var task = createJob().build();

        var validation = modelValidator.isValid(task);
        assertThat(validation.isPresent(), is(true));
        assertThat(validation.get().getMessage(), containsString("jobName"));
    }

    @Test
    void jobNameIsEnoughToPassValidation() {
        var task = createJob().jobName(Property.ofValue("myJob")).build();

        assertThat(modelValidator.isValid(task).isPresent(), is(false));
    }

    private CreateJob.CreateJobBuilder<?, ?> createJob() {
        return CreateJob.builder()
            .id(IdUtils.create())
            .type(CreateJob.class.getName())
            .jobTasks(
                List.of(
                    CreateJob.JobTaskSetting.builder()
                        .existingClusterId(Property.ofValue("clusterId"))
                        .taskKey(Property.ofValue("taskKey"))
                        .sparkPythonTask(
                            SparkPythonTaskSetting.builder()
                                .sparkPythonTaskSource(Property.ofValue(Source.WORKSPACE))
                                .pythonFile(Property.ofValue("/Shared/hello.py"))
                                .build()
                        )
                        .build()
                )
            );
    }
}
