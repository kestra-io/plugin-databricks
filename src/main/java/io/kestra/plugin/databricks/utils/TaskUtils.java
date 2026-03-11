package io.kestra.plugin.databricks.utils;

import java.util.List;

import com.databricks.sdk.service.jobs.TaskDependency;

public final class TaskUtils {
    private TaskUtils() {
        //utility class pattern
    }

    public static List<TaskDependency> dependsOn(List<String> dependsOn) {
        return dependsOn == null ? null
            : dependsOn.stream()
                .map(taskKey -> new TaskDependency().setTaskKey(taskKey))
                .toList();
    }
}
