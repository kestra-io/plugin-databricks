package io.kestra.plugin.databricks.utils;

import com.databricks.sdk.service.jobs.TaskDependenciesItem;

import java.util.List;

public final class TaskUtils {
    private TaskUtils() {
        //utility class pattern
    }

    public static List<TaskDependenciesItem> dependsOn(List<String> dependsOn) {
        return dependsOn == null ? null : dependsOn.stream()
            .map(taskKey -> new TaskDependenciesItem().setTaskKey(taskKey))
            .toList();
    }
}
