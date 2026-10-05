package io.kestra.plugin.databricks.dbfs;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface ActionInterface {
    @Schema(
        title = "Post-processing action",
        description = "MOVE, DELETE, or NONE (caller handles cleanup to avoid retriggers)"
    )
    @NotNull
    @PluginProperty(group = "main")
    Property<Action> getAction();

    @Schema(
        title = "Move destination",
        description = "Required when action is MOVE; absolute DBFS path"
    )
    @PluginProperty(group = "advanced")
    Property<String> getMoveDirectory();

    enum Action {
        MOVE,
        DELETE,
        NONE
    }
}
