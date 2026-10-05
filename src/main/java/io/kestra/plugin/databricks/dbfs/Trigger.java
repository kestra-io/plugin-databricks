package io.kestra.plugin.databricks.dbfs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.files.Delete;
import com.databricks.sdk.service.files.FileInfo;
import com.databricks.sdk.service.files.Move;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.models.triggers.StatefulTriggerService;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.DatabricksConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on new DBFS files",
    description = """
        Periodically lists a DBFS path and starts one execution for files detected since the previous poll.
        Directories are ignored as trigger events. DBFS listing can time out on very large directories;
        keep watched directories reasonably bounded. Set `recursive` to true to monitor files below
        partition directories.
        The trigger persists file state in Kestra's namespace KV store to avoid duplicate events.
        The first poll reports existing matching files as CREATE events; the default `on` mode is CREATE_OR_UPDATE.
        """
)
@Plugin(
    examples = {
        @Example(
            title = "Trigger a flow when a new file is added to a DBFS directory.",
            full = true,
            code = """
                id: databricks_dbfs_trigger_flow
                namespace: company.team

                tasks:
                  - id: each_file
                    type: io.kestra.plugin.core.flow.ForEach
                    values: "{{ trigger.files | jq('.[].path') }}"
                    tasks:
                      - id: download
                        type: io.kestra.plugin.databricks.dbfs.Download
                        host: "{{ secret('DATABRICKS_HOST') }}"
                        authentication:
                          token: "{{ secret('DATABRICKS_TOKEN') }}"
                        from: "{{ taskrun.value }}"

                triggers:
                  - id: watch_dbfs
                    type: io.kestra.plugin.databricks.dbfs.Trigger
                    host: "{{ secret('DATABRICKS_HOST') }}"
                    authentication:
                      token: "{{ secret('DATABRICKS_TOKEN') }}"
                    from: /mnt/incoming
                    interval: PT1M
                    recursive: true
                    on: CREATE
                """
        )
    }
)
public class Trigger extends AbstractTrigger
    implements PollingTriggerInterface, TriggerOutput<Trigger.Output>, StatefulTriggerInterface, DatabricksConnectionInterface, ActionInterface {

    @Schema(title = "Databricks host")
    @PluginProperty(group = "connection")
    private Property<String> host;

    @Schema(title = "Databricks account identifier")
    @PluginProperty(group = "advanced")
    private Property<String> accountId;

    @Schema(title = "Databricks configuration file")
    @PluginProperty(group = "advanced")
    private Property<String> configFile;

    @Schema(
        title = "Databricks authentication configuration",
        description = """
            Configure the Databricks authentication. Any property not explicitly configured can be
            supplied through the standard Databricks environment variables or configuration file.
            """
    )
    @PluginProperty(dynamic = true, group = "connection")
    private AbstractTask.AuthenticationConfig authentication;

    @Schema(
        title = "DBFS path to watch",
        description = "Absolute DBFS directory path to watch, such as `/mnt/incoming`."
    )
    @NotNull
    @PluginProperty(group = "main")
    protected Property<String> from;

    @Schema(title = "Interval between checks")
    @Builder.Default
    @PluginProperty(group = "execution")
    private final Duration interval = Duration.ofSeconds(60);

    @Schema(
        title = "Include files in subdirectories",
        description = "When enabled, files below nested DBFS directories are monitored recursively."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> recursive = Property.ofValue(false);

    @Schema(
        title = "Regex pattern to match DBFS paths",
        description = "Optional regular expression matched against the complete DBFS path of each file."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> regExp;

    @Schema(
        title = "Trigger condition",
        description = "Which file changes fire the trigger. Defaults to CREATE_OR_UPDATE."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(
        title = "State key",
        description = "Key used to persist the trigger state. Defaults to a stable per-trigger key."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = "How long the persisted trigger state is retained. Unset means no expiry."
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;


    @Schema(
        title = "Maximum files per execution",
        description = "Maximum number of detected files emitted by a single poll. Remaining files are evaluated on the next poll."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<Integer> maxFiles = Property.ofValue(25);


    @Schema(
        title = "Post-detection action",
        description = "NONE (default), MOVE to move detected files, or DELETE to remove them after state is persisted."
    )
    @Builder.Default
    protected Property<ActionInterface.Action> action = Property.ofValue(ActionInterface.Action.NONE);


    @Schema(
        title = "Move destination",
        description = "Target DBFS directory when action is MOVE."
    )
    @PluginProperty(group = "advanced")
    protected Property<String> moveDirectory;

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var path = runContext.render(from).as(String.class).orElseThrow();
        if (path.isBlank() || !path.startsWith("/")) {
            throw new IllegalArgumentException("DBFS path must be absolute and start with '/': " + path);
        }

        var recursiveFiles = runContext.render(recursive).as(Boolean.class).orElse(false);
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class)
            .orElseGet(() -> StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);
        var rMaxFiles = runContext.render(maxFiles).as(Integer.class).orElse(25);
        if (rMaxFiles < 1) {
            throw new IllegalArgumentException("maxFiles must be greater than 0");
        }
        var rAction = runContext.render(action).as(ActionInterface.Action.class).orElse(ActionInterface.Action.NONE);
        var rMoveDirectory = runContext.render(moveDirectory).as(String.class).orElse(null);
        if (rAction == ActionInterface.Action.MOVE && (rMoveDirectory == null || rMoveDirectory.isBlank())) {
            throw new IllegalArgumentException("moveDirectory is required when action is MOVE");
        }
        if (rAction == ActionInterface.Action.MOVE && !rMoveDirectory.startsWith("/")) {
            throw new IllegalArgumentException("moveDirectory must be an absolute DBFS path: " + rMoveDirectory);
        }
        var regexp = runContext.render(regExp).as(String.class).map(Pattern::compile).orElse(null);

        var workspaceClient = workspaceClient(runContext);
        var listedFiles = listFiles(workspaceClient, path, recursiveFiles).stream()
            .filter(file -> !Boolean.TRUE.equals(file.getIsDir()))
            .filter(file -> file.getPath() != null)
            .sorted(Comparator.comparing(FileInfo::getPath))
            .toList();

        Map<String, StatefulTriggerService.Entry> state =
            StatefulTriggerService.readState(runContext, rStateKey, rStateTtl);

        Set<String> seen = listedFiles.stream()
            .map(FileInfo::getPath)
            .collect(Collectors.toSet());

        var files = listedFiles.stream()
            .filter(file -> regexp == null || regexp.matcher(file.getPath()).matches())
            .toList();

        var detected = new ArrayList<TriggeredFile>();

        for (var file : files) {
            if (detected.size() >= rMaxFiles) {
                runContext.logger().warn(
                    "Reached maxFiles ({}), remaining DBFS files under '{}' will be evaluated on the next poll",
                    rMaxFiles,
                    path
                );
                break;
            }

            var pathKey = file.getPath();
            seen.add(pathKey);

            var modifiedAt = file.getModificationTime() == null
                ? null
                : Instant.ofEpochMilli(file.getModificationTime());

            var version = String.format(
                "%s:%s",
                Optional.ofNullable(file.getModificationTime()).map(String::valueOf).orElse(""),
                Optional.ofNullable(file.getFileSize()).map(String::valueOf).orElse("")
            );

            var candidate = StatefulTriggerService.Entry.candidate(pathKey, version, modifiedAt);
            var change = StatefulTriggerService.computeAndUpdateState(state, candidate, rOn);

            if (change.fire()) {
                detected.add(
                    TriggeredFile.builder()
                        .file(file)
                        .changeType(change.isNew() ? ChangeType.CREATE : ChangeType.UPDATE)
                        .build()
                );
            }
        }

        // Forget files that disappeared so a file recreated at the same path is detected again.
        state.keySet().retainAll(seen);
        StatefulTriggerService.writeState(runContext, rStateKey, state, rStateTtl);

        if (detected.isEmpty()) {
            return Optional.empty();
        }

        performAction(workspaceClient, detected, rAction, rMoveDirectory);

        runContext.logger().info(
            "Detected {} DBFS file(s) under '{}'",
            detected.size(),
            path
        );

        return Optional.of(
            TriggerService.generateExecution(
                this,
                conditionContext,
                context,
                Output.builder()
                    .files(detected)
                    .size(detected.size())
                    .build()
            )
        );
    }

    /**
     * Lists DBFS entries using the Databricks SDK. The SDK handles pagination for each directory listing.
     * Recursive traversal is implemented here because the DBFS SDK exposes pagination for list(), not
     * a recursive-list operation.
     */
    protected List<FileInfo> listFiles(WorkspaceClient workspaceClient, String path, boolean recursive) {
        var result = new ArrayList<FileInfo>();
        var directories = new ArrayDeque<String>();
        var visitedDirectories = new HashSet<String>();
        directories.add(path);

        while (!directories.isEmpty()) {
            var currentPath = directories.removeFirst();
            if (!visitedDirectories.add(currentPath)) {
                continue;
            }

            for (var file : listDirectory(workspaceClient, currentPath)) {
                result.add(file);

                if (recursive && Boolean.TRUE.equals(file.getIsDir()) && file.getPath() != null) {
                    directories.addLast(file.getPath());
                }
            }

            if (!recursive) {
                break;
            }
        }

        return result;
    }

    protected void performAction(
        WorkspaceClient workspaceClient,
        List<TriggeredFile> files,
        ActionInterface.Action rAction,
        String rMoveDirectory
    ) {
        if (rAction == ActionInterface.Action.NONE) {
            return;
        }

        for (var triggeredFile : files) {
            var filePath = triggeredFile.getFile().getPath();
            switch (rAction) {
                case MOVE -> workspaceClient.dbfs().move(
                    new Move()
                        .setSourcePath(filePath)
                        .setDestinationPath(
                            rMoveDirectory.endsWith("/")
                                ? rMoveDirectory + filePath.substring(filePath.lastIndexOf('/') + 1)
                                : rMoveDirectory + "/" + filePath.substring(filePath.lastIndexOf('/') + 1)
                        )
                );
                case DELETE -> workspaceClient.dbfs().delete(new Delete().setPath(filePath));
                case NONE -> { }
            }
        }
    }

    protected Iterable<FileInfo> listDirectory(WorkspaceClient workspaceClient, String path) {
        return workspaceClient.dbfs().list(path);
    }

    public enum ChangeType {
        CREATE,
        UPDATE
    }

    @Getter
    @AllArgsConstructor
    @Builder
    public static class TriggeredFile {
        @JsonUnwrapped
        private final FileInfo file;
        private final ChangeType changeType;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "DBFS files detected since the previous poll, with their change type")
        private final List<TriggeredFile> files;

        @Schema(title = "Number of detected DBFS files")
        private final Integer size;
    }
}
