package io.kestra.plugin.databricks.dbfs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.files.Delete;
import com.databricks.sdk.service.files.FileInfo;
import com.databricks.sdk.service.files.Move;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
import io.kestra.core.utils.PebbleUtil;
import io.kestra.plugin.databricks.AbstractTask;
import io.kestra.plugin.databricks.DatabricksConnectionInterface;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
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
        Directories are ignored as trigger events. The trigger persists file state in Kestra's namespace KV store
        to avoid duplicate events. The first poll reports existing matching files as CREATE events; the default
        `on` mode is CREATE_OR_UPDATE. Recursive traversal uses explicit visited-directory tracking, while listings
        are materialized during each poll; keep recursively watched trees to a few thousand files to avoid excessive memory usage.
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
        description = """
            Absolute DBFS directory path to watch, such as `/mnt/incoming`.
            The rendered value must be an absolute DBFS path.
            """
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
        description = """
            When enabled, files below nested DBFS directories are monitored recursively.
            Because the listing is materialized on each poll, keep recursively watched trees to a few thousand files.
            When `action: MOVE` is used with recursive polling, `moveDirectory` must be outside `from`.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<Boolean> recursive = Property.ofValue(false);

    @Schema(
        title = "Regex pattern to match DBFS paths",
        description = """
            Optional regular expression matched against the complete DBFS path of each file.
            Invalid expressions fail validation at runtime with an error naming this property.
            """
    )
    @PluginProperty(group = "advanced")
    protected Property<String> regExp;

    @Schema(
        title = "Trigger condition",
        description = """
            Which file changes fire the trigger. Defaults to CREATE_OR_UPDATE.
            """
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    protected Property<On> on = Property.ofValue(On.CREATE_OR_UPDATE);

    @Schema(
        title = "State key",
        description = """
            Key used to persist the trigger state. Defaults to a stable per-trigger key.
            """
    )
    @PluginProperty(group = "advanced")
    protected Property<String> stateKey;

    @Schema(
        title = "State TTL",
        description = """
            How long the persisted trigger state is retained.
            Unset means no expiry.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<Duration> stateTtl;


    @Schema(
        title = "Maximum files per execution",
        description = """
            Maximum number of detected files emitted by a single poll.
            Must be between 1 and 1000. The default is 25. Remaining files are evaluated on the next poll.
            """
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    protected Property<@Min(1) @Max(1000) Integer> maxFiles = Property.ofValue(25);


    @Schema(
        title = "Post-detection action",
        description = """
            NONE leaves detected files in place. MOVE relocates each detected file below `moveDirectory`,
            preserving its relative path under `from`. DELETE removes detected files after state is persisted.
            Failed actions are logged and do not cancel the trigger execution.
            """
    )
    @Builder.Default
    @PluginProperty(group = "main")
    protected Property<ActionInterface.Action> action = Property.ofValue(ActionInterface.Action.NONE);


    @Schema(
        title = "Move destination",
        description = """
            Target DBFS directory when action is MOVE.
            For recursive polling, the relative path below `from` is preserved under this directory.
            The destination must not be inside the watched `from` path when recursive polling is enabled.
            """
    )
    @PluginProperty(group = "advanced")
    protected Property<String> moveDirectory;

    @AssertTrue(message = "moveDirectory is required when action is MOVE")
    @JsonIgnore
    public boolean isMoveDirectorySetForMove() {
        if (action == null) {
            return true;
        }

        var expr = action.toString();

        if (PebbleUtil.containsOpeningBlockDelimiter(expr)) {
            return true;
        }

        if (!ActionInterface.Action.MOVE.name().equalsIgnoreCase(expr)) {
            return true;
        }

        return moveDirectory != null
            && (
                PebbleUtil.containsOpeningBlockDelimiter(moveDirectory.toString())
                    || !moveDirectory.toString().isBlank()
            );
    }

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        var runContext = conditionContext.getRunContext();
        var rPath = runContext.render(from).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("`from` is required: set it to an absolute DBFS directory such as /mnt/incoming"));
        if (rPath.isBlank() || !rPath.startsWith("/")) {
            throw new IllegalArgumentException("DBFS path must be absolute and start with '/': " + rPath);
        }

        var rRecursive = runContext.render(recursive).as(Boolean.class).orElse(false);
        var rOn = runContext.render(on).as(On.class).orElse(On.CREATE_OR_UPDATE);
        var rStateKey = runContext.render(stateKey).as(String.class)
            .orElseGet(() -> StatefulTriggerService.defaultKey(context.getNamespace(), context.getFlowId(), id));
        var rStateTtl = runContext.render(stateTtl).as(Duration.class);
        var rMaxFiles = runContext.render(maxFiles).as(Integer.class).orElse(25);
        if (rMaxFiles < 1 || rMaxFiles > 1000) {
            throw new IllegalArgumentException("maxFiles must be between 1 and 1000");
        }
        var rAction = runContext.render(action).as(ActionInterface.Action.class).orElse(ActionInterface.Action.NONE);
        var rMoveDirectory = runContext.render(moveDirectory).as(String.class).orElse(null);
        if (rAction == ActionInterface.Action.MOVE && (rMoveDirectory == null || rMoveDirectory.isBlank())) {
            throw new IllegalArgumentException("moveDirectory is required when action is MOVE");
        }
        if (rAction == ActionInterface.Action.MOVE && !rMoveDirectory.startsWith("/")) {
            throw new IllegalArgumentException("moveDirectory must be an absolute DBFS path: " + rMoveDirectory);
        }
        if (rAction == ActionInterface.Action.MOVE && rRecursive && isSameOrDescendantPath(rPath, rMoveDirectory)) {
            throw new IllegalArgumentException("moveDirectory must be outside the watched `from` path when recursive is enabled: " + rMoveDirectory);
        }

        var rRegExp = runContext.render(regExp).as(String.class).orElse(null);
        var rRegExpPattern = Optional.ofNullable(rRegExp)
            .map(value -> {
                try {
                    return Pattern.compile(value);
                } catch (PatternSyntaxException e) {
                    throw new IllegalArgumentException("Invalid `regExp`: " + value, e);
                }
            })
            .orElse(null);

        var workspaceClient = workspaceClient(runContext);
        var listedFiles = listFiles(workspaceClient, rPath, rRecursive).stream()
            .filter(file -> !Boolean.TRUE.equals(file.getIsDir()))
            .filter(file -> file.getPath() != null)
            .sorted(Comparator.comparing(FileInfo::getPath))
            .toList();

        var state = StatefulTriggerService.readState(runContext, rStateKey, rStateTtl);

        var seen = listedFiles.stream()
            .map(FileInfo::getPath)
            .collect(Collectors.toSet());

        var files = listedFiles.stream()
            .filter(file -> rRegExpPattern == null || rRegExpPattern.matcher(file.getPath()).matches())
            .toList();

        var detected = new ArrayList<TriggeredFile>();

        for (var file : files) {
            if (detected.size() >= rMaxFiles) {
                runContext.logger().warn(
                    "Reached maxFiles ({}), remaining DBFS files under '{}' will be evaluated on the next poll",
                    rMaxFiles,
                    rPath
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

        state.keySet().retainAll(seen);
        StatefulTriggerService.writeState(runContext, rStateKey, state, rStateTtl);

        if (detected.isEmpty()) {
            return Optional.empty();
        }

        performAction(workspaceClient, detected, rAction, rMoveDirectory, rPath, runContext);

        runContext.logger().info(
            "Detected {} DBFS file(s) under '{}'",
            detected.size(),
            rPath
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

    /** Recursive traversal is needed because the DBFS SDK list() operation is not recursive. */
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
        String rMoveDirectory,
        String rFrom,
        RunContext runContext
    ) {
        if (rAction == ActionInterface.Action.NONE) {
            return;
        }

        for (var triggeredFile : files) {
            var filePath = triggeredFile.getFile().getPath();

            try {
                performSingleAction(workspaceClient, triggeredFile, rAction, rMoveDirectory, rFrom);
            } catch (Exception e) {
                runContext.logger().warn(
                    "Failed to {} DBFS file '{}': {}",
                    rAction,
                    filePath,
                    e.getMessage(),
                    e
                );
            }
        }
    }

    protected void performSingleAction(
        WorkspaceClient workspaceClient,
        TriggeredFile triggeredFile,
        ActionInterface.Action rAction,
        String rMoveDirectory,
        String rFrom
    ) {
        var filePath = triggeredFile.getFile().getPath();

        switch (rAction) {
            case MOVE -> workspaceClient.dbfs().move(
                new Move()
                    .setSourcePath(filePath)
                    .setDestinationPath(moveDestination(rFrom, rMoveDirectory, filePath))
            );
            case DELETE -> workspaceClient.dbfs().delete(new Delete().setPath(filePath));
            case NONE -> { }
        }
    }

    private static String moveDestination(String rFrom, String rMoveDirectory, String filePath) {
        var normalizedFrom = normalizeDbfsDirectory(rFrom);
        var normalizedMoveDirectory = normalizeDbfsDirectory(rMoveDirectory);
        var normalizedFilePath = normalizeDbfsDirectory(filePath);

        var relativePath;
        if ("/".equals(normalizedFrom)) {
            relativePath = normalizedFilePath.substring(1);
        } else {
            var prefix = normalizedFrom + "/";
            if (!normalizedFilePath.startsWith(prefix)) {
                throw new IllegalArgumentException(
                    "Detected DBFS file is outside the watched `from` path: " + filePath
                );
            }
            relativePath = normalizedFilePath.substring(prefix.length());
        }

        if (relativePath.isBlank()) {
            throw new IllegalArgumentException("Detected DBFS file path is empty: " + filePath);
        }

        return "/".equals(normalizedMoveDirectory)
            ? "/" + relativePath
            : normalizedMoveDirectory + "/" + relativePath;
    }

    private static String normalizeDbfsDirectory(String path) {
        if (path.length() > 1 && path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    private static boolean isSameOrDescendantPath(String root, String candidate) {
        var normalizedRoot = normalizeDbfsDirectory(root);
        var normalizedCandidate = normalizeDbfsDirectory(candidate);

        return "/".equals(normalizedRoot)
            || normalizedCandidate.equals(normalizedRoot)
            || normalizedCandidate.startsWith(normalizedRoot + "/");
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
        @Schema(
            title = "DBFS file metadata",
            description = "Unwrapped DBFS metadata including path, fileSize, modificationTime, and isDir."
        )
        @JsonUnwrapped
        private final FileInfo file;

        @Schema(title = "Detected change type: CREATE for a newly observed file or UPDATE for changed metadata")
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
