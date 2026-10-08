package io.kestra.plugin.databricks.dbfs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.mixin.DbfsExt;
import com.databricks.sdk.service.files.Delete;
import com.databricks.sdk.service.files.FileInfo;
import com.databricks.sdk.service.files.Move;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Validator;

@KestraTest
class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private Validator validator;

    @Test
    void usesDefaultStateKeyWhenUnset() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.stateKey = null;
        trigger.regExp = null;

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void detectsNewFilesAndDoesNotRepeatThem() throws Exception {
        var trigger = new MockTrigger(List.of(
            file("/mnt/incoming/a.csv", 10L, 100L),
            file("/mnt/incoming/partition=2026-10-03/b.csv", 20L, 200L)
        ));

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));
        assertThat(triggeredPaths(first.get()), contains(
            "/mnt/incoming/a.csv",
            "/mnt/incoming/partition=2026-10-03/b.csv"
        ));

        var second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(false));
    }

    @Test
    void detectsUpdatesWhenConfigured() throws Exception {
        var trigger = new MockTrigger(
            List.of(file("/mnt/incoming/a.csv", 10L, 100L)),
            file("/mnt/incoming/a.csv", 15L, 200L)
        );
        trigger.on = Property.ofValue(StatefulTriggerInterface.On.CREATE_OR_UPDATE);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
        var update = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(update.isPresent(), is(true));
        assertThat(triggeredPaths(update.get()), contains("/mnt/incoming/a.csv"));
    }

    @Test
    void updateOnlyIgnoresInitialFilesAndFiresOnUpdate() throws Exception {
        var trigger = new MockTrigger(
            List.of(file("/mnt/incoming/a.csv", 10L, 100L)),
            file("/mnt/incoming/a.csv", 15L, 200L)
        );
        trigger.on = Property.ofValue(StatefulTriggerInterface.On.UPDATE);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
        var update = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(update.isPresent(), is(true));
        assertThat(triggeredPaths(update.get()), contains("/mnt/incoming/a.csv"));
    }

    @Test
    void rejectsRelativeDbfsPaths() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.from = Property.ofValue("mnt/incoming");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThrows(IllegalArgumentException.class, () -> trigger.evaluate(context.getKey(), context.getValue()));
    }

    @Test
    void createIgnoresUpdates() throws Exception {
        var trigger = new MockTrigger(
            List.of(file("/mnt/incoming/a.csv", 10L, 100L)),
            file("/mnt/incoming/a.csv", 15L, 200L)
        );

        trigger.on = Property.ofValue(StatefulTriggerInterface.On.CREATE);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void recreatedFileIsDetectedAgain() throws Exception {
        var trigger = new MockTrigger(
            List.of(file("/mnt/incoming/a.csv", 10L, 100L)),
            List.of()
        );

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));

        trigger.current = List.of(file("/mnt/incoming/a.csv", 10L, 100L));
        var recreated = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(recreated.isPresent(), is(true));
    }

    @Test
    void maxFilesLimitsOneExecutionAndLeavesRemainingFilesForNextPoll() throws Exception {
        var trigger = new MockTrigger(List.of(
            file("/mnt/incoming/a.csv", 10L, 100L),
            file("/mnt/incoming/b.csv", 20L, 200L),
            file("/mnt/incoming/c.csv", 30L, 300L)
        ));
        trigger.maxFiles = Property.ofValue(2);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(triggeredPaths(first.get()), contains("/mnt/incoming/a.csv", "/mnt/incoming/b.csv"));

        var second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(triggeredPaths(second.get()), contains("/mnt/incoming/c.csv"));
    }

    @Test
    void performsConfiguredActionAfterDetection() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);
        trigger.moveDirectory = Property.ofValue("/mnt/archive");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(trigger.performedAction, is(ActionInterface.Action.MOVE));
        assertThat(trigger.actedPaths, contains("/mnt/incoming/a.csv"));
    }

    @Test
    void failedActionDoesNotAbortBatch() throws Exception {
        var trigger = new MockTrigger(List.of(
            file("/mnt/incoming/a.csv", 10L, 100L),
            file("/mnt/incoming/b.csv", 20L, 200L)
        ));
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);
        trigger.moveDirectory = Property.ofValue("/mnt/archive");
        trigger.failingActionPath = "/mnt/incoming/a.csv";

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(triggeredPaths(execution.get()), contains(
            "/mnt/incoming/a.csv",
            "/mnt/incoming/b.csv"
        ));
        assertThat(trigger.actedPaths, contains(
            "/mnt/incoming/a.csv",
            "/mnt/incoming/b.csv"
        ));
    }

    @Test
    void moveActionRequiresMoveDirectory() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThrows(
            ConstraintViolationException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );
    }

    @Test
    void moveWithoutDirectoryFailsValidation() {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);

        var violations = validator.validate(trigger);

        assertThat(
            violations.stream()
                .filter(v -> v.getMessage().contains("moveDirectory is required when action is MOVE"))
                .toList(),
            hasSize(1)
        );
    }

    @Test
    void templatedActionPassesMoveDirectoryValidation() {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofExpression("{{ inputs.action }}");

        var violations = validator.validate(trigger);

        assertThat(
            violations.stream()
                .filter(v -> v.getMessage().contains("moveDirectory is required when action is MOVE"))
                .toList(),
            hasSize(0)
        );
    }

    @Test
    void templatedMaxFilesRejectsValuesOutsideDeclaredBounds() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.maxFiles = Property.ofExpression("{{ 0 }}");
        assertThrows(
            ConstraintViolationException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );

        trigger.maxFiles = Property.ofExpression("{{ 1001 }}");
        assertThrows(
            ConstraintViolationException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );
    }

    @Test
    void invalidRegexProducesHelpfulError() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.regExp = Property.ofValue("[");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );
        assertThat(exception.getMessage(), is("Invalid `regExp`: ["));
    }

    @Test
    void maxFilesHonorsDeclaredBounds() {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));

        for (var value : List.of(0, 1001)) {
            trigger.maxFiles = Property.ofValue(value);

            var violations = validator.validate(trigger);

            assertThat(
                violations.stream()
                    .anyMatch(v -> v.getPropertyPath().toString().equals("maxFiles")),
                is(true)
            );
        }
    }

    @Test
    void recursiveMoveCannotTargetWatchedDirectory() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.recursive = Property.ofValue(true);
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);
        trigger.moveDirectory = Property.ofValue("/mnt/incoming/archive");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var exception = assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );
        assertThat(
            exception.getMessage(),
            is("moveDirectory must be outside the watched `from` path when recursive is enabled: /mnt/incoming/archive")
        );
    }

    @Test
    void moveActionInvokesDbfsMoveAndPreservesRelativePath() throws Exception {
        assertMoveDestination("/mnt/archive", "/mnt/archive/partition=2026-10-05/a.csv");
        assertMoveDestination("/mnt/archive/", "/mnt/archive/partition=2026-10-05/a.csv");
    }

    @Test
    void deleteActionInvokesDbfsDelete() throws Exception {
        var dbfs = mock(DbfsExt.class);
        var client = mock(WorkspaceClient.class);
        when(client.dbfs()).thenReturn(dbfs);

        var trigger = Trigger.builder()
            .id("dbfs-trigger-test-" + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/incoming"))
            .stateKey(Property.ofValue("dbfs-trigger-state-" + IdUtils.create()))
            .action(Property.ofValue(ActionInterface.Action.DELETE))
            .build();

        var spied = spy(trigger);
        doReturn(client).when(spied).workspaceClient(any());
        doReturn(List.of(file("/mnt/incoming/a.csv", 10L, 100L)))
            .when(spied)
            .listFiles(any(WorkspaceClient.class), anyString(), anyBoolean());

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = spied.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        var request = ArgumentCaptor.forClass(Delete.class);
        verify(dbfs).delete(request.capture());
        assertThat(request.getValue().getPath(), is("/mnt/incoming/a.csv"));
    }

    private void assertMoveDestination(String moveDirectory, String expectedDestination) throws Exception {
        var dbfs = mock(DbfsExt.class);
        var client = mock(WorkspaceClient.class);
        when(client.dbfs()).thenReturn(dbfs);

        var trigger = Trigger.builder()
            .id("dbfs-trigger-test-" + IdUtils.create())
            .type(Trigger.class.getName())
            .from(Property.ofValue("/mnt/incoming"))
            .stateKey(Property.ofValue("dbfs-trigger-state-" + IdUtils.create()))
            .recursive(Property.ofValue(true))
            .action(Property.ofValue(ActionInterface.Action.MOVE))
            .moveDirectory(Property.ofValue(moveDirectory))
            .build();

        var spied = spy(trigger);
        doReturn(client).when(spied).workspaceClient(any());
        doReturn(List.of(file("/mnt/incoming/partition=2026-10-05/a.csv", 10L, 100L)))
            .when(spied)
            .listFiles(any(WorkspaceClient.class), anyString(), anyBoolean());

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = spied.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));

        var request = ArgumentCaptor.forClass(Move.class);
        verify(dbfs).move(request.capture());
        assertThat(request.getValue().getSourcePath(), is("/mnt/incoming/partition=2026-10-05/a.csv"));
        assertThat(request.getValue().getDestinationPath(), is(expectedDestination));
    }

    @Test
    void relativeMoveDirectoryIsRejected() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);
        trigger.moveDirectory = Property.ofValue("archive");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThrows(
            IllegalArgumentException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );
    }

    @Test
    void blankMoveDirectoryIsRejected() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofValue(ActionInterface.Action.MOVE);
        trigger.moveDirectory = Property.ofValue("   ");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThrows(
            ConstraintViolationException.class,
            () -> trigger.evaluate(context.getKey(), context.getValue())
        );
    }

    @Test
    void deleteActionDoesNotRequireMoveDirectory() throws Exception {
        var trigger = new MockTrigger(List.of(file("/mnt/incoming/a.csv", 10L, 100L)));
        trigger.action = Property.ofValue(ActionInterface.Action.DELETE);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(trigger.performedAction, is(ActionInterface.Action.DELETE));
        assertThat(trigger.actedPaths, contains("/mnt/incoming/a.csv"));
    }

    @Test
    void nullFileMetadataDoesNotBreakDetection() throws Exception {
        var trigger = new MockTrigger(List.of(
            new FileInfo()
                .setPath("/mnt/incoming/a.csv")
                .setIsDir(false)
        ));

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        var first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));

        var second = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(second.isPresent(), is(false));
    }

    @Test
    void nonRecursiveListingDoesNotDescendIntoDirectories() throws Exception {
        var trigger = new MockTrigger(Map.of(
            "/mnt/incoming", List.of(
                new FileInfo()
                    .setPath("/mnt/incoming/partition=2026-10-05")
                    .setIsDir(true),
                file("/mnt/incoming/root.csv", 10L, 100L)
            ),
            "/mnt/incoming/partition=2026-10-05", List.of(
                file("/mnt/incoming/partition=2026-10-05/nested.csv", 20L, 200L)
            )
        ));
        trigger.recursive = Property.ofValue(false);

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(
            triggeredPaths(execution.get()),
            contains("/mnt/incoming/root.csv")
        );
        assertThat(
            trigger.listedPaths,
            contains("/mnt/incoming")
        );
    }

    @Test
    void emptyDirectoryDoesNotTrigger() throws Exception {
        var trigger = new MockTrigger(List.of());
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void regexFilteringDoesNotPruneStateForExistingFiles() throws Exception {
        var trigger = new MockTrigger(List.of(
            file("/mnt/incoming/a.csv", 10L, 100L),
            file("/mnt/incoming/b.csv", 20L, 200L)
        ));
        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        trigger.regExp = Property.ofValue(".*/a\\.csv");
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));

        trigger.regExp = Property.ofValue(".*/b\\.csv");
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(true));

        trigger.regExp = Property.ofValue(".*/a\\.csv");
        assertThat(trigger.evaluate(context.getKey(), context.getValue()).isPresent(), is(false));
    }

    @Test
    void filtersDirectoriesAndSupportsRecursiveListingAndRegex() throws Exception {
        var trigger = new MockTrigger(Map.of(
            "/mnt/incoming", List.of(
                new FileInfo().setPath("/mnt/incoming/partition=2026-10-03").setIsDir(true),
                file("/mnt/incoming/a.csv", 10L, 100L)
            ),
            "/mnt/incoming/partition=2026-10-03", List.of(
                file("/mnt/incoming/partition=2026-10-03/b.csv", 20L, 200L)
            )
        ));
        trigger.recursive = Property.ofValue(true);
        trigger.regExp = Property.ofValue(".*/b\\.csv");

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);
        var execution = trigger.evaluate(context.getKey(), context.getValue());

        assertThat(execution.isPresent(), is(true));
        assertThat(triggeredPaths(execution.get()), contains("/mnt/incoming/partition=2026-10-03/b.csv"));
        assertThat(trigger.listedPaths, contains("/mnt/incoming", "/mnt/incoming/partition=2026-10-03"));
    }

    private static FileInfo file(String path, long size, long modifiedAt) {
        return new FileInfo()
            .setPath(path)
            .setFileSize(size)
            .setModificationTime(modifiedAt)
            .setIsDir(false);
    }

    @SuppressWarnings("unchecked")
    private static List<String> triggeredPaths(Execution execution) {
        return ((List<Map<String, Object>>) execution.getTrigger().getVariables().get("files")).stream()
            .map(file -> (String) file.get("path"))
            .toList();
    }

    private static class MockTrigger extends Trigger {
        private List<FileInfo> current;
        private List<FileInfo> next;
        private Map<String, List<FileInfo>> directories;
        private List<String> listedPaths = new ArrayList<>();
        private ActionInterface.Action performedAction;
        private List<String> actedPaths = new ArrayList<>();
        private String failingActionPath;

        MockTrigger(List<FileInfo> current) {
            this.current = current;
            this.id = "dbfs-trigger-test-" + IdUtils.create();
            this.type = Trigger.class.getName();
            this.from = Property.ofValue("/mnt/incoming");
            this.stateKey = Property.ofValue("dbfs-trigger-state-" + IdUtils.create());
        }

        MockTrigger(List<FileInfo> current, FileInfo next) {
            this(current);
            this.next = List.of(next);
        }

        MockTrigger(List<FileInfo> current, List<FileInfo> next) {
            this(current);
            this.next = next;
        }

        MockTrigger(Map<String, List<FileInfo>> directories) {
            this(directories.getOrDefault("/mnt/incoming", List.of()));
            this.directories = new HashMap<>(directories);
        }

        @Override
        public WorkspaceClient workspaceClient(RunContext runContext) {
            return null;
        }

        @Override
        protected void performSingleAction(
            WorkspaceClient workspaceClient,
            TriggeredFile triggeredFile,
            ActionInterface.Action action,
            String moveDirectory,
            String from
        ) {
            var filePath = triggeredFile.getFile().getPath();
            performedAction = action;
            actedPaths.add(filePath);

            if (filePath.equals(failingActionPath)) {
                throw new IllegalStateException("simulated action failure");
            }
        }

        @Override
        protected Iterable<FileInfo> listDirectory(WorkspaceClient workspaceClient, String path) {
            listedPaths.add(path);
            if (directories != null) {
                return directories.getOrDefault(path, List.of());
            }
            return current;
        }

        @Override
        protected List<FileInfo> listFiles(WorkspaceClient workspaceClient, String path, boolean recursive) {
            if (directories != null) {
                return super.listFiles(workspaceClient, path, recursive);
            }
            if (next != null) {
                var result = current;
                current = next;
                next = null;
                return result;
            }
            return current;
        }
    }
}
