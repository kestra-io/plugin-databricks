package io.kestra.plugin.databricks.dbfs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.files.FileInfo;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.StatefulTriggerInterface;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

class TriggerTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void detectsNewFilesAndDoesNotRepeatThem() throws Exception {
        var trigger = new MockTrigger(List.of(
            file("/mnt/incoming/a.csv", 10L, 100L),
            file("/mnt/incoming/partition=2026-10-03/b.csv", 20L, 200L)
        ));

        var context = TestsUtils.mockTrigger(runContextFactory, trigger);

        Optional<Execution> first = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(first.isPresent(), is(true));
        assertThat(triggeredPaths(first.get()), contains(
            "/mnt/incoming/a.csv",
            "/mnt/incoming/partition=2026-10-03/b.csv"
        ));

        Optional<Execution> second = trigger.evaluate(context.getKey(), context.getValue());
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
        Optional<Execution> update = trigger.evaluate(context.getKey(), context.getValue());
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
        Optional<Execution> update = trigger.evaluate(context.getKey(), context.getValue());
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
        Optional<Execution> recreated = trigger.evaluate(context.getKey(), context.getValue());
        assertThat(recreated.isPresent(), is(true));
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
        Optional<Execution> execution = trigger.evaluate(context.getKey(), context.getValue());

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
        private List<String> listedPaths = new java.util.ArrayList<>();

        MockTrigger(List<FileInfo> current) {
            this.current = current;
            this.from = Property.ofValue("/mnt/incoming");
            this.stateKey = Property.ofValue("dbfs-trigger-test-" + IdUtils.create());
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
        protected WorkspaceClient workspaceClient(RunContext runContext) {
            return null;
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
