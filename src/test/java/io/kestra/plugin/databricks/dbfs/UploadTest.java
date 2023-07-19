package io.kestra.plugin.databricks.dbfs;

import com.google.common.collect.ImmutableMap;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

@MicronautTest
@Disabled("Need an account to work")
class UploadTest {
    private static final String TOKEN = "";
    private static final String HOST = "";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    void run() throws Exception {
        URI source = storageInterface.put(
            new URI("/" + IdUtils.create()),
            new FileInputStream(new File(Objects.requireNonNull(UploadTest.class.getClassLoader()
                    .getResource("test.txt"))
                .toURI()))
        );

        var task = Upload.builder()
            .id(IdUtils.create())
            .type(Upload.class.getName())
            .authentication(
                AbstractTask.AuthenticationConfig.builder().token(TOKEN).build()
            )
            .host(HOST)
            .from(source.toString())
            .to("/Share/test.txt")
            .build();

        var runContext = TestsUtils.mockRunContext(runContextFactory, task, ImmutableMap.of());
        var output = task.run(runContext);
        assertThat(output, nullValue());
    }
}