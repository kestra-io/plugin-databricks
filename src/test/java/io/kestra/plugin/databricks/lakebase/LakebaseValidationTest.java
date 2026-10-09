package io.kestra.plugin.databricks.lakebase;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.validations.ModelValidator;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@KestraTest
class LakebaseValidationTest {
    @Inject
    private ModelValidator modelValidator;

    @Test
    void queryRequiresConnectionAndSql() {
        var task = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .build();

        var validation = modelValidator.isValid(task);
        assertThat(validation.isPresent(), is(true));
        assertThat(validation.get().getMessage(), containsString("workspaceHost"));
    }

    @Test
    void queryWithRequiredFieldsPassesValidation() {
        var task = Query.builder()
            .id(IdUtils.create())
            .type(Query.class.getName())
            .workspaceHost(Property.ofValue("https://example.databricks.com"))
            .clientId(Property.ofValue("client"))
            .clientSecret(Property.ofValue("secret"))
            .endpoint(Property.ofValue("projects/p/branches/b/endpoints/e"))
            .database(Property.ofValue("orders_db"))
            .sql(Property.ofValue("SELECT 1"))
            .build();

        assertThat(modelValidator.isValid(task).isPresent(), is(false));
    }

    @Test
    void clientSecretIsMarkedSecret() throws NoSuchFieldException {
        var annotation = AbstractLakebaseTask.class
            .getDeclaredField("clientSecret")
            .getAnnotation(io.kestra.core.models.annotations.PluginProperty.class);

        assertThat(annotation.secret(), is(true));
    }
}
