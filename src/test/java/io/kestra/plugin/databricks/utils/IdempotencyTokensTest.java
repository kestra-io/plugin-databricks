package io.kestra.plugin.databricks.utils;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

class IdempotencyTokensTest {
    @Test
    void isDeterministicForTheSameSeedAndGeneration() {
        var first = IdempotencyTokens.token("taskrun-id", 0);
        var second = IdempotencyTokens.token("taskrun-id", 0);

        assertThat(first, equalTo(second));
    }

    @Test
    void differsAcrossGenerations() {
        var generationZero = IdempotencyTokens.token("taskrun-id", 0);
        var generationOne = IdempotencyTokens.token("taskrun-id", 1);

        assertThat(generationZero, not(equalTo(generationOne)));
    }

    @Test
    void differsAcrossSeeds() {
        var first = IdempotencyTokens.token("taskrun-id-1", 0);
        var second = IdempotencyTokens.token("taskrun-id-2", 0);

        assertThat(first, not(equalTo(second)));
    }

    @Test
    void staysWithinTheDatabricksSixtyFourCharacterLimit() {
        var longSeed = "x".repeat(500);

        var token = IdempotencyTokens.token(longSeed, 999);

        assertThat(token.length(), lessThanOrEqualTo(64));
    }
}
