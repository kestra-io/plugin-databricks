package io.kestra.plugin.databricks.zerobus;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.databricks.AbstractTask.AuthenticationConfig;

import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@MicronautTest
class WriteRecordsTest {

    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;
    private int port;
    private String endpoint;
    private List<Map<String, Object>> receivedRecords;
    private AtomicInteger chunkCount;
    private AtomicInteger tokenRequestCount;
    private Long customExpiresIn;
    private int returnStatus = 200;
    private String returnBody = "{}";
    private boolean oauthEndpointCalled = false;
    private String tokenRequestBody = null;
    private String tokenRequestAuth = null;
    private Boolean returnEmptyTokenResponse = false;
    private List<byte[]> mockServerRequestBodies = new ArrayList<>();

    @BeforeEach
    void setup() throws IOException {
        receivedRecords = new ArrayList<>();
        chunkCount = new AtomicInteger(0);
        tokenRequestCount = new AtomicInteger(0);
        customExpiresIn = null;
        returnStatus = 200;
        returnBody = "{}";
        oauthEndpointCalled = false;
        tokenRequestBody = null;
        tokenRequestAuth = null;
        returnEmptyTokenResponse = false;
        mockServerRequestBodies = new ArrayList<>();

        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            chunkCount.incrementAndGet();
            if (returnStatus != 200) {
                byte[] response = returnBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(returnStatus, response.length);
                exchange.getResponseBody().write(response);
                exchange.close();
                return;
            }

            try {
                byte[] requestBytes = exchange.getRequestBody().readAllBytes();
                mockServerRequestBodies.add(requestBytes);
                List<Map<String, Object>> chunk = JacksonMapper.ofJson().readValue(requestBytes, new TypeReference<>() {
                });
                receivedRecords.addAll(chunk);
                byte[] response = returnBody.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            } catch (Exception e) {
                exchange.sendResponseHeaders(500, 0);
            }
            exchange.close();
        });

        server.createContext("/oidc/v1/token", exchange ->
        {
            oauthEndpointCalled = true;
            int count = tokenRequestCount.incrementAndGet();
            try {
                tokenRequestBody = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                tokenRequestAuth = exchange.getRequestHeaders().getFirst("Authorization");
                if (returnStatus != 200 && returnStatus != 403 && returnStatus != 404 && returnStatus != 500) {
                    // special error simulation for token if needed
                    byte[] response = returnBody.getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(returnStatus, response.length);
                    exchange.getResponseBody().write(response);
                    exchange.close();
                    return;
                }
                String response;
                if (Boolean.TRUE.equals(returnEmptyTokenResponse)) {
                    response = "{\"some_other_field\": \"value\"}";
                } else {
                    String exp = customExpiresIn != null ? ", \"expires_in\": " + customExpiresIn : "";
                    response = "{\"access_token\": \"mock_oauth_token_" + count + "\"" + exp + "}";
                }
                exchange.sendResponseHeaders(200, response.length());
                exchange.getResponseBody().write(response.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                exchange.sendResponseHeaders(500, 0);
            }
            exchange.close();
        });

        server.start();
        port = server.getAddress().getPort();
        endpoint = "http://localhost:" + port;
    }

    @AfterEach
    void teardown() {
        if (server != null) {
            server.stop(0);
        }
    }

    private WriteRecords.WriteRecordsBuilder<?, ?> baseBuilder() {
        return WriteRecords.builder()
            .id("test_task")
            .type(WriteRecords.class.getName())
            .host(Property.of(endpoint))
            .endpoint(Property.of(endpoint))
            .workspaceId(Property.of("123456"))
            .catalog(Property.of("my_cat"))
            .schema(Property.of("my_schema"))
            .table(Property.of("my_table"))
            .authentication(
                AuthenticationConfig.builder()
                    .clientId(Property.of("client"))
                    .clientSecret(Property.of("secret"))
                    .build()
            );
    }

    private WriteRecords buildTask() {
        return baseBuilder().build();
    }

    @Test
    void inlineRecordsSendsJsonArrayAndReturnsCount() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(
                Property.of(
                    List.of(
                        Map.of("id", 1, "name", "test1"),
                        Map.of("id", 2, "name", "test2")
                    )
                )
            )
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(2L));
        assertThat(receivedRecords.size(), is(2));
        assertThat(receivedRecords.get(0).get("name"), is("test1"));
        assertThat(oauthEndpointCalled, is(true));

        long recordCountMetric = runContext.metrics().stream()
            .filter(m -> m.getName().equals("records.count"))
            .mapToLong(m -> ((Number) m.getValue()).longValue())
            .sum();
        assertThat(recordCountMetric, is(2L));
    }

    @Test
    void fromIonFileReadsRecords() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile)) {
            Map<String, Object> record1 = new java.util.HashMap<>();
            record1.put("id", 1);
            record1.put("name", "test1");
            record1.put("temp", new java.math.BigDecimal("22.5"));
            record1.put("active", true);
            record1.put("time", java.time.Instant.parse("2024-01-01T00:00:00Z"));
            record1.put("empty", null);
            io.kestra.core.serializers.FileSerde.write(fos, record1);
            io.kestra.core.serializers.FileSerde.write(fos, Map.of("id", 2, "name", "test2"));
        }
        URI uri = runContext.storage().putFile(tempFile);

        WriteRecords task = baseBuilder()
            .from(Property.of(uri.toString()))
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(2L));
        assertThat(receivedRecords.size(), is(2));
        assertThat(receivedRecords.get(0).get("temp"), is(22.5));
        assertThat(receivedRecords.get(0).get("active"), is(true));
        assertThat(receivedRecords.get(0).get("time"), is("2024-01-01T00:00:00.000Z"));
    }

    @Test
    void inlineListNullElementThrowsException() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        List<Map<String, Object>> recordsWithNull = new ArrayList<>();
        recordsWithNull.add(Map.of("id", 1));
        recordsWithNull.add(null);
        recordsWithNull.add(Map.of("id", 3));

        WriteRecords task = baseBuilder()
            .records(Property.of(recordsWithNull))
            .build();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(e.getMessage(), is("Record 2 is not an object."));
    }

    @Test
    void ionFileReadsRecordsTextAndBinary() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        File textFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(textFile)) {
            fos.write("{id:1, name:\"test1\", temp:22.5, active:true, time:2024-01-01T00:00:00Z, empty:null}\n{id:2, name:\"test2\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        URI textUri = runContext.storage().putFile(textFile);

        WriteRecords textTask = baseBuilder().from(Property.of(textUri.toString())).build();
        textTask.run(runContext);
        List<Map<String, Object>> textReceived = new ArrayList<>(receivedRecords);
        receivedRecords.clear();

        // Generated with ion-java 1.12.1 IonBinaryWriterBuilder: {id:1, name:"test1", temp:22.5, active:true, time:2024-01-01T00:00:00Z, empty:null} and {id:2, name:"test2"}
        String hex = "E00100EAEEA18183DE9D87BE9A8269648474656D70866163746976658474696D6585656D707479DE9D8A2101848574657374318B53C100E18C118D68800FE881818080808E0FDA8A210284857465737432";
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        assertThat("First four bytes must be binary Ion magic number", bytes[0] == (byte) 0xE0 && bytes[1] == 0x01 && bytes[2] == 0x00 && bytes[3] == (byte) 0xEA, is(true));

        File binFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(binFile)) {
            fos.write(bytes);
        }
        URI binUri = runContext.storage().putFile(binFile);

        WriteRecords binTask = baseBuilder().from(Property.of(binUri.toString())).build();
        binTask.run(runContext);
        List<Map<String, Object>> binReceived = new ArrayList<>(receivedRecords);
        receivedRecords.clear();

        assertThat(textReceived, is(binReceived));
        assertThat(textReceived.size(), is(2));
        assertThat(textReceived.get(0).get("temp"), is(22.5));
        assertThat(textReceived.get(0).get("active"), is(true));
        assertThat(textReceived.get(0).get("time"), is("2024-01-01T00:00:00Z"));
        assertThat(binReceived.get(0).get("time"), is("2024-01-01T00:00:00Z"));
    }

    @Test
    void chunkingMalformedBinaryThrows() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        String hex = "E00100EAEEA18183DE9D87BE9A8269648474656D70866163746976658474696D6585656D707479DE9D8A2101848574657374318B53C100E18C118D68800FE881818080808E0FDA8A210284857465737432";
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        // Truncate 1 byte (nextValue throws on record 2)
        byte[] truncated = java.util.Arrays.copyOf(bytes, bytes.length - 1);

        File binFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.FileOutputStream fos = new java.io.FileOutputStream(binFile)) {
            fos.write(truncated);
        }
        URI binUri = runContext.storage().putFile(binFile);

        WriteRecords task = baseBuilder().from(Property.of(binUri.toString())).build();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertTrue(e.getMessage().contains("Failed to parse record 2"));
        assertTrue(receivedRecords.isEmpty());
    }

    @Test
    void chunkingExactlyMaxRecords() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> recs = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK; i++) {
            recs.add(Map.of("id", i));
        }
        WriteRecords task = baseBuilder().records(Property.of(recs)).build();
        WriteRecords.Output run = task.run(runContext);
        assertThat(run.getRecordsCount(), is((long) WriteRecords.MAX_RECORDS_PER_CHUNK));
        assertThat(chunkCount.get(), is(1));
    }

    @Test
    void chunkingOneMoreThanMaxRecords() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> recs = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK + 1; i++) {
            recs.add(Map.of("id", i));
        }
        WriteRecords task = baseBuilder().records(Property.of(recs)).build();
        WriteRecords.Output run = task.run(runContext);
        assertThat(run.getRecordsCount(), is((long) WriteRecords.MAX_RECORDS_PER_CHUNK + 1));
        assertThat(chunkCount.get(), is(2));

        // Assert chunk sizes
        List<Integer> sizes = new ArrayList<>();
        for (byte[] req : mockServerRequestBodies) {
            List<Map<String, Object>> parsed = JacksonMapper.ofJson().readValue(req, new TypeReference<>() {
            });
            sizes.add(parsed.size());
        }
        assertThat(sizes.get(0), is(WriteRecords.MAX_RECORDS_PER_CHUNK));
        assertThat(sizes.get(1), is(1));
    }

    @Test
    void chunkingByByteSize() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        // small, oversized, small gives 3 requests with the oversized one alone
        Map<String, Object> small = Map.of("id", 1);
        String hugeStr = "a".repeat(WriteRecords.MAX_BYTES_PER_CHUNK + 10);
        Map<String, Object> oversized = Map.of("id", 2, "huge", hugeStr);
        Map<String, Object> small2 = Map.of("id", 3);

        WriteRecords task = baseBuilder().records(Property.of(List.of(small, oversized, small2))).build();
        WriteRecords.Output run = task.run(runContext);

        assertThat(run.getRecordsCount(), is(3L));
        assertThat(chunkCount.get(), is(3));

        List<Integer> sizes = new ArrayList<>();
        for (byte[] req : mockServerRequestBodies) {
            List<Map<String, Object>> parsed = JacksonMapper.ofJson().readValue(req, new TypeReference<>() {
            });
            sizes.add(parsed.size());
        }
        assertThat(sizes.get(0), is(1)); // small
        assertThat(sizes.get(1), is(1)); // oversized alone
        assertThat(sizes.get(2), is(1)); // small2
    }

    @Test
    void emptyInputDoesNothing() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        File tempFile = runContext.workingDir().createTempFile(".jsonl").toFile();
        tempFile.createNewFile(); // empty file
        URI internalUri = runContext.storage().putFile(tempFile);

        WriteRecords task = baseBuilder().from(Property.of(internalUri.toString())).build();
        WriteRecords.Output run = task.run(runContext);

        assertThat(run.getRecordsCount(), is(0L));
        assertThat(chunkCount.get(), is(0));
        assertThat(oauthEndpointCalled, is(false));
    }

    @Test
    void fromJsonLinesFileReadsRecords() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        File tempFile = runContext.workingDir().createTempFile(".jsonl").toFile();
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("{\"id\":1, \"name\":\"test1\"}\n");
            writer.write("{\"id\":2, \"name\":\"test2\"}\n");
        }
        URI uri = runContext.storage().putFile(tempFile);

        WriteRecords task = baseBuilder()
            .from(Property.of(uri.toString()))
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(2L));
        assertThat(receivedRecords.size(), is(2));
        assertThat(receivedRecords.get(1).get("name"), is("test2"));
    }

    @Test
    void emptyInputSendsNoRequest() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of()))
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(0L));
        assertThat(chunkCount.get(), is(0));
    }

    @Test
    void recordsAndFromBothSetThrows() {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of(Map.of("id", 1))))
            .from(Property.of("kestra://something"))
            .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Set either 'records' or 'from', not both"));
    }

    @Test
    void neitherRecordsNorFromThrows() {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Set 'records' for inline data or 'from'"));
    }

    @Test
    void chunkingSplitsAtMaxRecords() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> lotsOfRecords = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK + 10; i++) {
            lotsOfRecords.add(Map.of("id", i));
        }

        WriteRecords task = baseBuilder()
            .records(Property.of(lotsOfRecords))
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is((long) WriteRecords.MAX_RECORDS_PER_CHUNK + 10));
        assertThat(chunkCount.get(), is(2));
    }

    @Test
    void oversizedSingleRecordSentAlone() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        // Create a 5MB string
        String hugeString = "A".repeat(5 * 1024 * 1024);

        WriteRecords task = baseBuilder()
            .records(
                Property.of(
                    List.of(
                        Map.of("id", 1, "data", hugeString),
                        Map.of("id", 2)
                    )
                )
            )
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(2L));
        // The first record exceeds max bytes, so it gets sent alone in chunk 1, second record in chunk 2
        assertThat(chunkCount.get(), is(2));
    }

    @Test
    void serverErrorThrowsWithAcceptedCount() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> lotsOfRecords = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK + 10; i++) {
            lotsOfRecords.add(Map.of("id", i));
        }

        WriteRecords task = baseBuilder()
            .records(Property.of(lotsOfRecords))
            .build();

        // Fail the second chunk
        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            int current = chunkCount.incrementAndGet();
            if (current == 2) {
                String errorBody = "Internal Server Error";
                exchange.sendResponseHeaders(500, errorBody.length());
                exchange.getResponseBody().write(errorBody.getBytes());
            } else {
                exchange.sendResponseHeaders(200, 2);
                exchange.getResponseBody().write("{}".getBytes());
            }
            exchange.close();
        });

        Exception ex = assertThrows(Exception.class, () -> task.run(runContext));

        assertThat(ex.getMessage(), containsString("Zerobus Ingest returned a server error (HTTP 500)"));
        assertThat(ex.getMessage(), containsString("Internal Server Error"));
        assertThat(ex.getMessage(), containsString("500 records in prior chunks were already accepted"));
        assertThat(ex.getMessage(), containsString("at-least-once delivery"));

        // Ensure no secrets are leaked
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));

        Throwable cause = ex.getCause();
        if (cause != null) {
            assertThat(cause.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
            assertThat(cause.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));
            String stackTrace = java.util.Arrays.toString(cause.getStackTrace());
            assertThat(stackTrace, org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
            assertThat(stackTrace, org.hamcrest.Matchers.not(containsString("secret")));
        }

        long recordCountMetric = runContext.metrics().stream()
            .filter(m -> m.getName().equals("records.count"))
            .mapToLong(m -> ((Number) m.getValue()).longValue())
            .sum();
        assertThat(recordCountMetric, is(500L));
    }

    @Test
    void clientErrorOnSecondChunkThrowsWithAcceptedCount() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> lotsOfRecords = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK + 10; i++) {
            lotsOfRecords.add(Map.of("id", i));
        }

        WriteRecords task = baseBuilder()
            .records(Property.of(lotsOfRecords))
            .build();

        // Fail the second chunk
        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            int current = chunkCount.incrementAndGet();
            if (current == 2) {
                String errorBody = "Forbidden";
                exchange.sendResponseHeaders(403, errorBody.length());
                exchange.getResponseBody().write(errorBody.getBytes());
            } else {
                exchange.sendResponseHeaders(200, 2);
                exchange.getResponseBody().write("{}".getBytes());
            }
            exchange.close();
        });

        Exception ex = assertThrows(Exception.class, () -> task.run(runContext));

        assertThat(ex.getMessage(), containsString("Zerobus Ingest rejected the request (HTTP 403)"));
        assertThat(ex.getMessage(), containsString("Forbidden"));
        assertThat(ex.getMessage(), containsString("500 records in prior chunks were already accepted"));
        assertThat(ex.getMessage(), containsString("at-least-once delivery"));

        // Ensure no secrets are leaked
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));

        Throwable cause = ex.getCause();
        if (cause != null) {
            assertThat(cause.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
            assertThat(cause.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));
            String stackTrace = java.util.Arrays.toString(cause.getStackTrace());
            assertThat(stackTrace, org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
            assertThat(stackTrace, org.hamcrest.Matchers.not(containsString("secret")));
        }
    }

    @Test
    void clientErrorThrowsWithTableAdvice() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        returnStatus = 403;
        returnBody = "Permission Denied";

        Exception ex = assertThrows(Exception.class, () -> task.run(runContext));

        assertThat(ex.getMessage(), containsString("Zerobus Ingest rejected the request (HTTP 403)"));
        assertThat(ex.getMessage(), containsString("Permission Denied"));
        assertThat(ex.getMessage(), containsString("Check that the table 'my_cat.my_schema.my_table' exists"));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("qa_secret")));
    }

    @Test
    void oauthTokenRequestCarriesExactFormParams() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        task.run(runContext);

        assertThat(oauthEndpointCalled, is(true));
        assertThat(tokenRequestBody, containsString("grant_type=client_credentials"));
        assertThat(tokenRequestBody, containsString("scope=all-apis"));
        assertThat(tokenRequestBody, containsString("resource=api%3A%2F%2Fdatabricks%2Fworkspaces%2F123456%2FzerobusDirectWriteApi"));
        assertThat(tokenRequestBody, containsString("authorization_details="));

        String expectedAuth = "Basic " + Base64.getEncoder().encodeToString("client:secret".getBytes(StandardCharsets.UTF_8));
        assertThat(tokenRequestAuth, is(expectedAuth));

        // decode and check auth details JSON
        String encodedDetails = tokenRequestBody.split("authorization_details=")[1].split("&")[0];
        String decodedDetails = java.net.URLDecoder.decode(encodedDetails, StandardCharsets.UTF_8);
        assertThat(decodedDetails, containsString("\"object_full_path\":\"my_cat.my_schema.my_table\""));
    }

    @Test
    void patSendsBearerDirectlyPrecedenceOverOAuth() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .authentication(
                AuthenticationConfig.builder()
                    .token(Property.of("my_pat_token"))
                    .clientId(Property.of("client"))
                    .clientSecret(Property.of("secret"))
                    .build()
            )
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        task.run(runContext);

        assertThat(oauthEndpointCalled, is(false));

        // Let's capture the ingest headers
        final List<String> authHeaders = new ArrayList<>();
        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            authHeaders.addAll(exchange.getRequestHeaders().get("Authorization"));
            exchange.sendResponseHeaders(200, 2);
            exchange.getResponseBody().write("{}".getBytes());
            exchange.close();
        });

        task.run(runContext);

        assertThat(oauthEndpointCalled, is(false)); // Should skip OAuth
        assertThat(authHeaders.size(), is(1));
        assertThat(authHeaders.get(0), is("Bearer my_pat_token"));
    }

    @Test
    void unsupportedAuthTypeThrows() {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .authentication(
                AuthenticationConfig.builder()
                    .password(Property.of("password"))
                    .build()
            )
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Zerobus Ingest REST requires either a personal access token"));
    }

    @Test
    void oauthTokenRequestMissingAccessTokenThrowsError() throws Exception {
        returnEmptyTokenResponse = true;
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, baseBuilder().build(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        Exception e = assertThrows(Exception.class, () -> task.run(runContext));
        assertThat(e.getMessage(), containsString("access_token missing in response"));
        assertThat(e.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));
    }

    @Test
    void missingHostWithOAuthThrows() {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, baseBuilder().build(), Map.of());
        WriteRecords task = baseBuilder()
            .host(null)
            .endpoint(null)
            .region(Property.of("us-east-1"))
            .workspaceId(Property.of("123456"))
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("host is required when using OAuth authentication."));
    }

    @Test
    void hostTrailingSlashIsStripped() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, baseBuilder().build(), Map.of());
        WriteRecords task = baseBuilder()
            .host(Property.of("http://localhost:" + port + "/")) // Adds a trailing slash
            .endpoint(Property.of("http://localhost:" + port)) // Use mock server for ingest
            .workspaceId(Property.of("123456"))
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        task.run(runContext);

        assertThat(oauthEndpointCalled, is(true));
        // We know it reached our mock server, which means the token URL was constructed properly without a double slash
    }

    @Test
    void defaultUrlUsesWorkspaceAndRegion() {
        String url = WriteRecords.buildIngestUrl(null, "ws123", "us-east-1", "cat", "sch", "tbl");
        assertThat(url, is("https://ws123.zerobus.us-east-1.cloud.databricks.com/zerobus/v1/tables/cat.sch.tbl/insert"));
    }

    @Test
    void endpointOverridesRegion() {
        String url = WriteRecords.buildIngestUrl("https://my-proxy.example.com", "ws123", "us-east-1", "cat", "sch", "tbl");
        assertThat(url, is("https://my-proxy.example.com/zerobus/v1/tables/cat.sch.tbl/insert"));
    }

    @Test
    void oauthResourceStringContainsWorkspaceId() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        task.run(runContext);

        assertThat(oauthEndpointCalled, is(true));
        String decoded = java.net.URLDecoder.decode(tokenRequestBody, StandardCharsets.UTF_8);
        assertThat(decoded, containsString("resource=api://databricks/workspaces/123456/zerobusDirectWriteApi"));
    }

    @Test
    void oauthAuthorizationDetailsContainsAllPrivileges() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        task.run(runContext);

        assertThat(oauthEndpointCalled, is(true));
        String decoded = java.net.URLDecoder.decode(tokenRequestBody, StandardCharsets.UTF_8);
        assertThat(decoded, containsString("USE CATALOG"));
        assertThat(decoded, containsString("USE SCHEMA"));
        assertThat(decoded, containsString("SELECT"));
        assertThat(decoded, containsString("MODIFY"));
    }

    @Test
    void invalidFileAndUnsupportedAuthTypeThrows() throws Exception {
        WriteRecords task = WriteRecords.builder()
            .id("test")
            .type(WriteRecords.class.getName())
            .workspaceId(Property.of("workspace"))
            .catalog(Property.of("catalog"))
            .schema(Property.of("schema"))
            .table(Property.of("table"))
            .region(Property.of("us-east-1"))
            .from(Property.of("kestra://invalid/path"))
            .build();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, null)));
        assertTrue(e.getMessage().contains("Other authentication types are not supported"));
    }

    @Test
    void oauthTokenRequestNonStringAccessTokenThrowsError() throws Exception {
        // We will temporarily change the /oidc/v1/token behaviour
        server.removeContext("/oidc/v1/token");
        server.createContext("/oidc/v1/token", exchange ->
        {
            String response = "{\"access_token\": 12345}";
            exchange.sendResponseHeaders(200, response.length());
            exchange.getResponseBody().write(response.getBytes(StandardCharsets.UTF_8));
            exchange.close();
        });

        WriteRecords task = WriteRecords.builder()
            .id("test")
            .type(WriteRecords.class.getName())
            .workspaceId(Property.of("workspace"))
            .host(Property.of(endpoint))
            .authentication(
                AuthenticationConfig.builder()
                    .clientId(Property.of("mock-client"))
                    .clientSecret(Property.of("mock-secret"))
                    .build()
            )
            .catalog(Property.of("my_cat"))
            .schema(Property.of("my_schema"))
            .table(Property.of("my_table"))
            .region(Property.of("us-east-1"))
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, null)));
        assertTrue(e.getMessage().contains("access_token in response is not a string"));
    }

    @Test
    void severalRecordsOnOneLine() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write("{\"id\":1} {\"id\":2}\n\n{\"id\":3}");
        }
        URI uri = runContext.storage().putFile(tempFile);

        WriteRecords task = baseBuilder()
            .from(Property.of(uri.toString()))
            .build();

        WriteRecords.Output output = task.run(runContext);

        assertThat(output.getRecordsCount(), is(3L));
        assertThat(receivedRecords.size(), is(3));
    }

    @Test
    void nonObjectRecordThrowsException() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), null);
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.PrintWriter out = new java.io.PrintWriter(tempFile)) {
            out.print("{\"a\": 1} \"not an object\"");
        }
        URI fromUri = runContext.storage().putFile(tempFile);

        WriteRecords task = baseBuilder()
            .from(Property.of(fromUri.toString()))
            .build();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertTrue(e.getMessage().contains("Record 2 is not an object."), e.getMessage());

        // Test with null
        File tempFile2 = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.PrintWriter out = new java.io.PrintWriter(tempFile2)) {
            out.print("{\"a\": 1} null");
        }
        URI fromUri2 = runContext.storage().putFile(tempFile2);

        WriteRecords task2 = baseBuilder()
            .from(Property.of(fromUri2.toString()))
            .build();

        IllegalStateException e2 = assertThrows(IllegalStateException.class, () -> task2.run(runContext));
        assertTrue(e2.getMessage().contains("Record 2 is not an object."), e2.getMessage());
    }

    @Test
    void malformedRecordThrowsException() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), null);
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (java.io.PrintWriter out = new java.io.PrintWriter(tempFile)) {
            out.print("{\"a\": 1}\n{not valid}");
        }
        URI fromUri = runContext.storage().putFile(tempFile);

        WriteRecords task = baseBuilder()
            .from(Property.of(fromUri.toString()))
            .build();

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertTrue(e.getMessage().contains("Failed to parse record 2"), e.getMessage());
    }

    @Test
    void shortExpiresInRefreshesBetweenChunks() throws Exception {
        customExpiresIn = 10L; // < 60 margin
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> recs = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK + 1; i++) {
            recs.add(Map.of("id", i));
        }
        WriteRecords task = baseBuilder().records(Property.of(recs)).build();

        final List<String> authHeaders = new ArrayList<>();
        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            authHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            exchange.sendResponseHeaders(200, 2);
            exchange.getResponseBody().write("{}".getBytes());
            exchange.close();
        });

        WriteRecords.Output output = task.run(runContext);
        assertThat(output.getRecordsCount(), is((long) WriteRecords.MAX_RECORDS_PER_CHUNK + 1));
        assertThat(tokenRequestCount.get(), is(2)); // Fetched initially and then refreshed for the second chunk
        assertThat(authHeaders.size(), is(2));
        assertThat(authHeaders.get(0), is("Bearer mock_oauth_token_1"));
        assertThat(authHeaders.get(1), is("Bearer mock_oauth_token_2"));
    }

    @Test
    void normalExpiresInOverThreeChunksHitsTokenEndpointOnce() throws Exception {
        customExpiresIn = 3600L;
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> recs = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK * 2 + 1; i++) {
            recs.add(Map.of("id", i));
        }
        WriteRecords task = baseBuilder().records(Property.of(recs)).build();

        WriteRecords.Output output = task.run(runContext);
        assertThat(output.getRecordsCount(), is((long) WriteRecords.MAX_RECORDS_PER_CHUNK * 2 + 1));
        assertThat(chunkCount.get(), is(3));
        assertThat(tokenRequestCount.get(), is(1));
    }

    @Test
    void one401OnIngestRecovers() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder().records(Property.of(List.of(Map.of("id", 1)))).build();

        AtomicInteger chunkAttempts = new AtomicInteger(0);
        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            int attempt = chunkAttempts.incrementAndGet();
            if (attempt == 1) {
                exchange.sendResponseHeaders(401, 2);
                exchange.getResponseBody().write("{}".getBytes());
            } else {
                chunkCount.incrementAndGet();
                exchange.sendResponseHeaders(200, 2);
                exchange.getResponseBody().write("{}".getBytes());
            }
            exchange.close();
        });

        WriteRecords.Output output = task.run(runContext);
        assertThat(output.getRecordsCount(), is(1L));
        assertThat(chunkAttempts.get(), is(2));
        assertThat(tokenRequestCount.get(), is(2)); // Initial fetch + retry fetch
    }

    @Test
    void two401sFailWithClearMessage() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder().records(Property.of(List.of(Map.of("id", 1)))).build();

        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            exchange.sendResponseHeaders(401, 2);
            exchange.getResponseBody().write("{}".getBytes());
            exchange.close();
        });

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Zerobus Ingest returned a 401 Unauthorized even after refreshing the OAuth token. 0 records in prior chunks were already accepted"));
        assertThat(tokenRequestCount.get(), is(2));

        // Ensure no secrets are leaked
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
        assertThat(ex.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));

        Throwable cause = ex.getCause();
        if (cause != null) {
            assertThat(cause.getMessage(), org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
            assertThat(cause.getMessage(), org.hamcrest.Matchers.not(containsString("secret")));
            String stackTrace = java.util.Arrays.toString(cause.getStackTrace());
            assertThat(stackTrace, org.hamcrest.Matchers.not(containsString("mock_oauth_token")));
            assertThat(stackTrace, org.hamcrest.Matchers.not(containsString("secret")));
        }
    }

    @Test
    void pat401FailsWithoutTokenEndpointHit() throws Exception {
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        WriteRecords task = baseBuilder()
            .authentication(
                AuthenticationConfig.builder()
                    .token(Property.of("my_pat_token"))
                    .build()
            )
            .records(Property.of(List.of(Map.of("id", 1))))
            .build();

        server.removeContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert");
        server.createContext("/zerobus/v1/tables/my_cat.my_schema.my_table/insert", exchange ->
        {
            exchange.sendResponseHeaders(401, 2);
            exchange.getResponseBody().write("{}".getBytes());
            exchange.close();
        });

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> task.run(runContext));
        assertThat(ex.getMessage(), containsString("Zerobus Ingest rejected the request (HTTP 401)"));
        assertThat(tokenRequestCount.get(), is(0));
    }

    @Test
    void missingExpiresInDefaultsToNoRefresh() throws Exception {
        customExpiresIn = null;
        RunContext runContext = TestsUtils.mockRunContext(runContextFactory, buildTask(), Map.of());
        List<Map<String, Object>> recs = new ArrayList<>();
        for (int i = 0; i < WriteRecords.MAX_RECORDS_PER_CHUNK * 2 + 1; i++) {
            recs.add(Map.of("id", i));
        }
        WriteRecords task = baseBuilder().records(Property.of(recs)).build();

        WriteRecords.Output output = task.run(runContext);
        assertThat(output.getRecordsCount(), is((long) WriteRecords.MAX_RECORDS_PER_CHUNK * 2 + 1));
        assertThat(chunkCount.get(), is(3));
        assertThat(tokenRequestCount.get(), is(1)); // Because 3600 is > margin, no refresh
    }

}