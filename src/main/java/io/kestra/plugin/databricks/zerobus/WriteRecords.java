package io.kestra.plugin.databricks.zerobus;

import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpHeaders;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;

import io.kestra.core.http.HttpRequest;
import io.kestra.core.http.HttpResponse;
import io.kestra.core.http.client.HttpClient;
import io.kestra.core.http.client.HttpClientResponseException;
import io.kestra.core.http.client.configurations.HttpConfiguration;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Metric;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.databricks.AbstractTask;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
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
@Plugin(
    examples = {
        @Example(title = "Push inline records to a Databricks Delta table", full = true, code = """
            id: write_inline_records_to_databricks
            namespace: company.team

            tasks:
              - id: write_records
                type: io.kestra.plugin.databricks.zerobus.WriteRecords
                host: "{{ secret('DATABRICKS_HOST') }}"
                authentication:
                  token: "{{ secret('DATABRICKS_TOKEN') }}"
                workspaceId: "{{ secret('DATABRICKS_WORKSPACE_ID') }}"
                region: us-east-1
                catalog: main
                schema: events
                table: user_events
                records:
                  - userId: usr_001
                    event: page_view
                  - userId: usr_002
                    event: purchase
            """),
        @Example(title = "Push records from an internal storage file (Ion or JSON-lines)", full = true, code = """
            id: write_file_to_databricks
            namespace: company.team

            tasks:
              - id: write_records
                type: io.kestra.plugin.databricks.zerobus.WriteRecords
                host: "{{ secret('DATABRICKS_HOST') }}"
                authentication:
                  clientId: "{{ secret('DATABRICKS_CLIENT_ID') }}"
                  clientSecret: "{{ secret('DATABRICKS_CLIENT_SECRET') }}"
                workspaceId: "{{ secret('DATABRICKS_WORKSPACE_ID') }}"
                region: us-east-1
                catalog: main
                schema: events
                table: raw_events
                from: "{{ outputs.fetch_events.uri }}"
            """)
    },
    metrics = {
        @Metric(name = "records.count", type = "counter", description = "Number of records pushed to Zerobus Ingest")
    }
)
@Schema(title = "Push data directly into a Unity Catalog Delta table via Zerobus Ingest", description = """
    This task sends data to a Databricks Delta table via the push-based Zerobus Ingest REST API.

    It supports at-least-once delivery. If a task is retried after a partial failure, records that were already
    successfully accepted in previous chunks will be sent again and may be duplicated in the target table.

    The task authenticates using either OAuth client credentials or a Personal Access Token (PAT).
    An OAuth token is refreshed shortly before it expires, and a 401 on the ingest call triggers one refresh and one retry of that chunk. A PAT is never refreshed or retried.
    **Note:** PAT authentication is documented as unverified against Zerobus Ingest; use OAuth client credentials if possible.
    The `host` property is required when using OAuth authentication.

    Records are sent in chunks, bounded by conservative assumptions since the API docs do not specify a per-request limit.

    **Note:** The inherited properties `configFile` and `accountId` are ignored by this task, and `AbstractTask` environment variable fallbacks are not honoured.
    """)
public class WriteRecords extends AbstractTask implements RunnableTask<WriteRecords.Output> {
    private static final String UNSUPPORTED_AUTH_MSG = "Zerobus Ingest REST requires either a personal access token (authentication.token) or OAuth client credentials (authentication.clientId + authentication.clientSecret). Other authentication types are not supported.";

    static final int MAX_RECORDS_PER_CHUNK = 500;
    static final int MAX_BYTES_PER_CHUNK = 4 * 1024 * 1024; // 4 MB
    static final long TOKEN_REFRESH_MARGIN_SECONDS = 60;

    @NotNull
    @Schema(title = "Unity Catalog catalog name")
    @PluginProperty(group = "main")
    private Property<String> catalog;

    @NotNull
    @Schema(title = "Schema (database) name")
    @PluginProperty(group = "main")
    private Property<String> schema;

    @NotNull
    @Schema(title = "Target Delta table name")
    @PluginProperty(group = "main")
    private Property<String> table;

    @Schema(title = "Numeric workspace ID", description = "Used to build the Zerobus endpoint and OAuth resource parameter.")
    @PluginProperty(group = "connection")
    private Property<String> workspaceId;

    @Schema(title = "Cloud region", description = "Region (e.g. us-east-1). Ignored when 'endpoint' is set. Required when 'endpoint' is not set.")
    @PluginProperty(group = "connection")
    private Property<String> region;

    @Schema(title = "Zerobus endpoint URL override", description = "Replaces the auto-computed URL (e.g. https://my-proxy).")
    @PluginProperty(group = "connection")
    private Property<String> endpoint;

    @Schema(title = "Inline records", description = "List of JSON objects. Mutually exclusive with 'from'.")
    @PluginProperty(group = "main")
    private Property<List<Map<String, Object>>> records;

    @Schema(title = "Internal storage URI", description = "URI of an Ion or JSON-lines file. Mutually exclusive with 'records'.")
    @PluginProperty(internalStorageURI = true, group = "main")
    private Property<String> from;

    @Override
    public Output run(RunContext runContext) throws Exception {
        boolean hasRecords = this.records != null;
        boolean hasFrom = this.from != null;

        if (hasRecords == hasFrom) {
            if (hasRecords) {
                throw new IllegalArgumentException("Set either 'records' or 'from', not both.");
            } else {
                throw new IllegalArgumentException(
                    "Set 'records' for inline data or 'from' for a Kestra internal storage URI."
                );
            }
        }

        String renderedEndpoint = runContext.render(this.endpoint).as(String.class).orElse(null);
        String renderedRegion = runContext.render(this.region).as(String.class).orElse(null);
        String renderedWorkspaceId = runContext.render(this.workspaceId).as(String.class).orElse(null);

        if (renderedEndpoint == null && renderedRegion == null) {
            throw new IllegalArgumentException(
                "Either 'endpoint' or 'region' must be set to determine the Zerobus Ingest server."
            );
        }

        String renderedCatalog = runContext.render(this.catalog).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("catalog must be provided"));
        String renderedSchema = runContext.render(this.schema).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("schema must be provided"));
        String renderedTable = runContext.render(this.table).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("table must be provided"));
        String renderedHost = this.getHost() != null ? runContext.render(this.getHost()).as(String.class).orElse(null)
            : null;
        validateAuthentication(runContext, renderedWorkspaceId, renderedHost);

        if (renderedEndpoint == null && renderedWorkspaceId == null) {
            throw new IllegalArgumentException("workspaceId is required when endpoint is not set.");
        }

        String ingestUrl = buildIngestUrl(
            renderedEndpoint, renderedWorkspaceId, renderedRegion, renderedCatalog,
            renderedSchema, renderedTable
        );

        AtomicLong totalAcceptedCount = new AtomicLong(0);
        try (
            var httpClient = HttpClient.builder()
                .runContext(runContext)
                .configuration(HttpConfiguration.builder().build())
                .build()
        ) {

            List<Map<String, Object>> chunk = new ArrayList<>();
            int currentChunkBytes = 0;
            TokenHolder tokenHolder = null;

            @SuppressWarnings({ "unchecked", "rawtypes" })
            List<Map<String, Object>> recordsList = hasRecords
                ? (List) runContext.render(this.records).asList(Map.class)
                : null;

            InputStream inputStream = null;
            MappingIterator<Map<String, Object>> iterator = null;
            if (!hasRecords) {
                String fromUri = runContext.render(this.from).as(String.class)
                    .orElseThrow(() -> new IllegalArgumentException("from must be provided"));
                inputStream = runContext.storage().getFile(URI.create(fromUri));
                iterator = JacksonMapper.ofIon().readerFor(new TypeReference<Map<String, Object>>() {
                }).readValues(inputStream);
            }

            try {
                int inlineIndex = 0;
                int recordOrdinal = 0;
                while (true) {
                    Map<String, Object> record = null;
                    if (hasRecords) {
                        if (inlineIndex < recordsList.size()) {
                            recordOrdinal++;
                            Map<String, Object> listElement = recordsList.get(inlineIndex++);
                            if (listElement == null) {
                                throw new IllegalStateException("Record " + recordOrdinal + " is not an object.");
                            }
                            record = listElement;
                        }
                    } else {
                        recordOrdinal++;
                        boolean hasNext = false;
                        try {
                            hasNext = iterator.hasNextValue();
                        } catch (Exception e) {
                            throw new IllegalStateException("Failed to parse record " + recordOrdinal + ": " + e.getMessage(), e);
                        }
                        if (hasNext) {
                            Map<String, Object> mapObj;
                            try {
                                mapObj = iterator.nextValue();
                            } catch (com.fasterxml.jackson.databind.exc.MismatchedInputException e) {
                                throw new IllegalStateException("Record " + recordOrdinal + " is not an object.");
                            } catch (Exception e) {
                                throw new IllegalStateException("Failed to parse record " + recordOrdinal + ": " + e.getMessage(), e);
                            }
                            if (mapObj == null) {
                                throw new IllegalStateException("Record " + recordOrdinal + " is not an object.");
                            }
                            @SuppressWarnings("unchecked")
                            Map<String, Object> castedRecord = (Map<String, Object>) convertIonValues(mapObj);
                            record = castedRecord;
                        }
                    }

                    if (record == null) {
                        break;
                    }

                    byte[] recordBytes = JacksonMapper.ofJson().writeValueAsBytes(record);

                    if (
                        !chunk.isEmpty() && (chunk.size() >= MAX_RECORDS_PER_CHUNK
                            || currentChunkBytes + recordBytes.length > MAX_BYTES_PER_CHUNK)
                    ) {
                        tokenHolder = getOrRefreshBearerToken(tokenHolder, runContext, renderedWorkspaceId, renderedCatalog, renderedSchema, renderedTable, renderedHost);
                        sendChunkWithRetry(
                            httpClient, ingestUrl, tokenHolder, chunk, renderedCatalog, renderedSchema,
                            renderedTable, totalAcceptedCount, runContext, renderedHost, renderedWorkspaceId
                        );
                        chunk.clear();
                        currentChunkBytes = 0;
                    }

                    chunk.add(record);
                    currentChunkBytes += recordBytes.length;
                }

                if (!chunk.isEmpty()) {
                    tokenHolder = getOrRefreshBearerToken(tokenHolder, runContext, renderedWorkspaceId, renderedCatalog, renderedSchema, renderedTable, renderedHost);
                    sendChunkWithRetry(
                        httpClient, ingestUrl, tokenHolder, chunk, renderedCatalog, renderedSchema,
                        renderedTable, totalAcceptedCount, runContext, renderedHost, renderedWorkspaceId
                    );
                }
            } finally {
                if (iterator != null) {
                    iterator.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        } finally {
            runContext.metric(Counter.of("records.count", totalAcceptedCount.get()));
        }

        return Output.builder().recordsCount(totalAcceptedCount.get()).build();
    }

    private void sendChunkWithRetry(HttpClient httpClient, String url, TokenHolder tokenHolder,
        List<Map<String, Object>> chunk, String catalog, String schema, String table, AtomicLong alreadyAccepted,
        RunContext runContext, String host, String workspaceId) throws Exception {
        try {
            sendChunk(httpClient, url, tokenHolder.getToken(), chunk, catalog, schema, table, alreadyAccepted);
        } catch (HttpClientResponseException e) {
            int status = e.getResponse() != null ? e.getResponse().getStatus().getCode() : 500;
            if (status == 401 && !tokenHolder.isPat()) {
                refreshOAuthToken(tokenHolder, runContext, host, workspaceId, catalog, schema, table);
                try {
                    sendChunk(httpClient, url, tokenHolder.getToken(), chunk, catalog, schema, table, alreadyAccepted);
                } catch (HttpClientResponseException retryEx) {
                    int retryStatus = retryEx.getResponse() != null ? retryEx.getResponse().getStatus().getCode() : 500;
                    if (retryStatus == 401) {
                        throw new IllegalStateException(
                            String.format(
                                "Zerobus Ingest returned a 401 Unauthorized even after refreshing the OAuth token. %d records in prior chunks were already accepted (at-least-once delivery).",
                                alreadyAccepted.get()
                            ), retryEx
                        );
                    }
                    throw mapSendChunkException(retryEx, catalog, schema, table, alreadyAccepted);
                }
            } else {
                throw mapSendChunkException(e, catalog, schema, table, alreadyAccepted);
            }
        }
    }

    private void sendChunk(HttpClient httpClient, String url, String token, List<Map<String, Object>> chunk,
        String catalog, String schema, String table, AtomicLong alreadyAccepted) throws Exception {
        HttpRequest request = HttpRequest.builder()
            .uri(URI.create(url))
            .method("POST")
            .body(HttpRequest.JsonRequestBody.of(chunk))
            .headers(
                HttpHeaders.of(
                    Map.of(
                        "Authorization", List.of("Bearer " + token)
                    ),
                    (a, b) -> true
                )
            )
            .build();

        httpClient.request(request, String.class);
        alreadyAccepted.addAndGet(chunk.size());
    }

    private IllegalStateException mapSendChunkException(HttpClientResponseException e, String catalog, String schema,
        String table, AtomicLong alreadyAccepted) {
        int status = e.getResponse() != null ? e.getResponse().getStatus().getCode() : 500;
        String body = readHttpErrorBody(e);

        if (status >= 400 && status < 500) {
            String advice = (status == 403 || status == 404) ? String.format(
                " Check that the table '%s.%s.%s' exists, the schema matches, and the service principal has USE CATALOG, USE SCHEMA, SELECT, and MODIFY privileges.",
                catalog, schema, table
            ) : "";
            return new IllegalStateException(
                String.format(
                    "Zerobus Ingest rejected the request (HTTP %d): %s.%s %d records in prior chunks were already accepted (at-least-once delivery).",
                    status, truncate(body), advice,
                    alreadyAccepted.get()
                ),
                e
            );
        } else {
            return new IllegalStateException(
                String.format(
                    "Zerobus Ingest returned a server error (HTTP %d): %s. The request may be retried. %d records in prior chunks were already accepted (at-least-once delivery).",
                    status,
                    truncate(body), alreadyAccepted.get()
                ),
                e
            );
        }
    }

    private void refreshOAuthToken(TokenHolder tokenHolder, RunContext runContext, String host, String workspaceId,
        String catalog, String schema, String table) throws Exception {
        String clientId = runContext.render(getAuthentication().getClientId()).as(String.class).orElseThrow(() -> new IllegalArgumentException("clientId must be provided"));
        String clientSecret = runContext.render(getAuthentication().getClientSecret()).as(String.class).orElseThrow(() -> new IllegalArgumentException("clientSecret must be provided"));
        OAuthToken refreshed = fetchOAuthToken(
            runContext, host, workspaceId, clientId, clientSecret, catalog, schema, table
        );
        tokenHolder.update(refreshed.accessToken, refreshed.expiresIn);
    }

    private TokenHolder getOrRefreshBearerToken(TokenHolder currentHolder, RunContext runContext, String workspaceId,
        String catalog, String schema, String table, String host) throws Exception {
        if (currentHolder == null) {
            return resolveBearerToken(runContext, workspaceId, catalog, schema, table);
        } else if (currentHolder.needsRefresh()) {
            refreshOAuthToken(currentHolder, runContext, host, workspaceId, catalog, schema, table);
        }
        return currentHolder;
    }

    private void validateAuthentication(RunContext runContext, String renderedWorkspaceId, String renderedHost)
        throws Exception {
        if (this.getAuthentication() != null) {
            String token = runContext.render(this.getAuthentication().getToken()).as(String.class).orElse(null);
            String clientId = runContext.render(this.getAuthentication().getClientId()).as(String.class).orElse(null);
            String clientSecret = runContext.render(this.getAuthentication().getClientSecret()).as(String.class)
                .orElse(null);

            if (token != null) {
                return;
            } else if (clientId != null && clientSecret != null) {
                if (renderedWorkspaceId == null) {
                    throw new IllegalArgumentException("workspaceId is required when using OAuth authentication.");
                }
                if (renderedHost == null) {
                    throw new IllegalArgumentException("host is required when using OAuth authentication.");
                }
                return;
            }
        }
        throw new IllegalArgumentException(UNSUPPORTED_AUTH_MSG);
    }

    private TokenHolder resolveBearerToken(RunContext runContext, String renderedWorkspaceId, String renderedCatalog,
        String renderedSchema, String renderedTable) throws Exception {
        String token = runContext.render(this.getAuthentication().getToken()).as(String.class).orElse(null);
        if (token != null) {
            return new TokenHolder(token, true, 0);
        }

        String clientId = runContext.render(this.getAuthentication().getClientId()).as(String.class).orElseThrow(() -> new IllegalArgumentException("clientId must be provided"));
        String clientSecret = runContext.render(this.getAuthentication().getClientSecret()).as(String.class)
            .orElseThrow(() -> new IllegalArgumentException("clientSecret must be provided"));
        String renderedHost = runContext.render(this.getHost()).as(String.class).orElseThrow(() -> new IllegalArgumentException("host must be provided"));

        OAuthToken oauthToken = fetchOAuthToken(
            runContext, renderedHost, renderedWorkspaceId, clientId, clientSecret,
            renderedCatalog, renderedSchema, renderedTable
        );
        return new TokenHolder(oauthToken.accessToken, false, oauthToken.expiresIn);
    }

    static OAuthToken fetchOAuthToken(RunContext runContext, String host, String workspaceId, String clientId, String clientSecret, String catalog, String schema, String table)
        throws Exception {
        String cleanHost = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
        String tokenUrl = cleanHost + "/oidc/v1/token";

        List<Map<String, Object>> authDetails = List.of(
            Map.of("type", "unity_catalog_privileges", "privileges", List.of("USE CATALOG"), "object_type", "CATALOG", "object_full_path", catalog),
            Map.of("type", "unity_catalog_privileges", "privileges", List.of("USE SCHEMA"), "object_type", "SCHEMA", "object_full_path", catalog + "." + schema),
            Map.of("type", "unity_catalog_privileges", "privileges", List.of("SELECT", "MODIFY"), "object_type", "TABLE", "object_full_path", catalog + "." + schema + "." + table)
        );

        String authDetailsJson = JacksonMapper.ofJson().writeValueAsString(authDetails);

        Map<String, Object> formParams = Map.of(
            "grant_type", "client_credentials",
            "scope", "all-apis",
            "resource", "api://databricks/workspaces/" + workspaceId + "/zerobusDirectWriteApi",
            "authorization_details", authDetailsJson
        );

        String basicAuth = "Basic " + Base64.getEncoder().encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

        try (
            var httpClient = HttpClient.builder()
                .runContext(runContext)
                .configuration(HttpConfiguration.builder().build())
                .build()
        ) {

            HttpRequest request = HttpRequest.builder()
                .uri(URI.create(tokenUrl))
                .method("POST")
                .body(HttpRequest.UrlEncodedRequestBody.of(formParams))
                .headers(
                    HttpHeaders.of(
                        Map.of(
                            "Authorization", List.of(basicAuth)
                        ), (a, b) -> true
                    )
                )
                .build();

            HttpResponse<String> response = httpClient.request(request, String.class);

            Map<String, Object> body = JacksonMapper.ofJson().readValue(response.getBody(), new TypeReference<>() {
            });
            long expiresIn = 3600;
            if (body.containsKey("expires_in")) {
                Object exp = body.get("expires_in");
                if (exp instanceof Number) {
                    expiresIn = ((Number) exp).longValue();
                } else if (exp instanceof String) {
                    try {
                        expiresIn = Long.parseLong((String) exp);
                    } catch (NumberFormatException ignored) {
                    }
                }
            }

            if (body.containsKey("access_token")) {
                Object tokenObj = body.get("access_token");
                if (tokenObj instanceof String) {
                    return new OAuthToken((String) tokenObj, expiresIn);
                } else {
                    throw new IllegalStateException(
                        "Failed to obtain an OAuth token from " + tokenUrl
                            + " (HTTP 200). access_token in response is not a string."
                    );
                }
            } else {
                throw new IllegalStateException(
                    "Failed to obtain an OAuth token from " + tokenUrl
                        + " (HTTP 200). access_token missing in response."
                );
            }
        } catch (HttpClientResponseException e) {
            int status = e.getResponse() != null ? e.getResponse().getStatus().getCode() : 500;
            String body = readHttpErrorBody(e);
            throw new IllegalStateException(String.format("Failed to obtain an OAuth token from %s (HTTP %d): %s. Check clientId and clientSecret.", tokenUrl, status, truncate(body)));
        }
    }

    static String buildIngestUrl(String endpoint, String workspaceId, String region, String catalog, String schema, String table) {
        String base = endpoint != null ? (endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint)
            : "https://" + workspaceId + ".zerobus." + region + ".cloud.databricks.com";
        return base + "/zerobus/v1/tables/" + encode(catalog) + "." + encode(schema) + "." + encode(table) + "/insert";
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static String readHttpErrorBody(HttpClientResponseException e) {
        if (e.getResponse() != null && e.getResponse().getBody() != null) {
            Object rawBody = e.getResponse().getBody();
            if (rawBody instanceof byte[]) {
                return new String((byte[]) rawBody, StandardCharsets.UTF_8);
            } else {
                return rawBody.toString();
            }
        }
        return "";
    }

    private static String truncate(String body) {
        // Truncate body to avoid overly large exception messages.
        if (body == null)
            return "";
        return body.length() > 500 ? body.substring(0, 500) + "..." : body;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Records count", description = "Total number of records successfully sent to Zerobus Ingest")
        private final long recordsCount;
    }

    static class TokenHolder {
        private String token;
        private final boolean isPat;
        private long expiresAt;

        TokenHolder(String token, boolean isPat, long expiresIn) {
            this.token = token;
            this.isPat = isPat;
            this.expiresAt = isPat ? Long.MAX_VALUE : (System.currentTimeMillis() + expiresIn * 1000);
        }

        boolean needsRefresh() {
            return !isPat && System.currentTimeMillis() >= (expiresAt - TOKEN_REFRESH_MARGIN_SECONDS * 1000);
        }

        void update(String token, long expiresIn) {
            this.token = token;
            this.expiresAt = System.currentTimeMillis() + expiresIn * 1000;
        }

        String getToken() {
            return token;
        }

        boolean isPat() {
            return isPat;
        }
    }

    static class OAuthToken {
        final String accessToken;
        final long expiresIn;

        OAuthToken(String accessToken, long expiresIn) {
            this.accessToken = accessToken;
            this.expiresIn = expiresIn;
        }
    }

    // Ion timestamps are converted to ISO-8601 strings and the class is matched by name to avoid a compile dependency on ion-java.
    private static Object convertIonValues(Object value) {
        if (value instanceof java.util.Map) {
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) value;
            map.replaceAll((k, v) -> convertIonValues(v));
            return map;
        } else if (value instanceof java.util.List) {
            @SuppressWarnings("unchecked")
            java.util.List<Object> list = (java.util.List<Object>) value;
            list.replaceAll(v -> convertIonValues(v));
            return list;
        } else if (value != null && "com.amazon.ion.Timestamp".equals(value.getClass().getName())) {
            return value.toString();
        }
        return value;
    }
}
