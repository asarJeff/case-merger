package com.asar.casemerge.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Component
public class CaseApiClient {

    private static final Logger log = LoggerFactory.getLogger(CaseApiClient.class);

    // FIX: How many times to retry a 429 before giving up for this cycle
    private static final int MAX_RETRIES = 3;

    // FIX: How long to wait on first 429 before retrying (doubles each attempt)
    private static final long INITIAL_BACKOFF_MS = 10_000; // 10 seconds

    private final WebClient client;

    public CaseApiClient(org.springframework.core.env.Environment env) {
        String baseUrl = must(env.getProperty("case-api.base-url"));
        String user = must(env.getProperty("case-api.username"));
        String pass = must(env.getProperty("case-api.password"));

        this.client = WebClient.builder()
                .baseUrl(baseUrl)
                .defaultHeaders(h -> h.setBasicAuth(user, pass))
                .build();
    }

    /**
     * FIX: getCases() now handles 429 responses gracefully with exponential backoff.
     * Previously a 429 threw an unhandled exception that crashed the entire scheduler cycle.
     * Now it retries up to MAX_RETRIES times with increasing wait times before giving up.
     * If all retries are exhausted it returns null, which CaseMergeService treats as
     * "no more pages" and exits the pagination loop cleanly — no crash, no wasted calls.
     */
    public JsonNode getCases(int top, int skip) {
        String uri = "/sap/c4c/api/v1/case-service/cases"
                + "?$top=" + top
                + "&$skip=" + skip
                + "&$select=id,displayId,status,statusSchema,lifeCycleStatus,extensions,account,individualCustomer,parentCaseId,parentCaseDisplayId";

        long backoffMs = INITIAL_BACKOFF_MS;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                log.debug("GET {} (attempt {}/{})", uri, attempt, MAX_RETRIES);

                return client.get()
                        .uri(uri)
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .block();

            } catch (WebClientResponseException.TooManyRequests e) {
                // FIX: Catch 429 specifically — do NOT crash, just wait and retry
                if (attempt == MAX_RETRIES) {
                    log.warn("GET /cases hit 429 after {} attempts — skipping this cycle to preserve quota", MAX_RETRIES);
                    return null; // CaseMergeService null-checks this and exits pagination cleanly
                }

                log.warn("GET /cases hit 429 (attempt {}/{}) — waiting {}ms before retry",
                        attempt, MAX_RETRIES, backoffMs);

                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return null;
                }

                backoffMs *= 2; // Exponential backoff: 10s → 20s → 40s

            } catch (WebClientResponseException e) {
                // Non-429 errors (400, 401, 500 etc) — log and return null, don't crash
                log.error("GET /cases failed status={} body={}", e.getStatusCode(), e.getResponseBodyAsString());
                return null;
            }
        }

        return null;
    }

    /** Simple GET body — unchanged */
    public JsonNode getCaseById(String caseId) {
        return client.get()
                .uri("/sap/c4c/api/v1/case-service/cases/{id}", caseId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();
    }

    /** GET case + headers (so we can read ETag) — unchanged */
    public ResponseEntity<JsonNode> getCaseByIdWithHeaders(String caseId) {
        return client.get()
                .uri("/sap/c4c/api/v1/case-service/cases/{id}", caseId)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toEntity(JsonNode.class)
                .block();
    }

    /** PATCH case using ETag via If-Match — unchanged */
    public JsonNode patchCaseWithEtag(String caseId, JsonNode patchBody) {
        ResponseEntity<JsonNode> getResp = getCaseByIdWithHeaders(caseId);

        String etag = getResp.getHeaders().getETag();
        if (etag == null || etag.isBlank()) {
            throw new RuntimeException("No ETag returned by GET for case " + caseId + ". Cannot PATCH (428).");
        }

        return client.patch()
                .uri("/sap/c4c/api/v1/case-service/cases/{id}", caseId)
                .header("If-Match", etag)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_JSON)
                .bodyValue(patchBody)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .block();
    }

    private static String must(String v) {
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing required config value");
        return v;
    }
}