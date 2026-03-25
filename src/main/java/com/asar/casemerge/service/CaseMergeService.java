package com.asar.casemerge.service;

import com.asar.casemerge.api.CaseApiClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class CaseMergeService {

    private final CaseApiClient api;
    private final ObjectMapper om;

    @Value("${merge.enabled:true}")
    private boolean enabled;

    @Value("${merge.page-size:200}")
    private int pageSize;

    @Value("${merge.max-pages:50}")
    private int maxPages;

    @Value("${merge.tracking-field:TrackingNumber}")
    private String trackingField;

    @Value("${merge.duplicate-case-field:DuplicateCase}")
    private String duplicateCaseField;

    @Value("${merge.require-individual-customer:true}")
    private boolean requireIndividualCustomer;

    // Keep true: do not merge/close account-based cases as children.
    @Value("${merge.exclude-account:true}")
    private boolean excludeAccount;

    // Relationship meta (you observed type=2886, role 14/13 in different directions)
    @Value("${merge.related-case-type:2886}")
    private String relatedCaseType;

    // Kept for reference (older approach: parent -> child relatedObjects)
    @Value("${merge.related-case-role:14}")
    private String relatedCaseRole;

    // This matches your GET sample for child -> parent link (role=13).
    @Value("${merge.parent-link-role:13}")
    private String parentLinkRole;

    @Value("${merge.status-schema:Z2}")
    private String statusSchema;

    // Status values:
    // Z2 = New, Z3 = Open (active)
    // Z5 = Solved (Completed) — terminal for Claims (Z2 schema)
    // 06 = Closed — terminal for Operations (Z3 schema)
    @Value("${merge.status-closed:Z5}")
    private String statusClosed;

    @Value("${merge.status-completed:Z5}")
    private String statusCompleted;

    @Value("${merge.lifecycle-open:OPEN}")
    private String lifecycleOpen;

    @Value("${merge.lifecycle-completed:COMPLETED}")
    private String lifecycleCompleted;

    // Per-schema terminal status map.
    // Z2 (Claims) resolves at Z5 — never uses 06.
    // Z3 (Operations) closes at 06.
    @Value("#{${merge.schema-closed-status:{}}}")
    private Map<String, String> schemaClosedStatusMap;

    public CaseMergeService(CaseApiClient api, ObjectMapper om) {
        this.api = api;
        this.om = om;
    }

    // IMPORTANT: no SpEL wrapper; just read the property string.
    @Scheduled(fixedDelayString = "${merge.fixed-delay-ms:300000}")
    public void scheduled() {
        if (!enabled) return;
        processOnce();
    }

    public void processOnce() {
        long start = System.currentTimeMillis();
        System.out.println("==================================================");
        System.out.println("Case Merger - processOnce START");
        System.out.println("==================================================");

        List<JsonNode> candidates = new ArrayList<>();

        int merged = 0;
        int skippedNoValidRoot = 0;
        int skippedParentIsSubcase = 0;
        int skippedChildAlreadyHasParent = 0;
        int skippedNotIndividual = 0;
        int skippedAccountChild = 0;
        int skippedInactiveOrg = 0;
        int skippedInvalidExtension = 0;
        int skippedNotActiveStatus = 0;
        int failed = 0;

        // Active statuses (customer-defined):
        // Z2 = New, Z3 = Open
        Set<String> activeStatuses = Set.of("Z2", "Z3");

        // -------------------------------
        // Step 1: Collect Candidate Cases
        // -------------------------------
        for (int page = 0; page < maxPages; page++) {
            int skip = page * pageSize;

            System.out.printf("Fetching cases page=%d skip=%d top=%d%n", page, skip, pageSize);

            JsonNode root = api.getCases(pageSize, skip);
            JsonNode value = (root == null) ? null : root.get("value");
            if (value == null || !value.isArray() || value.size() == 0) {
                System.out.println("No more cases returned, stopping pagination.");
                break;
            }

            for (JsonNode c : value) {
                // Tracking number required
                String tracking = text(c.at("/extensions/" + trackingField));
                if (tracking == null || tracking.isBlank()) continue;

                // Only active statuses (status, not statusCode)
                String status = text(c.get("status"));
                if (status == null || !activeStatuses.contains(status)) {
                    skippedNotActiveStatus++;
                    continue;
                }

                // IMPORTANT: do not filter account cases here.
                // We need them present so they can become the parent/root.
                candidates.add(c);
            }
        }

        System.out.printf("Candidates collected: %d (skipped not active status: %d)%n",
                candidates.size(), skippedNotActiveStatus);

        // ---------------------------------------
        // Step 2: Group By Tracking Number
        // ---------------------------------------
        Map<String, List<JsonNode>> byTracking =
                candidates.stream()
                        .collect(Collectors.groupingBy(c -> text(c.at("/extensions/" + trackingField))));

        System.out.printf("Tracking groups found: %d%n", byTracking.size());

        // ---------------------------------------
        // Step 3: Process Each Duplicate Group
        // ---------------------------------------
        for (Map.Entry<String, List<JsonNode>> entry : byTracking.entrySet()) {
            String tracking = entry.getKey();
            List<JsonNode> group = entry.getValue();
            if (group.size() < 2) continue;

            System.out.printf("[Tracking=%s] Duplicate group size=%d%n", tracking, group.size());

            JsonNode main = selectRootParent(group); // account-preferred, and MUST NOT be a subcase
            if (main == null) {
                skippedNoValidRoot++;
                System.out.printf("[Tracking=%s] SKIP group : no valid ROOT parent found.%n", tracking);
                continue;
            }

            String parentId = text(main.get("id"));
            String parentDisplay = text(main.get("displayId"));

            // Root cannot be a subcase (avoids case.10174)
            if (isSubcase(main)) {
                skippedParentIsSubcase++;
                System.out.printf("[Tracking=%s] SKIP group : chosen parent %s is already a subcase.%n",
                        tracking, parentDisplay);
                continue;
            }

            List<JsonNode> children = group.stream()
                    .filter(c -> !Objects.equals(text(c.get("id")), parentId))
                    .toList();

            System.out.printf("[Tracking=%s] Parent=%s (%s). Children=%d%n",
                    tracking, parentDisplay, parentId, children.size());

            for (JsonNode child : children) {
                String childId = text(child.get("id"));
                String childDisplay = text(child.get("displayId"));

                // Only merge/close Individual Customer cases
                if (requireIndividualCustomer) {
                    String indId = text(child.at("/individualCustomer/id"));
                    if (indId == null || indId.isBlank()) {
                        skippedNotIndividual++;
                        System.out.printf("[Tracking=%s] SKIP child=%s : not Individual Customer.%n",
                                tracking, childDisplay);
                        continue;
                    }
                }

                // Exclude account cases as children
                if (excludeAccount) {
                    String acctId = text(child.at("/account/id"));
                    if (acctId != null && !acctId.isBlank()) {
                        skippedAccountChild++;
                        System.out.printf("[Tracking=%s] SKIP child=%s : account-based case (not merged).%n",
                                tracking, childDisplay);
                        continue;
                    }
                }

                // If child already has a parent, don't touch
                if (isSubcase(child)) {
                    skippedChildAlreadyHasParent++;
                    System.out.printf("[Tracking=%s] SKIP child=%s : already has parentCaseId=%s.%n",
                            tracking, childDisplay, text(child.get("parentCaseDisplayId")));
                    continue;
                }

                try {
                    System.out.printf("[Tracking=%s] Linking child %s -> parent %s...%n",
                            tracking, childDisplay, parentDisplay);

                    // ✅ link by PATCHing the CHILD (parentCaseId)
                    linkChildToParent(parentId, parentDisplay, childId);

                    System.out.printf("[Tracking=%s] Linked child %s -> parent %s.%n",
                            tracking, childDisplay, parentDisplay);

                    // ✅ Flag as duplicate BEFORE closing so auto-flow suppresses email
                    markAsDuplicate(childId);

                    // Close/resolve only after link
                    closeCase(childId);

                    System.out.printf("[Tracking=%s] Resolved/closed case %s.%n",
                            tracking, childDisplay);

                    merged++;

                } catch (Exception ex) {
                    String msg = ex.getMessage();

                    // Only one level hierarchy
                    if (msg != null && msg.contains("case.10174")) {
                        skippedParentIsSubcase++;
                        System.out.printf("[Tracking=%s] SKIP child=%s : hierarchy depth issue (10174).%n",
                                tracking, childDisplay);
                        continue;
                    }

                    // Multiple parents
                    if (msg != null && msg.contains("case.10175")) {
                        skippedChildAlreadyHasParent++;
                        System.out.printf("[Tracking=%s] SKIP child=%s : already has a parent (10175).%n",
                                tracking, childDisplay);
                        continue;
                    }

                    // Party validation error
                    if (msg != null && msg.contains("case.10066")) {
                        skippedInactiveOrg++;
                        System.out.printf("[Tracking=%s] SKIP child=%s : inactive/misconfigured service team (10066).%n",
                                tracking, childDisplay);
                        continue;
                    }

                    // Invalid extension enum
                    if (msg != null && msg.contains("case.10147")) {
                        skippedInvalidExtension++;
                        System.out.printf("[Tracking=%s] SKIP child=%s : invalid extension data (10147).%n",
                                tracking, childDisplay);
                        continue;
                    }

                    failed++;
                    System.err.printf("[Tracking=%s] FAILED child=%s parent=%s : %s%n",
                            tracking, childDisplay, parentDisplay, msg);
                }
            }
        }

        // ---------------------------------------
        // Final Run Summary
        // ---------------------------------------
        long ms = System.currentTimeMillis() - start;

        System.out.println("--------------------------------------------------");
        System.out.println("Case Merge Run Summary");
        System.out.println("--------------------------------------------------");
        System.out.printf("Merged (linked+resolved): %d%n", merged);
        System.out.printf("Skipped - Not Active Status (not Z2/Z3): %d%n", skippedNotActiveStatus);
        System.out.printf("Skipped - No Valid Root Parent: %d%n", skippedNoValidRoot);
        System.out.printf("Skipped - Parent Is Subcase: %d%n", skippedParentIsSubcase);
        System.out.printf("Skipped - Child Already Has Parent: %d%n", skippedChildAlreadyHasParent);
        System.out.printf("Skipped - Not Individual: %d%n", skippedNotIndividual);
        System.out.printf("Skipped - Account Child: %d%n", skippedAccountChild);
        System.out.printf("Skipped - Inactive Org (10066): %d%n", skippedInactiveOrg);
        System.out.printf("Skipped - Invalid Extension (10147): %d%n", skippedInvalidExtension);
        System.out.printf("Failures: %d%n", failed);
        System.out.printf("Elapsed: %d ms%n", ms);
        System.out.println("--------------------------------------------------");
    }

    /**
     * Root selection (safe):
     * - Prefer an ACCOUNT case as the parent/root if one exists
     * - Root MUST NOT be a subcase (parentCaseId must be null)
     * - Otherwise pick oldest non-subcase
     */
    private JsonNode selectRootParent(List<JsonNode> group) {

        // Prefer account cases that are NOT subcases
        List<JsonNode> accountRoots = group.stream()
                .filter(c -> {
                    String acctId = text(c.at("/account/id"));
                    return acctId != null && !acctId.isBlank();
                })
                .filter(c -> !isSubcase(c))
                .toList();

        if (!accountRoots.isEmpty()) {
            return accountRoots.stream()
                    .min(Comparator.comparing(this::createdOnSafe))
                    .orElse(accountRoots.get(0));
        }

        // Otherwise: any non-subcase, oldest
        List<JsonNode> nonSubcases = group.stream()
                .filter(c -> !isSubcase(c))
                .toList();

        if (nonSubcases.isEmpty()) return null;

        return nonSubcases.stream()
                .min(Comparator.comparing(this::createdOnSafe))
                .orElse(nonSubcases.get(0));
    }

    private boolean isSubcase(JsonNode c) {
        String parentId = text(c.get("parentCaseId"));
        return parentId != null && !parentId.isBlank();
    }

    private Instant createdOnSafe(JsonNode c) {
        String s = text(c.at("/adminData/createdOn"));
        if (s == null || s.isBlank()) return Instant.MAX;
        try { return Instant.parse(s); } catch (Exception e) { return Instant.MAX; }
    }

    /**
     * ✅ Correct hierarchy link for UI:
     * PATCH CHILD to set parentCaseId/parentCaseDisplayId.
     * Optionally also set the reverse relatedObjects role=13 (matches your GET sample).
     */
    private void linkChildToParent(String parentCaseId, String parentCaseDisplayId, String childCaseId) throws Exception {

        // Read child to keep/merge existing relatedObjects safely
        JsonNode child = api.getCaseById(childCaseId);

        // If it already has a parent, do nothing (idempotent)
        String existingParent = text(child.get("parentCaseId"));
        if (existingParent != null && !existingParent.isBlank()) return;

        ObjectNode patch = om.createObjectNode();
        patch.put("parentCaseId", parentCaseId);
        patch.put("parentCaseDisplayId", parentCaseDisplayId);

        // Merge relatedObjects (optional but matches your tenant's GET pattern)
        ArrayNode related = (child != null && child.has("relatedObjects") && child.get("relatedObjects").isArray())
                ? (ArrayNode) child.get("relatedObjects")
                : om.createArrayNode();

        boolean alreadyHasParentRel = false;
        for (JsonNode ro : related) {
            if (Objects.equals(text(ro.get("objectId")), parentCaseId)
                    && Objects.equals(text(ro.get("type")), relatedCaseType)
                    && Objects.equals(text(ro.get("role")), parentLinkRole)) {
                alreadyHasParentRel = true;
                break;
            }
        }

        if (!alreadyHasParentRel) {
            ObjectNode parentRel = om.createObjectNode();
            parentRel.put("objectId", parentCaseId);
            parentRel.put("objectDisplayId", parentCaseDisplayId);
            parentRel.put("type", relatedCaseType);
            parentRel.put("role", parentLinkRole);

            ArrayNode updated = om.createArrayNode();
            related.forEach(updated::add);
            updated.add(parentRel);

            patch.set("relatedObjects", updated);
        }

        api.patchCaseWithEtag(childCaseId, patch);
    }

    /**
     * Sets the DuplicateCase indicator field to true on the given case
     * so that the SAP Service Cloud auto-flow can suppress the closure email.
     * Must be called BEFORE closeCase().
     */
    private void markAsDuplicate(String caseId) throws Exception {
        ObjectNode extensions = om.createObjectNode();
        extensions.put(duplicateCaseField, true);

        ObjectNode patch = om.createObjectNode();
        patch.set("extensions", extensions);

        api.patchCaseWithEtag(caseId, patch);
        System.out.printf("[markAsDuplicate] Set %s=true on case %s%n", duplicateCaseField, caseId);
    }

    /**
     * Resolves or closes a child case based on its status schema.
     *
     * Z2 (Claims) — terminal state is Z5 (Solved/Completed). Never transitions to 06.
     * Z3 (Operations) — terminal state is 06 (Closed).
     *
     * The per-schema terminal status is driven by merge.schema-closed-status in application.yml,
     * falling back to merge.status-closed if the schema is not mapped.
     */
    private void closeCase(String caseId) throws Exception {
        JsonNode current = api.getCaseById(caseId);

        String currentStatus = text(current.get("status"));
        String currentSchema = text(current.get("statusSchema"));

        String schemaToUse = (currentSchema == null || currentSchema.isBlank()) ? statusSchema : currentSchema;

        // Resolve the correct terminal status for this schema.
        // e.g. Z2 -> Z5 (Solved), Z3 -> 06 (Closed)
        String terminalStatus = schemaClosedStatusMap.getOrDefault(schemaToUse, statusClosed);

        System.out.printf("[closeCase] caseId=%s schema=%s terminalStatus=%s currentStatus=%s%n",
                caseId, schemaToUse, terminalStatus, currentStatus);

        // Already at terminal status — nothing to do
        if (terminalStatus.equals(currentStatus)) {
            System.out.printf("[closeCase] caseId=%s already at terminal status %s, skipping.%n",
                    caseId, terminalStatus);
            return;
        }

        // Step 1: Transition to Z5 (Solved/Completed) if not already there
        if (!statusCompleted.equals(currentStatus)) {
            ObjectNode toCompleted = om.createObjectNode();
            toCompleted.put("statusSchema", schemaToUse);
            toCompleted.put("status", statusCompleted);         // Z5
            toCompleted.put("lifeCycleStatus", lifecycleCompleted); // COMPLETED
            api.patchCaseWithEtag(caseId, toCompleted);
            System.out.printf("[closeCase] caseId=%s transitioned to %s (Solved).%n", caseId, statusCompleted);
        }

        // Step 2: For schemas that support 06 (e.g. Z3), push to Closed.
        // For Z2 (Claims), Z5 is the terminal state — skip this step.
        if (!statusCompleted.equals(terminalStatus)) {
            ObjectNode toClosed = om.createObjectNode();
            toClosed.put("statusSchema", schemaToUse);
            toClosed.put("status", terminalStatus);             // 06
            toClosed.put("lifeCycleStatus", lifecycleCompleted);
            api.patchCaseWithEtag(caseId, toClosed);
            System.out.printf("[closeCase] caseId=%s transitioned to %s (Closed).%n", caseId, terminalStatus);
        }
    }

    private static String text(JsonNode n) {
        if (n == null || n.isMissingNode() || n.isNull()) return null;
        String v = n.asText();
        return (v == null) ? null : v.trim();
    }

    // Quick manual proof helper
    public void proofLinkAndClose(String parentCaseId, String parentDisplayId, String childCaseId) throws Exception {
        linkChildToParent(parentCaseId, parentDisplayId, childCaseId);
        closeCase(childCaseId);
    }
}