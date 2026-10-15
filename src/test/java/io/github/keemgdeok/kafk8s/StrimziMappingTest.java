package io.github.keemgdeok.kafk8s;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StrimziMappingTest {
    private JsonObject kafka;
    private List<JsonObject> pools;
    private List<JsonObject> pods;
    private List<JsonObject> sets;
    private String clusterId;

    @BeforeEach void load() throws Exception {
        String run = "20261014T143858Z-colocated";
        kafka = Fixtures.json(run, "before-kafka.json");
        pools = Fixtures.objects(Fixtures.json(run, "before-pools.json"));
        pods = Fixtures.objects(Fixtures.json(run, "before-pods.json"));
        sets = Fixtures.objects(Fixtures.json(run, "before-podsets.json"));
        clusterId = kafka.getAsJsonObject("status").get("clusterId").getAsString();
    }

    private StrimziMapping.Mapping resolve() {
        return StrimziMapping.resolve("demo", clusterId, kafka, pools, pods, sets);
    }

    private JsonObject pod3() {
        return pods.stream().filter(p -> p.getAsJsonObject("metadata").get("name").getAsString().equals("demo-b3-3")).findFirst().orElseThrow();
    }

    @Test void nonContiguousIdsAndControllersAreMappedCorrectly() {
        var mapping = resolve();
        assertNull(mapping.problem());
        assertEquals(Set.of(3, 7, 11), mapping.placements().keySet());
        assertTrue(mapping.placements().values().stream().allMatch(p -> p.problem() == null));
        assertEquals(mapping.placements().get(3).node(), mapping.placements().get(7).node());
    }

    @Test void wrongClusterIdIsNotAValidMapping() {
        kafka.getAsJsonObject("status").addProperty("clusterId", "other");
        assertEquals("CLUSTER_MISMATCH", assertThrows(Failure.class, this::resolve).reasonCode);
    }

    @Test void wrongOwnerUidInvalidatesOnlyThatMapping() {
        pod3().getAsJsonObject("metadata").getAsJsonArray("ownerReferences").get(0).getAsJsonObject().addProperty("uid", "other");
        var mapping = resolve();
        assertEquals("MAPPING_CONFLICT", mapping.placements().get(3).problem());
        assertNull(mapping.placements().get(11).problem());
    }

    @Test void missingPodCannotBeInventedFromName() {
        JsonObject removed = pod3();
        pods = pods.stream().filter(p -> p != removed).toList();
        assertEquals("MAPPING_MISSING", resolve().placements().get(3).problem());
    }

    @Test void notReadyIsNotTheSameAsNotInIsr() {
        JsonArray conditions = pod3().getAsJsonObject("status").getAsJsonArray("conditions");
        for (var c : conditions) {
            if (c.getAsJsonObject().get("type").getAsString().equals("Ready")) {
                c.getAsJsonObject().addProperty("status", "False");
            }
        }
        assertNull(resolve().placements().get(3).problem());
    }

    @Test void unassignedPodIsIncomplete() {
        pod3().getAsJsonObject("spec").remove("nodeName");
        assertNotNull(resolve().placements().get(3).problem());
    }

    @Test void missingPodSetChainIsRejected() {
        sets = List.of();
        assertTrue(resolve().placements().values().stream().allMatch(p -> p.problem() != null));
    }

    @Test void stalePoolGenerationDoesNotBecomeHealthy() {
        JsonObject pool = pools.stream().filter(p -> p.getAsJsonObject("metadata").get("name").getAsString().equals("b3")).findFirst().orElseThrow();
        pool.getAsJsonObject("status").addProperty("observedGeneration", 0);
        assertEquals("INCONSISTENT_EVIDENCE", resolve().placements().get(3).problem());
        assertNull(resolve().placements().get(7).problem());
    }

    @Test void unsupportedOperatorIsExplicit() {
        kafka.getAsJsonObject("status").addProperty("operatorLastSuccessfulVersion", "0.0.0");
        assertEquals("UNSUPPORTED_PROFILE", resolve().problem());
    }

    @Test void conflictingBrokerIdIsRejected() {
        JsonObject pool = pools.stream().filter(p -> p.getAsJsonObject("metadata").get("name").getAsString().equals("b7")).findFirst().orElseThrow();
        JsonArray ids = new JsonArray();
        ids.add(3);
        pool.getAsJsonObject("status").add("nodeIds", ids);
        assertEquals("MAPPING_CONFLICT", resolve().placements().get(3).problem());
    }
}
