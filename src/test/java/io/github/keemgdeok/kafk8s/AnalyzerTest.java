package io.github.keemgdeok.kafk8s;

import static io.github.keemgdeok.kafk8s.Evidence.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AnalyzerTest {
    private static final String SPREAD = "20261014T143455Z-spread";
    private static final String COLOCATED = "20261014T143858Z-colocated";

    private static Snapshot sample(List<Integer> isr, int min, boolean colocated) {
        Map<Integer, Placement> placements = Map.of(
                3, new Placement(3, "b3", "p3", "a", "pool3", 1, null),
                7, new Placement(7, "b7", "p7", colocated ? "a" : "b", "pool7", 1, null),
                11, new Placement(11, "b11", "p11", "c", "pool11", 1, null));
        return new Snapshot("id", "uid", 1, null, placements,
                List.of(new Partition("events", "tid", 0, isr.getFirst(), List.of(3, 7, 11), isr, min, "DYNAMIC_TOPIC_CONFIG", null)), Map.of());
    }

    private static Snapshot with(Snapshot s, Map<Integer, Placement> placements, List<Partition> partitions) {
        return new Snapshot(s.clusterId(), s.kafkaUid(), s.kafkaGeneration(), s.profileProblem(), placements, partitions, s.features());
    }

    @Test void realSpreadLeavesTwo() throws Exception {
        Snapshot s = Fixtures.snapshot(SPREAD, "before");
        assertNull(s.profileProblem());
        assertEquals(3, s.placements().size());
        assertTrue(Analyzer.analyze(s, s, "kafk8s-lab-worker").stream()
                .allMatch(r -> r.status().equals("NO_SHORTFALL") && r.remainingIsr() == 2));
    }

    @Test void realColocatedLeavesOne() throws Exception {
        Snapshot s = Fixtures.snapshot(COLOCATED, "before");
        assertNull(s.profileProblem());
        var rows = Analyzer.analyze(s, s, "kafk8s-lab-worker");
        assertEquals(1, Analyzer.exitCode(rows));
        assertTrue(rows.stream().allMatch(r -> r.status().equals("RISK") && r.remainingIsr() == 1));
    }

    @Test void colocatedWithMinOneIsNotAutomaticallyRisky() {
        Snapshot s = sample(List.of(3, 7, 11), 1, true);
        assertEquals("NO_SHORTFALL", Analyzer.analyze(s, s, "a").getFirst().status());
    }

    @Test void spreadCanStillHaveInsufficientIsrBudget() {
        Snapshot s = sample(List.of(3, 7), 2, false);
        assertEquals("RISK", Analyzer.analyze(s, s, "a").getFirst().status());
    }

    @Test void alreadyShortIsDistinguished() {
        Snapshot s = sample(List.of(11), 2, false);
        assertEquals("ALREADY_BELOW_MIN_ISR", Analyzer.analyze(s, s, "a").getFirst().reasonCode());
    }

    @Test void nonIsrReplicaIsNotSubtracted() {
        Snapshot s = sample(List.of(7, 11), 2, false);
        assertEquals(2, Analyzer.analyze(s, s, "a").getFirst().remainingIsr());
    }

    @Test void missingIsrMappingIsUnknown() {
        Snapshot s = sample(List.of(3, 7, 11), 2, false);
        Map<Integer, Placement> map = new HashMap<>(s.placements());
        map.remove(7);
        Snapshot incomplete = with(s, map, s.partitions());
        var rows = Analyzer.analyze(incomplete, incomplete, "a");
        assertEquals("MAPPING_MISSING", rows.getFirst().reasonCode());
        assertNull(rows.getFirst().remainingIsr());
        assertEquals(3, Analyzer.exitCode(rows));
    }

    @Test void changedPodUidIsNotStableEvidence() {
        Snapshot before = sample(List.of(3, 7, 11), 2, false);
        Map<Integer, Placement> map = new HashMap<>(before.placements());
        map.put(3, new Placement(3, "b3", "replacement", "a", "pool3", 1, null));
        assertEquals("OBSERVATION_CHANGED", Analyzer.analyze(before, with(before, map, before.partitions()), "a").getFirst().reasonCode());
    }

    @Test void recreatedTopicIsNotStableEvidence() {
        Snapshot s = sample(List.of(3, 7, 11), 2, false);
        Partition p = s.partitions().getFirst();
        Partition replacement = new Partition(p.topic(), "new-id", p.partition(), p.leader(), p.replicas(), p.isr(), p.minIsr(), p.configSource(), null);
        assertEquals("OBSERVATION_CHANGED", Analyzer.analyze(s, with(s, s.placements(), List.of(replacement)), "a").getFirst().reasonCode());
    }

    @Test void duplicateIsrCannotProducePass() {
        Snapshot s = sample(List.of(3, 3, 7), 2, false);
        assertEquals("INCONSISTENT_EVIDENCE", Analyzer.analyze(s, s, "a").getFirst().reasonCode());
    }

    @Test void unrelatedConfigFailureDoesNotPoisonOtherTopic() {
        Snapshot s = sample(List.of(3, 7, 11), 2, true);
        var unknown = new Partition("denied", "tid2", 0, 3, List.of(3, 7, 11), List.of(3, 7, 11), null, null, "CONFIG_UNAVAILABLE");
        Snapshot mixed = with(s, s.placements(), List.of(s.partitions().getFirst(), unknown));
        var rows = Analyzer.analyze(mixed, mixed, "a");
        assertEquals("RISK", rows.getFirst().status());
        assertEquals("UNKNOWN", rows.getLast().status());
        assertEquals(1, Analyzer.exitCode(rows));
    }

    @Test void inspectIsNotRiskEvaluation() {
        Snapshot s = sample(List.of(3, 7, 11), 2, true);
        var rows = Analyzer.analyze(s, s, null);
        assertEquals("OBSERVED", rows.getFirst().status());
        assertEquals(0, Analyzer.exitCode(rows));
    }

    @Test void clusterMismatchIsCommandError() {
        Snapshot s = sample(List.of(3, 7, 11), 2, true);
        Snapshot other = new Snapshot("other", s.kafkaUid(), 1, null, s.placements(), s.partitions(), Map.of());
        assertEquals("CLUSTER_MISMATCH", assertThrows(Failure.class, () -> Analyzer.analyze(s, other, "a")).reasonCode);
    }

    @Test void emptyMetadataCannotSucceed() {
        Snapshot s = sample(List.of(3, 7, 11), 2, true);
        Snapshot empty = with(s, s.placements(), List.of());
        assertThrows(Failure.class, () -> Analyzer.analyze(empty, empty, "a"));
    }
}
