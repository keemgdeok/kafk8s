package io.github.keemgdeok.kafk8s;

import static io.github.keemgdeok.kafk8s.Evidence.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public final class Analyzer {
    private Analyzer() {}

    public static List<Row> analyze(Snapshot before, Snapshot after, String lostNode) {
        if (after.partitions().isEmpty()) {
            throw new Failure("INCONSISTENT_EVIDENCE");
        }
        if (!before.clusterId().equals(after.clusterId()) || !before.kafkaUid().equals(after.kafkaUid())) {
            throw new Failure("CLUSTER_MISMATCH");
        }
        List<Row> rows = new ArrayList<>();
        for (Partition p : after.partitions()) {
            Partition previous = before.partitions().stream()
                    .filter(v -> v.topic().equals(p.topic()) && v.partition() == p.partition())
                    .findFirst().orElse(null);
            String problem = after.profileProblem();
            if (problem == null) {
                problem = p.problem();
            }
            if (problem == null && (p.replicas().isEmpty() || p.isr().isEmpty()
                    || new HashSet<>(p.replicas()).size() != p.replicas().size()
                    || new HashSet<>(p.isr()).size() != p.isr().size()
                    || !p.replicas().containsAll(p.isr()) || !p.replicas().contains(p.leader())
                    || !p.isr().contains(p.leader()) || p.minIsr() == null || p.minIsr() < 1)) {
                problem = "INCONSISTENT_EVIDENCE";
            }
            boolean changed = !p.equals(previous)
                    || before.kafkaGeneration() != after.kafkaGeneration()
                    || !Objects.equals(before.profileProblem(), after.profileProblem())
                    || !before.features().equals(after.features());
            List<Placement> placements = new ArrayList<>();
            List<Integer> lost = new ArrayList<>();
            for (int id : p.replicas()) {
                Placement placement = after.placements().get(id);
                if (placement != null) {
                    placements.add(placement);
                }
                if (p.isr().contains(id)) {
                    if (placement == null && problem == null) {
                        problem = "MAPPING_MISSING";
                    } else if (placement != null) {
                        if (placement.problem() != null && problem == null) {
                            problem = placement.problem();
                        }
                        changed |= !placement.equals(before.placements().get(id));
                        if (Objects.equals(placement.node(), lostNode)) {
                            lost.add(id);
                        }
                    }
                }
            }
            if (changed) {
                problem = "OBSERVATION_CHANGED";
            }
            Integer remaining = problem == null ? p.isr().size() - lost.size() : null;
            String status;
            String reason;
            if (problem != null) {
                status = "UNKNOWN";
                reason = problem;
            } else if (lostNode == null) {
                status = "OBSERVED";
                reason = "PLACEMENT_OBSERVED";
            } else if (p.isr().size() < p.minIsr()) {
                status = "RISK";
                reason = "ALREADY_BELOW_MIN_ISR";
            } else if (remaining < p.minIsr()) {
                status = "RISK";
                reason = "NODE_LOSS_BELOW_MIN_ISR";
            } else {
                status = "NO_SHORTFALL";
                reason = "MIN_ISR_BUDGET_MET";
            }
            rows.add(new Row(p.topic(), p.topicId(), p.partition(), status, reason, p.leader(), p.replicas(),
                    p.isr(), p.minIsr(), p.configSource(), List.copyOf(lost), remaining, List.copyOf(placements)));
        }
        if (before.partitions().size() != after.partitions().size()) {
            return rows.stream().map(r -> new Row(r.topic(), r.topicId(), r.partition(), "UNKNOWN", "OBSERVATION_CHANGED",
                    r.leader(), r.replicas(), r.isr(), r.minIsr(), r.configSource(), r.lostIsr(), null, r.placements())).toList();
        }
        return List.copyOf(rows);
    }

    public static int exitCode(List<Row> rows) {
        if (rows.stream().anyMatch(r -> r.status().equals("RISK"))) {
            return 1;
        }
        return rows.stream().anyMatch(r -> r.status().equals("UNKNOWN")) ? 3 : 0;
    }
}
