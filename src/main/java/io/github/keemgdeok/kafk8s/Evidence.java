package io.github.keemgdeok.kafk8s;

import java.util.List;
import java.util.Map;

public final class Evidence {
    private Evidence() {}

    public record Placement(int brokerId, String pod, String podUid, String node,
                            String poolUid, long poolGeneration, String problem) {}

    public record Partition(String topic, String topicId, int partition, int leader, List<Integer> replicas,
                            List<Integer> isr, Integer minIsr, String configSource, String problem) {
        public Partition {
            replicas = List.copyOf(replicas);
            isr = List.copyOf(isr);
        }
    }

    public record Snapshot(String clusterId, String kafkaUid, long kafkaGeneration,
                           String profileProblem, Map<Integer, Placement> placements,
                           List<Partition> partitions, Map<String, Short> features) {
        public Snapshot {
            placements = Map.copyOf(placements);
            partitions = List.copyOf(partitions);
            features = Map.copyOf(features);
        }
    }

    public record Row(String topic, String topicId, int partition, String status, String reasonCode,
                      int leader, List<Integer> replicas, List<Integer> isr,
                      Integer minIsr, String configSource, List<Integer> lostIsr,
                      Integer remainingIsr, List<Placement> placements) {}
}
