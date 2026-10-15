package io.github.keemgdeok.kafk8s;

import static io.github.keemgdeok.kafk8s.Evidence.*;

import com.google.gson.JsonObject;
import io.kubernetes.client.openapi.ApiClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeFeaturesOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;

public final class Collector implements AutoCloseable {
    private final Admin admin;
    private final ApiClient kube;
    private final long deadline;
    private final String namespace;
    private final String cluster;
    private final List<String> topics;
    private final KubernetesReader kubernetes;

    public Collector(Admin admin, ApiClient kube, long deadline, String namespace, String cluster, List<String> topics) {
        this.admin = admin;
        this.kube = kube;
        this.deadline = deadline;
        this.namespace = namespace;
        this.cluster = cluster;
        this.topics = List.copyOf(topics);
        this.kubernetes = new KubernetesReader(kube, deadline, namespace, cluster);
    }

    public Snapshot collect() throws Exception {
        String clusterId = await(admin.describeCluster(new DescribeClusterOptions().timeoutMs(remaining())).clusterId());
        var descriptions = await(admin.describeTopics(topics, new DescribeTopicsOptions().timeoutMs(remaining())).allTopicNames());
        List<ConfigResource> resources = topics.stream().map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).toList();
        var configFutures = admin.describeConfigs(resources, new DescribeConfigsOptions().timeoutMs(remaining())).values();
        List<Partition> partitions = new ArrayList<>();
        Map<Integer, ConfigEntry> brokerDefaults = new HashMap<>();
        for (String topic : topics) {
            Integer minIsr = null;
            String source = null;
            String problem = null;
            try {
                ConfigEntry entry = await(configFutures.get(new ConfigResource(ConfigResource.Type.TOPIC, topic))).get("min.insync.replicas");
                if (entry == null || entry.value() == null) {
                    problem = "CONFIG_UNAVAILABLE";
                } else {
                    List<Integer> brokerIds = descriptions.get(topic).partitions().stream()
                            .flatMap(p -> p.replicas().stream()).map(n -> n.id()).distinct().toList();
                    List<ConfigEntry> defaults = List.of();
                    if (entry.source() != ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                        List<ConfigResource> missing = brokerIds.stream().filter(id -> !brokerDefaults.containsKey(id))
                                .map(id -> new ConfigResource(ConfigResource.Type.BROKER, id.toString())).toList();
                        if (!missing.isEmpty()) {
                            var futures = admin.describeConfigs(missing, new DescribeConfigsOptions().timeoutMs(remaining())).values();
                            for (var future : futures.entrySet()) {
                                ConfigEntry value = null;
                                try {
                                    value = await(future.getValue()).get("min.insync.replicas");
                                } catch (Exception e) {
                                    remaining();
                                }
                                brokerDefaults.put(Integer.valueOf(future.getKey().name()), value);
                            }
                        }
                        defaults = brokerIds.stream().map(brokerDefaults::get).toList();
                    }
                    var resolved = MinIsr.resolve(entry, defaults);
                    minIsr = resolved.count();
                    source = resolved.source();
                    problem = resolved.problem();
                }
            } catch (NumberFormatException e) {
                problem = "INCONSISTENT_EVIDENCE";
            } catch (Exception e) {
                remaining();
                problem = "CONFIG_UNAVAILABLE";
            }
            for (var p : descriptions.get(topic).partitions()) {
                partitions.add(new Partition(topic, descriptions.get(topic).topicId().toString(), p.partition(), p.leader() == null ? -1 : p.leader().id(),
                        p.replicas().stream().map(n -> n.id()).toList(), p.isr().stream().map(n -> n.id()).toList(),
                        minIsr, source, problem));
            }
        }
        Map<String, Short> features = new LinkedHashMap<>();
        var featureInfo = await(admin.describeFeatures(new DescribeFeaturesOptions().timeoutMs(remaining())).featureMetadata());
        featureInfo.finalizedFeatures().forEach((name, levels) -> features.put(name, levels.maxVersionLevel()));
        JsonObject kafka = kubernetes.getKafka(cluster);
        var mapping = StrimziMapping.resolve(cluster, clusterId, kafka,
                kubernetes.list("kafka.strimzi.io", "kafkanodepools", cluster),
                kubernetes.list("", "pods", cluster), kubernetes.list("core.strimzi.io", "strimzipodsets", cluster));
        String problem = mapping.problem();
        if (!Short.valueOf((short) 1).equals(features.get("eligible.leader.replicas.version"))
                || !Short.valueOf((short) 30).equals(features.get("metadata.version"))) {
            problem = "UNSUPPORTED_PROFILE";
        }
        remaining();
        return new Snapshot(clusterId, mapping.kafkaUid(), mapping.generation(), problem,
                mapping.placements(), partitions, features);
    }

    private <T> T await(KafkaFuture<T> future) throws Exception {
        try {
            return future.get(remaining(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof Exception cause) {
                throw cause;
            }
            throw e;
        }
    }

    private int remaining() {
        long millis = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (millis <= 0) {
            throw new Failure("DEADLINE_EXCEEDED");
        }
        return (int) Math.min(Integer.MAX_VALUE, millis);
    }

    @Override
    public void close() {
        admin.close(Duration.ZERO);
        kube.getHttpClient().dispatcher().executorService().shutdownNow();
        kube.getHttpClient().connectionPool().evictAll();
    }
}
