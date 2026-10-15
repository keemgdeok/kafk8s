package lab;

import com.google.gson.Gson;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Probe {
    private static final Gson JSON = new Gson();

    private Probe() {}

    private static synchronized void emit(Map<String, ?> value) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("at", Instant.now().toString());
        event.putAll(value);
        System.out.println(JSON.toJson(event));
        System.out.flush();
    }

    private static Properties properties(String bootstrap) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("request.timeout.ms", "1500");
        p.put("default.api.timeout.ms", "10000");
        p.put("client.id", "kafk8s-lab-probe");
        return p;
    }

    private static Map<String, Object> metadata(Admin admin, String topic) throws Exception {
        var description = admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS).get(topic);
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        ConfigEntry min = admin.describeConfigs(List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource).get("min.insync.replicas");
        List<Map<String, Object>> partitions = new ArrayList<>();
        for (var p : description.partitions()) {
            partitions.add(Map.of("partition", p.partition(), "leader", p.leader().id(),
                    "replicas", p.replicas().stream().map(n -> n.id()).toList(),
                    "isr", p.isr().stream().map(n -> n.id()).toList()));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("event", "METADATA");
        result.put("cluster_id", admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS));
        result.put("topic", topic);
        result.put("min_isr", min.value());
        result.put("min_isr_source", min.source().name());
        result.put("partitions", partitions);
        var features = admin.describeFeatures().featureMetadata().get(10, TimeUnit.SECONDS);
        Map<String, Object> finalized = new LinkedHashMap<>();
        features.finalizedFeatures().forEach((key, range) -> finalized.put(key,
                Map.of("min", range.minVersionLevel(), "max", range.maxVersionLevel())));
        result.put("finalized_features", finalized);
        return result;
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = args[1];
        String topic = args[2];
        if (args[0].equals("metadata")) {
            try (Admin admin = Admin.create(properties(bootstrap))) {
                emit(metadata(admin, topic));
            }
            return;
        }
        if (!args[0].equals("traffic")) {
            throw new IllegalArgumentException("metadata or traffic required");
        }
        String run = args[3];
        int seconds = Integer.parseInt(args[4]);
        if (seconds < 30 || seconds > 300) {
            throw new IllegalArgumentException("duration must be 30..300 seconds");
        }
        Properties producerProperties = properties(bootstrap);
        producerProperties.put("acks", "all");
        producerProperties.put("enable.idempotence", "true");
        producerProperties.put("delivery.timeout.ms", "4000");
        producerProperties.put("max.block.ms", "2000");
        producerProperties.put("key.serializer", StringSerializer.class.getName());
        producerProperties.put("value.serializer", StringSerializer.class.getName());
        Properties consumerProperties = properties(bootstrap);
        consumerProperties.put("enable.auto.commit", "false");
        consumerProperties.put("key.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
        consumerProperties.put("auto.offset.reset", "none");
        Set<String> acked = ConcurrentHashMap.newKeySet();
        Set<String> observed = ConcurrentHashMap.newKeySet();
        Set<String> ambiguous = new HashSet<>();
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicBoolean consumerFailed = new AtomicBoolean(false);
        long started = System.nanoTime();
        try (Admin admin = Admin.create(properties(bootstrap));
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            List<TopicPartition> partitions = admin.describeTopics(List.of(topic)).allTopicNames()
                    .get(3, TimeUnit.SECONDS).get(topic).partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition())).toList();
            consumer.assign(partitions);
            var initialOffsets = consumer.endOffsets(partitions, Duration.ofSeconds(3));
            initialOffsets.forEach(consumer::seek);
            Thread reader = new Thread(() -> {
                try {
                    while (running.get()) {
                        for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                            if (record.key() != null && record.key().startsWith(run + ":")) {
                                boolean duplicate = !observed.add(record.key());
                                emit(Map.of("event", "RECEIVED", "id", record.key(), "partition", record.partition(),
                                        "offset", record.offset(), "duplicate", duplicate,
                                        "payload_matches", record.value().equals("payload:" + record.key())));
                            }
                        }
                    }
                } catch (RuntimeException e) {
                    consumerFailed.set(true);
                    emit(Map.of("event", "CONSUMER_ERROR", "type", e.getClass().getSimpleName()));
                }
            }, "probe-consumer");
            reader.start();
            emit(Map.of("event", "READY", "run", run, "initial_offsets", initialOffsets.toString(),
                    "duration_seconds", seconds, "acks", "all", "idempotence", true));
            int sequence = 0;
            long nextMetadata = 0;
            while (System.nanoTime() - started < TimeUnit.SECONDS.toNanos(seconds)) {
                long now = System.nanoTime();
                if (now >= nextMetadata) {
                    try {
                        emit(metadata(admin, topic));
                    } catch (Exception e) {
                        emit(Map.of("event", "METADATA_ERROR", "type", e.getClass().getSimpleName()));
                    }
                    nextMetadata = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                }
                String id = run + ":" + sequence;
                int partition = sequence++ % partitions.size();
                long sendStart = System.nanoTime();
                try {
                    var result = producer.send(new ProducerRecord<>(topic, partition, id, "payload:" + id))
                            .get(5, TimeUnit.SECONDS);
                    acked.add(id);
                    emit(Map.of("event", "ACK", "id", id, "partition", partition, "offset", result.offset(),
                            "elapsed_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sendStart)));
                } catch (Exception e) {
                    ambiguous.add(id);
                    Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                    emit(Map.of("event", "SEND_ERROR", "id", id, "partition", partition,
                            "type", cause.getClass().getSimpleName(), "delivery", "NOT_ACKNOWLEDGED_OR_AMBIGUOUS",
                            "elapsed_ms", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - sendStart)));
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
            TimeUnit.SECONDS.sleep(5);
            running.set(false);
            reader.join(3000);
            if (reader.isAlive()) {
                consumer.wakeup();
                reader.join(3000);
            }
            Set<String> notObserved = new HashSet<>(acked);
            notObserved.removeAll(observed);
            Set<String> ambiguousObserved = new HashSet<>(ambiguous);
            ambiguousObserved.retainAll(observed);
            emit(Map.of("event", "SUMMARY", "run", run, "acked", acked.size(), "observed", observed.size(),
                    "send_errors", ambiguous.size(), "acked_not_observed_within_window", notObserved,
                    "unacknowledged_but_observed", ambiguousObserved, "consumer_failed", consumerFailed.get()));
        }
    }
}
