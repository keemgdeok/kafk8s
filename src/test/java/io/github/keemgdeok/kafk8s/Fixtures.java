package io.github.keemgdeok.kafk8s;

import static io.github.keemgdeok.kafk8s.Evidence.*;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

final class Fixtures {
    private Fixtures() {}

    static JsonObject json(String run, String file) throws IOException {
        return JsonParser.parseString(Files.readString(Path.of("docs/evidence", run, file))).getAsJsonObject();
    }

    static List<JsonObject> objects(JsonObject list) {
        return list.getAsJsonArray("items").asList().stream().map(v -> v.getAsJsonObject()).toList();
    }

    static Snapshot snapshot(String run, String phase) throws IOException {
        JsonObject metadata = json(run, phase + "-metadata.jsonl");
        var mapping = StrimziMapping.resolve("demo", metadata.get("cluster_id").getAsString(),
                json(run, phase + "-kafka.json"), objects(json(run, phase + "-pools.json")),
                objects(json(run, phase + "-pods.json")), objects(json(run, phase + "-podsets.json")));
        List<Partition> partitions = metadata.getAsJsonArray("partitions").asList().stream().map(v -> {
            JsonObject p = v.getAsJsonObject();
            return new Partition("lab-events", "historical-fixture-topic-id-not-recorded", p.get("partition").getAsInt(),
                    p.get("leader").getAsInt(), p.getAsJsonArray("replicas").asList().stream().map(e -> e.getAsInt()).toList(),
                    p.getAsJsonArray("isr").asList().stream().map(e -> e.getAsInt()).toList(),
                    metadata.get("min_isr").getAsInt(), metadata.get("min_isr_source").getAsString(), null);
        }).toList();
        return new Snapshot(mapping.clusterId(), mapping.kafkaUid(), mapping.generation(), mapping.problem(),
                mapping.placements(), partitions, Map.of("eligible.leader.replicas.version", (short) 1, "metadata.version", (short) 30));
    }
}
