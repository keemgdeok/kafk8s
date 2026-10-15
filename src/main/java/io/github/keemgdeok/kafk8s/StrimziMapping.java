package io.github.keemgdeok.kafk8s;

import static io.github.keemgdeok.kafk8s.Evidence.Placement;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class StrimziMapping {
    private StrimziMapping() {}

    public record Mapping(String clusterId, String kafkaUid, long generation,
                          String problem, Map<Integer, Placement> placements) {}

    public static Mapping resolve(String name, String kafkaClusterId, JsonObject kafka,
                                  List<JsonObject> pools, List<JsonObject> pods, List<JsonObject> podsets) {
        JsonObject status = object(kafka, "status");
        String recordedId = text(status, "clusterId");
        if (!recordedId.isEmpty() && !recordedId.equals(kafkaClusterId)) {
            throw new Failure("CLUSTER_MISMATCH");
        }
        JsonObject metadata = object(kafka, "metadata");
        String uid = text(metadata, "uid");
        long generation = number(metadata, "generation");
        String problem = null;
        if (recordedId.isEmpty() || uid.isEmpty() || generation < 1
                || number(status, "observedGeneration") != generation) {
            problem = "INCONSISTENT_EVIDENCE";
        }
        if (!text(status, "operatorLastSuccessfulVersion").equals("1.3.0")
                || !text(status, "kafkaVersion").equals("4.3.1")) {
            problem = "UNSUPPORTED_PROFILE";
        }
        Map<Integer, Placement> placements = new LinkedHashMap<>();
        for (JsonObject pool : pools) {
            JsonObject poolMeta = object(pool, "metadata");
            if (!text(object(poolMeta, "labels"), "strimzi.io/cluster").equals(name)) {
                continue;
            }
            JsonObject poolStatus = object(pool, "status");
            List<String> roles = strings(array(poolStatus, "roles"));
            if (!roles.contains("broker")) {
                continue;
            }
            String poolProblem = null;
            if (!text(poolStatus, "clusterId").isEmpty() && !text(poolStatus, "clusterId").equals(kafkaClusterId)) {
                throw new Failure("CLUSTER_MISMATCH");
            }
            if (roles.size() != 1 || !strings(array(object(pool, "spec"), "roles")).equals(roles)) {
                problem = "UNSUPPORTED_PROFILE";
            }
            String poolName = text(poolMeta, "name");
            String poolUid = text(poolMeta, "uid");
            long poolGeneration = number(poolMeta, "generation");
            if (poolUid.isEmpty() || text(poolStatus, "clusterId").isEmpty() || poolGeneration < 1
                    || number(poolStatus, "observedGeneration") != poolGeneration) {
                poolProblem = "INCONSISTENT_EVIDENCE";
            }
            for (JsonElement idValue : array(poolStatus, "nodeIds")) {
                int id;
                try {
                    id = Integer.parseInt(idValue.getAsString());
                    if (id < 0) {
                        continue;
                    }
                } catch (RuntimeException e) {
                    continue;
                }
                String podName = name + "-" + poolName + "-" + id;
                List<JsonObject> candidates = pods.stream()
                        .filter(p -> text(object(p, "metadata"), "name").equals(podName)).toList();
                String issue = poolProblem;
                String podUid = "";
                String node = "";
                if (candidates.size() != 1) {
                    issue = candidates.isEmpty() ? "MAPPING_MISSING" : "MAPPING_CONFLICT";
                } else {
                    JsonObject pod = candidates.getFirst();
                    JsonObject podMeta = object(pod, "metadata");
                    JsonObject labels = object(podMeta, "labels");
                    podUid = text(podMeta, "uid");
                    node = text(object(pod, "spec"), "nodeName");
                    if (podUid.isEmpty() || node.isEmpty() || podMeta.has("deletionTimestamp")
                            || !text(labels, "strimzi.io/cluster").equals(name)
                            || !text(labels, "strimzi.io/pool-name").equals(poolName)
                            || !text(labels, "strimzi.io/broker-role").equals("true")
                            || !text(labels, "strimzi.io/controller-role").equals("false")
                            || !text(labels, "strimzi.io/pod-name").equals(podName)
                            || !validOwnerChain(podMeta, podName, poolName, poolUid, name, podsets)) {
                        issue = "MAPPING_CONFLICT";
                    }
                }
                Placement placement = new Placement(id, podName, podUid, node, poolUid, poolGeneration, issue);
                if (placements.containsKey(id)) {
                    placement = new Placement(id, podName, podUid, node, poolUid, poolGeneration, "MAPPING_CONFLICT");
                }
                placements.put(id, placement);
            }
        }
        return new Mapping(kafkaClusterId, uid, generation, problem, Map.copyOf(placements));
    }

    private static boolean validOwnerChain(JsonObject metadata, String podName, String poolName,
                                           String poolUid, String cluster, List<JsonObject> sets) {
        List<JsonObject> owners = new ArrayList<>();
        for (JsonElement owner : array(metadata, "ownerReferences")) {
            JsonObject o = owner.getAsJsonObject();
            if (text(o, "kind").equals("StrimziPodSet") && text(o, "controller").equals("true")
                    && text(o, "apiVersion").equals("core.strimzi.io/v1")) {
                owners.add(o);
            }
        }
        if (owners.size() != 1) {
            return false;
        }
        JsonObject owner = owners.getFirst();
        List<JsonObject> matching = sets.stream().filter(set -> {
            JsonObject m = object(set, "metadata");
            return text(m, "name").equals(text(owner, "name")) && text(m, "uid").equals(text(owner, "uid"))
                    && !text(m, "uid").isEmpty();
        }).toList();
        if (matching.size() != 1) {
            return false;
        }
        JsonObject set = matching.getFirst();
        JsonObject labels = object(object(set, "metadata"), "labels");
        if (!text(labels, "strimzi.io/cluster").equals(cluster)
                || !text(labels, "strimzi.io/pool-name").equals(poolName)) {
            return false;
        }
        boolean poolOwner = false;
        for (JsonElement o : array(object(set, "metadata"), "ownerReferences")) {
            JsonObject ref = o.getAsJsonObject();
            poolOwner |= text(ref, "kind").equals("KafkaNodePool") && text(ref, "name").equals(poolName)
                    && text(ref, "uid").equals(poolUid) && !poolUid.isEmpty()
                    && text(ref, "apiVersion").equals("kafka.strimzi.io/v1");
        }
        boolean declared = false;
        for (JsonElement p : array(object(set, "spec"), "pods")) {
            declared |= text(object(p.getAsJsonObject(), "metadata"), "name").equals(podName);
        }
        return poolOwner && declared;
    }

    static JsonObject object(JsonObject parent, String key) {
        JsonElement value = parent.get(key);
        return value != null && value.isJsonObject() ? value.getAsJsonObject() : new JsonObject();
    }

    static JsonArray array(JsonObject parent, String key) {
        JsonElement value = parent.get(key);
        return value != null && value.isJsonArray() ? value.getAsJsonArray() : new JsonArray();
    }

    static String text(JsonObject parent, String key) {
        JsonElement value = parent.get(key);
        return value != null && value.isJsonPrimitive() ? value.getAsString() : "";
    }

    static long number(JsonObject parent, String key) {
        try {
            return Long.parseLong(text(parent, key));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static List<String> strings(JsonArray values) {
        return values.asList().stream().filter(JsonElement::isJsonPrimitive).map(JsonElement::getAsString).toList();
    }
}
