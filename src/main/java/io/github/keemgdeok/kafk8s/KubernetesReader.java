package io.github.keemgdeok.kafk8s;

import com.google.gson.JsonObject;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.generic.KubernetesApiResponse;
import io.kubernetes.client.util.generic.dynamic.DynamicKubernetesApi;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

final class KubernetesReader {
    private final ApiClient client;
    private final long deadline;
    private final String namespace;
    private final Set<String> paths;

    KubernetesReader(ApiClient client, long deadline, String namespace, String cluster) {
        this.client = client;
        this.deadline = deadline;
        this.namespace = namespace;
        paths = Set.of("/api/v1/namespaces/" + namespace + "/pods",
                "/apis/kafka.strimzi.io/v1/namespaces/" + namespace + "/kafkas/" + cluster,
                "/apis/kafka.strimzi.io/v1/namespaces/" + namespace + "/kafkanodepools",
                "/apis/core.strimzi.io/v1/namespaces/" + namespace + "/strimzipodsets");
        client.setHttpClient(client.getHttpClient().newBuilder().addInterceptor(chain -> {
            if (!chain.request().method().equals("GET") || !paths.contains(chain.request().url().encodedPath())) {
                throw new IOException("Request outside read-only resource allowlist");
            }
            return chain.proceed(chain.request());
        }).build());
    }

    JsonObject getKafka(String name) {
        requirePath("kafka.strimzi.io", "kafkas", name);
        var response = api("kafka.strimzi.io", "kafkas").get(namespace, name);
        requireSuccess(response);
        return response.getObject().getRaw();
    }

    List<JsonObject> list(String group, String resource, String cluster) {
        requirePath(group, resource, null);
        ListOptions options = new ListOptions();
        options.setLabelSelector("strimzi.io/cluster=" + cluster);
        var response = api(group, resource).list(namespace, options);
        requireSuccess(response);
        if (response.getObject().getMetadata().getContinue() != null
                && !response.getObject().getMetadata().getContinue().isBlank()) {
            throw new Failure("PARTIAL_LIST");
        }
        return response.getObject().getItems().stream().map(o -> o.getRaw()).toList();
    }

    private DynamicKubernetesApi api(String group, String resource) {
        long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
        if (remaining <= 0) throw new Failure("DEADLINE_EXCEEDED");
        client.setHttpClient(client.getHttpClient().newBuilder().callTimeout(Duration.ofMillis(remaining))
                .retryOnConnectionFailure(false).build());
        return new DynamicKubernetesApi(group, "v1", resource, client);
    }

    private void requirePath(String group, String resource, String name) {
        String prefix = group.isEmpty() ? "/api/v1" : "/apis/" + group + "/v1";
        String path = prefix + "/namespaces/" + namespace + "/" + resource + (name == null ? "" : "/" + name);
        if (!paths.contains(path)) throw new Failure("READ_ONLY_VIOLATION");
    }

    private void requireSuccess(KubernetesApiResponse<?> response) {
        if (System.nanoTime() >= deadline) throw new Failure("DEADLINE_EXCEEDED");
        if (!response.isSuccess()) {
            int code = response.getHttpStatusCode();
            throw new Failure(code == 401 || code == 403 ? "AUTHORIZATION_ERROR"
                    : code == 404 ? "RESOURCE_NOT_FOUND" : "KUBERNETES_ERROR");
        }
    }
}
