package io.github.keemgdeok.kafk8s;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import io.kubernetes.client.openapi.ApiClient;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KubernetesReaderTest {
    private HttpServer server;
    private ApiClient client;
    private List<String> requests;
    private int status;
    private String body;

    @BeforeEach void start() throws Exception {
        requests = new ArrayList<>();
        status = 200;
        body = "{\"apiVersion\":\"v1\",\"kind\":\"List\",\"metadata\":{},\"items\":[]}";
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            requests.add(exchange.getRequestMethod() + " " + exchange.getRequestURI());
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        client = new ApiClient().setBasePath("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach void stop() {
        server.stop(0);
        client.getHttpClient().dispatcher().executorService().shutdownNow();
        client.getHttpClient().connectionPool().evictAll();
    }

    private KubernetesReader reader() {
        return new KubernetesReader(client, System.nanoTime() + TimeUnit.SECONDS.toNanos(5), "lab", "demo");
    }

    @Test void listUsesGetAndClusterSelector() {
        assertTrue(reader().list("", "pods", "demo").isEmpty());
        assertEquals(1, requests.size());
        assertTrue(requests.getFirst().startsWith("GET /api/v1/namespaces/lab/pods?"));
        assertTrue(requests.getFirst().contains("labelSelector=strimzi.io%2Fcluster%3Ddemo"));
    }

    @Test void secretsAreBlockedBeforeAnyRequest() {
        assertThrows(Failure.class, () -> reader().list("", "secrets", "demo"));
        assertTrue(requests.isEmpty());
    }

    @Test void differentClusterPathIsBlockedBeforeRequest() {
        assertThrows(Failure.class, () -> reader().getKafka("other"));
        assertTrue(requests.isEmpty());
    }

    @Test void actualForbiddenResponseIsClassified() {
        status = 403;
        body = "{\"apiVersion\":\"v1\",\"kind\":\"Status\",\"code\":403,\"status\":\"Failure\",\"reason\":\"Forbidden\"}";
        assertEquals("AUTHORIZATION_ERROR", assertThrows(Failure.class, () -> reader().list("", "pods", "demo")).reasonCode);
        assertEquals(1, requests.size());
    }

    @Test void continuationCannotSilentlyTruncateEvidence() {
        body = "{\"apiVersion\":\"v1\",\"kind\":\"List\",\"metadata\":{\"continue\":\"next\"},\"items\":[]}";
        assertEquals("PARTIAL_LIST", assertThrows(Failure.class, () -> reader().list("", "pods", "demo")).reasonCode);
    }

    @Test void expiredBudgetMakesNoRequest() {
        var reader = new KubernetesReader(client, System.nanoTime() - 1, "lab", "demo");
        assertEquals("DEADLINE_EXCEEDED", assertThrows(Failure.class, () -> reader.list("", "pods", "demo")).reasonCode);
        assertTrue(requests.isEmpty());
    }
}
