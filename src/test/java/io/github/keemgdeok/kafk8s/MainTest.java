package io.github.keemgdeok.kafk8s;

import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.JsonParser;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MainTest {
    @TempDir Path temporary;
    private record Result(int exit, String out, String err) {}

    private static Result run(String... args) {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        try (PrintStream stdout = new PrintStream(out, true, StandardCharsets.UTF_8);
             PrintStream stderr = new PrintStream(err, true, StandardCharsets.UTF_8)) {
            System.setOut(stdout);
            System.setErr(stderr);
            int code = Main.execute(args);
            return new Result(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    @Test void helpDoesNotRequireACluster() {
        Result result = run("--help");
        assertEquals(0, result.exit());
        assertTrue(result.out().contains("inspect"));
        assertTrue(result.out().contains("check"));
    }

    @Test void versionIsFromGradleResource() throws Exception {
        Result result = run("--version");
        assertEquals(0, result.exit());
        assertEquals(new Main.Version().getVersion()[0], result.out().strip());
        assertFalse(result.out().contains("${version}"));
    }

    @Test void parseErrorIsSingleJsonDocument() {
        Result result = run("check", "--format", "json");
        assertEquals(2, result.exit());
        var json = JsonParser.parseString(result.out()).getAsJsonObject();
        assertEquals("ERROR", json.get("status").getAsString());
        assertEquals("INVALID_ARGUMENT", json.get("reason_code").getAsString());
        assertTrue(result.err().isEmpty());
    }

    @Test void invalidTimeoutIsStructuredBeforeConnecting() {
        Result result = run("check", "--namespace", "ns", "--cluster", "demo", "--bootstrap-server", "invalid:1",
                "--topic", "events", "--lost-node", "a", "--timeout", "0s", "--format=json");
        assertEquals(2, result.exit());
        assertEquals("INVALID_ARGUMENT", JsonParser.parseString(result.out()).getAsJsonObject().get("reason_code").getAsString());
    }

    @Test void fileErrorDoesNotEchoSensitivePath() {
        Result result = run("inspect", "--namespace", "ns", "--cluster", "demo", "--bootstrap-server", "invalid:1",
                "--topic", "events", "--client-properties", "/does-not-exist/private-secret", "--format=json");
        assertEquals(2, result.exit());
        assertEquals("ERROR", JsonParser.parseString(result.out()).getAsJsonObject().get("status").getAsString());
        assertFalse(result.out().contains("private-secret"));
    }

    @Test void errorClassificationIsSpecificWithoutRawExceptionMessages() {
        assertEquals("AUTHORIZATION_ERROR", Main.reason(new org.apache.kafka.common.errors.TopicAuthorizationException("secret")));
        assertEquals("TOPIC_NOT_FOUND", Main.reason(new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("secret")));
        assertEquals("DEADLINE_EXCEEDED", Main.reason(new java.util.concurrent.TimeoutException("secret")));
    }

    private Path kubeconfig() throws Exception {
        String certificate = Fixtures.json("20261014T143455Z-spread", "before-kafka.json")
                .getAsJsonObject("status").getAsJsonArray("listeners").get(1).getAsJsonObject()
                .getAsJsonArray("certificates").get(0).getAsString();
        Files.writeString(temporary.resolve("ca.crt"), certificate);
        Path config = temporary.resolve("config");
        Files.writeString(config, """
                apiVersion: v1
                kind: Config
                current-context: selected
                clusters:
                  - name: lab
                    cluster:
                      server: https://127.0.0.1:6443
                      certificate-authority: ca.crt
                users:
                  - name: fixture
                    user:
                      token: non-secret-test-placeholder
                contexts:
                  - name: selected
                    context:
                      cluster: lab
                      user: fixture
                """);
        return config;
    }

    @Test void explicitContextAndRelativeCertificateResolveBeforeKafkaTimeout() throws Exception {
        Result result = run("inspect", "--namespace", "lab", "--cluster", "demo", "--bootstrap-server", "127.0.0.1:1",
                "--topic", "events", "--kubeconfig", kubeconfig().toString(), "--context", "selected", "--timeout", "2s", "--format=json");
        assertEquals(2, result.exit());
        assertEquals("DEADLINE_EXCEEDED", JsonParser.parseString(result.out()).getAsJsonObject().get("reason_code").getAsString());
    }

    @Test void unknownContextIsAnArgumentError() throws Exception {
        Result result = run("inspect", "--namespace", "lab", "--cluster", "demo", "--bootstrap-server", "127.0.0.1:1",
                "--topic", "events", "--kubeconfig", kubeconfig().toString(), "--context", "missing", "--format=json");
        assertEquals("INVALID_ARGUMENT", JsonParser.parseString(result.out()).getAsJsonObject().get("reason_code").getAsString());
    }
}
