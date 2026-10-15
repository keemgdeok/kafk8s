package io.github.keemgdeok.kafk8s;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.KubeConfig;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "kafk8s", mixinStandardHelpOptions = true, versionProvider = Main.Version.class,
        description = "Explain selected Kafka partitions' conditional node-loss ISR budget.",
        subcommands = {Main.Inspect.class, Main.Check.class})
public final class Main implements Runnable {
    static final Gson JSON = new GsonBuilder().serializeNulls()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

    @Spec
    private CommandSpec spec;

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    public static int execute(String... args) {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "warn");
        CommandLine cli = new CommandLine(new Main());
        cli.setParameterExceptionHandler((exception, arguments) -> {
            error(cli, wantsJson(args), "INVALID_ARGUMENT");
            return 2;
        });
        cli.setExecutionExceptionHandler((exception, command, parseResult) -> {
            error(command, wantsJson(args), reason(exception));
            return 2;
        });
        return cli.execute(args);
    }

    @Override
    public void run() {
        spec.commandLine().usage(spec.commandLine().getOut());
    }

    public static final class Version implements CommandLine.IVersionProvider {
        @Override
        public String[] getVersion() throws Exception {
            Properties version = new Properties();
            try (InputStream input = Main.class.getResourceAsStream("/version.properties")) {
                if (input == null) {
                    throw new IllegalStateException("Version resource missing");
                }
                version.load(input);
            }
            return new String[]{"kafk8s " + version.getProperty("version")};
        }
    }

    @Command(name = "inspect", mixinStandardHelpOptions = true, description = "Show partition placement and evidence.")
    static final class Inspect extends Operation {}

    @Command(name = "check", mixinStandardHelpOptions = true, description = "Analyze one hypothetical node loss; not a drain-safety check.")
    static final class Check extends Operation {
        @Option(names = "--lost-node", required = true)
        private String lostNode;

        @Override
        String lostNode() {
            return lostNode;
        }
    }

    abstract static class Operation implements Callable<Integer> {
        @Spec private CommandSpec spec;
        @Option(names = "--namespace", required = true) private String namespace;
        @Option(names = "--cluster", required = true) private String cluster;
        @Option(names = "--bootstrap-server", required = true) private String bootstrap;
        @Option(names = "--topic", required = true, description = "Explicit topic; repeat for more topics.") private List<String> topics;
        @Option(names = "--client-properties") private Path propertiesFile;
        @Option(names = "--kubeconfig") private Path kubeconfig;
        @Option(names = "--context") private String context;
        @Option(names = "--format", defaultValue = "text") private String format;
        @Option(names = "--timeout", defaultValue = "30s") private String timeout;

        String lostNode() { return null; }

        @Override
        public Integer call() throws Exception {
            if (!List.of("text", "json").contains(format) || !timeout.matches("[1-9][0-9]*s")
                    || namespace.isBlank() || cluster.isBlank() || bootstrap.isBlank()
                    || topics.stream().anyMatch(String::isBlank) || (lostNode() != null && lostNode().isBlank())) {
                throw new Failure("INVALID_ARGUMENT");
            }
            int seconds;
            try {
                seconds = Integer.parseInt(timeout.substring(0, timeout.length() - 1));
            } catch (NumberFormatException e) {
                throw new Failure("INVALID_ARGUMENT");
            }
            if (seconds > 120 || topics.size() > 100) {
                throw new Failure("INVALID_ARGUMENT");
            }
            long started = System.nanoTime();
            long deadline = started + TimeUnit.SECONDS.toNanos(seconds);
            String startAt = Instant.now().toString();
            Properties properties = new Properties();
            if (propertiesFile != null) {
                try (InputStream input = Files.newInputStream(propertiesFile)) {
                    properties.load(input);
                }
            }
            properties.put("bootstrap.servers", bootstrap);
            properties.put("client.id", "kafk8s-inspector");
            properties.put("default.api.timeout.ms", Integer.toString(seconds * 1000));
            properties.put("request.timeout.ms", Integer.toString(Math.min(5000, seconds * 1000)));
            if (properties.getProperty("security.protocol", "PLAINTEXT").contains("SSL")
                    && !properties.getProperty("ssl.endpoint.identification.algorithm", "https").equalsIgnoreCase("https")) {
                throw new Failure("INVALID_ARGUMENT");
            }
            ApiClient client = client();
            if (!client.isVerifyingSsl()) throw new Failure("INVALID_ARGUMENT");
            List<Evidence.Row> rows;
            Evidence.Snapshot snapshot;
            try (Collector collector = new Collector(Admin.create(properties), client, deadline,
                    namespace, cluster, List.copyOf(new LinkedHashSet<>(topics)))) {
                Evidence.Snapshot first = collector.collect();
                snapshot = collector.collect();
                if (lostNode() != null && snapshot.placements().values().stream()
                        .noneMatch(p -> p.node().equals(lostNode()))) {
                    throw new Failure("NODE_NOT_FOUND");
                }
                rows = Analyzer.analyze(first, snapshot, lostNode());
            }
            int code = Analyzer.exitCode(rows);
            boolean complete = rows.stream().noneMatch(r -> r.status().equals("UNKNOWN"));
            if (format.equals("json")) {
                spec.commandLine().getOut().println(JSON.toJson(new Report(1,
                        complete ? "COMPLETE" : "INCOMPLETE", complete, namespace, cluster, snapshot.clusterId(),
                        snapshot.kafkaUid(), lostNode(), startAt, Instant.now().toString(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started), snapshot.features(),
                        "Observed ISR only; simultaneous node loss; no catch-up/recovery; producer acks=all assumed. Not SAFE, outage or data-loss prediction.", rows)));
            } else {
                var out = spec.commandLine().getOut();
                out.println("TOPIC PARTITION STATUS ISR MIN_ISR LOST_ISR REMAINING REASON");
                for (Evidence.Row row : rows) {
                    out.printf("%s %d %s %s %s %s %s %s%n", row.topic(), row.partition(), row.status(),
                            row.isr(), row.minIsr(), row.lostIsr(), row.remainingIsr(), row.reasonCode());
                    row.placements().forEach(p -> out.printf("  broker=%d pod=%s node=%s%s%n",
                            p.brokerId(), p.pod(), p.node(), p.problem() == null ? "" : " problem=" + p.problem()));
                }
                out.println("Assumes acks=all and no catch-up/recovery. NO_SHORTFALL does not mean SAFE or zero interruption.");
            }
            return code;
        }

        private ApiClient client() throws Exception {
            if (kubeconfig == null && context == null) {
                return Config.defaultClient();
            }
            Path path = kubeconfig != null ? kubeconfig : Path.of(System.getenv().getOrDefault("KUBECONFIG",
                    Path.of(System.getProperty("user.home"), ".kube", "config").toString()));
            try (var reader = Files.newBufferedReader(path)) {
                KubeConfig config = KubeConfig.loadKubeConfig(reader);
                config.setFile(path.toAbsolutePath().toFile());
                if (context != null) {
                    boolean found = false;
                    for (Object raw : config.getContexts()) {
                        if (raw instanceof Map<?, ?> entry && context.equals(entry.get("name"))) {
                            found = true;
                        }
                    }
                    if (!found) {
                        throw new Failure("INVALID_ARGUMENT");
                    }
                    config.setContext(context);
                }
                return ClientBuilder.kubeconfig(config).build();
            }
        }
    }

    record Report(int schemaVersion, String status, boolean complete, String namespace, String cluster,
                  String clusterId, String kafkaUid, String lostNode, String startedAt, String finishedAt,
                  long elapsedMs, Map<String, Short> finalizedFeatures, String assumptions, List<Evidence.Row> partitions) {}

    private static boolean wantsJson(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("--format=json") || (args[i].equals("--format") && i + 1 < args.length && args[i + 1].equals("json"))) {
                return true;
            }
        }
        return false;
    }

    static String reason(Throwable error) {
        if (error instanceof Failure failure) return failure.reasonCode;
        if (error instanceof AuthenticationException || error instanceof AuthorizationException) return "AUTHORIZATION_ERROR";
        if (error instanceof UnknownTopicOrPartitionException) return "TOPIC_NOT_FOUND";
        if (error instanceof java.util.concurrent.TimeoutException || error instanceof org.apache.kafka.common.errors.TimeoutException) return "DEADLINE_EXCEEDED";
        if (error instanceof java.io.IOException) return "CONFIGURATION_OR_CONNECTION_ERROR";
        return "CONNECTION_OR_CONFIGURATION_ERROR";
    }

    private static void error(CommandLine cli, boolean json, String reason) {
        if (json) {
            cli.getOut().println(JSON.toJson(Map.of("schema_version", 1, "status", "ERROR", "reason_code", reason,
                    "message", "Command could not complete. Check arguments, permissions, supported profile and connectivity.")));
        } else {
            cli.getErr().println("ERROR: " + reason);
        }
    }
}
