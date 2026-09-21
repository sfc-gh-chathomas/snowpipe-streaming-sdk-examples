package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Level 2: continuous Futures. Source storage and process-crash replay are not provided. */
public class ElasticChannelContinuous {
    static final String DATABASE = env("SNOWFLAKE_DATABASE", "MY_DATABASE");
    static final String SCHEMA = env("SNOWFLAKE_SCHEMA", "MY_SCHEMA");
    static final String TABLE = env("SNOWFLAKE_TABLE", "MY_TABLE");
    static final String PROFILE = env("SNOWFLAKE_PROFILE", "profile.json");
    static final String RUN_ID = env("SNOWFLAKE_RUN_ID", UUID.randomUUID().toString());
    static final int MAX_PENDING = 1000;
    static final long STALL_NANOS = TimeUnit.MINUTES.toNanos(30);

    /** Identify client/channel invalidation; do not replay on mere delayed acknowledgement. */
    static boolean invalid(SFException error) {
        return Set.of("InvalidChannelError", "InvalidClientError", "ClosedClientError", "ClosedElasticChannelError")
                .contains(error.getErrorCodeName());
    }



    /** Append until capacity is reached, then collect outcomes; retain unresolved rows for recovery. */
    public static void main(String[] args) throws Exception {
        int total = Integer.parseInt(env("SNOWFLAKE_TEST_ROWS", "5000"));
        if (total < 0) throw new IllegalArgumentException("Row count must be nonnegative");
        AtomicBoolean stopping = new AtomicBoolean();
        CountDownLatch finished = new CountDownLatch(1);
        // JVM shutdown hooks request drain, rather than assuming SIGINT interrupts main.
        Thread shutdown = new Thread(() -> {
            stopping.set(true);
            try { finished.await(31, TimeUnit.MINUTES); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }, "ingest-drain");
        Runtime.getRuntime().addShutdownHook(shutdown);
        SnowflakeStreamingIngestClient client = null;
        Map<Integer, CompletableFuture<Void>> pending = new LinkedHashMap<>();
        int nextId = 0;
        int confirmed = 0;
        int generation = 0;
        int attempts = 0;
        boolean complete = false;
        boolean waitingForCapacity = false;
        long deadline = System.nanoTime() + STALL_NANOS;

        try {
            client = createClient();
            SnowflakeStreamingIngestElasticChannel channel = client.getElasticChannel();

            while ((!stopping.get() && nextId < total) || !pending.isEmpty()) {
                try {

                    boolean progress = false;
                    Iterator<Map.Entry<Integer, CompletableFuture<Void>>> entries = pending.entrySet().iterator();
                    while (entries.hasNext()) {
                        Map.Entry<Integer, CompletableFuture<Void>> entry = entries.next();
                        if (!entry.getValue().isDone()) continue;
                        try { entry.getValue().get(); }
                        catch (ExecutionException failed) {
                            if (failed.getCause() instanceof SFException) throw (SFException) failed.getCause();
                            throw failed;
                        }
                        entries.remove();
                        confirmed++;
                        progress = true;
                    }
                    if (progress || (pending.isEmpty() && !waitingForCapacity)) deadline = System.nanoTime() + STALL_NANOS;
                    if (System.nanoTime() >= deadline) throw new TimeoutException("No durable progress for 30 minutes");
                    if (!stopping.get() && nextId < total && pending.size() < MAX_PENDING) {
                        int eventId = nextId;
                        // Replace the sample mapping with your retained source event.
                        pending.put(eventId, channel.appendRowWithWait(sampleRow(eventId), null));
                        nextId++;
                        waitingForCapacity = false;
                    } else {
                        Thread.sleep(10);
                    }
                } catch (SFException error) {
                    if (error.getHttpStatusCode() == 429) {
                        for (Map.Entry<Integer, CompletableFuture<Void>> entry : pending.entrySet()) {
                            if (!entry.getValue().isCompletedExceptionally()) continue;
                            try { entry.getValue().get(); }
                            catch (ExecutionException failed) {
                                if (failed.getCause() != error) continue;
                                int eventId = entry.getKey();
                                try {
                                    entry.setValue(channel.appendRowWithWait(sampleRow(eventId), null));
                                } catch (SFException retry) {
                                    if (retry.getHttpStatusCode() != 429) throw retry;
                                }
                            }
                        }
                        waitingForCapacity = true;
                        if (System.nanoTime() >= deadline) throw new TimeoutException("Backpressure persisted for 30 minutes");
                        Thread.sleep(250);
                        continue;
                    }
                    if (!invalid(error) || attempts++ >= 6) throw error;
                    Iterator<Map.Entry<Integer, CompletableFuture<Void>>> accepted = pending.entrySet().iterator();
                    while (accepted.hasNext()) {
                        CompletableFuture<Void> ack = accepted.next().getValue();
                        if (ack.isDone() && !ack.isCompletedExceptionally()) { accepted.remove(); confirmed++; }
                    }
                    try { client.close(false, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS); }
                    catch (SFException closing) { if (!invalid(closing)) throw closing; }
                    client = null;
                    generation++;
                    client = createClient();
                    channel = client.getElasticChannel();

                    System.err.println("Recreated client; replaying " + pending.size() + " unresolved rows; duplicates possible");
                    for (int eventId : new ArrayList<>(pending.keySet())) {
                        while (true) {
                            if (System.nanoTime() >= deadline) throw new TimeoutException("Recovery exceeded stalled-progress budget");
                            try {
                                pending.put(eventId, channel.appendRowWithWait(sampleRow(eventId), null));
                                break;
                            } catch (SFException retry) {
                                if (retry.getHttpStatusCode() != 429) throw retry;
                                Thread.sleep(250);
                            }
                        }
                    }
                }
            }
            complete = true;
            System.out.println("Durably acknowledged " + confirmed + " rows; submitted=" + nextId
                    + "; run=" + RUN_ID + "; stopped=" + stopping.get());
        } finally {
            try {
                if (!complete) System.err.println("Retain source for replay; confirmed=" + confirmed + ", submitted=" + nextId);
                if (client != null) client.close(complete, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            } finally {
                finished.countDown();
                try { Runtime.getRuntime().removeShutdownHook(shutdown); }
                catch (IllegalStateException shuttingDown) { /* Hook is already waiting for this drain. */ }
            }
        }
    }
    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    static Properties connectionProperties() {
        return connectionProperties(System::getenv);
    }

    static Properties connectionProperties(Function<String, String> env) {
        String pat = env.apply("SNOWFLAKE_PAT");
        if (pat == null || pat.isBlank()) {
            return null;
        }
        String account = env.apply("SNOWFLAKE_ACCOUNT");
        String url = env.apply("SNOWFLAKE_URL");
        if (account == null || account.isBlank() || url == null || url.isBlank()) {
            throw new IllegalArgumentException(
                    "PAT authentication requires SNOWFLAKE_ACCOUNT and SNOWFLAKE_URL");
        }
        Properties properties = new Properties();
        properties.setProperty("authorization_type", "PAT");
        properties.setProperty("personal_access_token", pat);
        properties.setProperty("account", account);
        properties.setProperty("url", url);
        String role = env.apply("SNOWFLAKE_ROLE");
        if (role != null && !role.isBlank()) {
            properties.setProperty("role", role);
        }
        return properties;
    }

    static SnowflakeStreamingIngestClient createClient() throws Exception {
        return openClient();
    }

    static SnowflakeStreamingIngestClient openClient() throws Exception {
        Properties properties = connectionProperties();
        if (properties == null) {
            Properties profileProperties = new Properties();
            JsonNode profile = new ObjectMapper().readTree(Files.readAllBytes(Paths.get(PROFILE)));
            profile.fields().forEachRemaining(
                    entry -> profileProperties.put(entry.getKey(), entry.getValue().asText()));
            properties = profileProperties;
        }
        return SnowflakeStreamingIngestClientFactory.tableBuilder(
                "ingest-" + UUID.randomUUID(), DATABASE, SCHEMA, TABLE)
                .setProperties(properties)
                .build();
    }

    static Map<String, Object> sampleRow(int eventId) {
        return Map.of(
                "EVENT_ID", eventId,
                "C1", eventId,
                "C2", RUN_ID + "-" + eventId);
    }

}
