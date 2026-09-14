package com.snowflake.example;

import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestElasticChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Append immediately; checkpoint all acknowledgements before source handoff.
 * The SDK owns batching. Caller timeouts keep the original Future alive.
 * Retain source events across restarts; Elastic replay may duplicate them.
 */
public class ElasticProducer {
    static class Pending {
        final ProductionSupport.Event event;
        final CompletableFuture<Void> future;
        final int generation;
        Pending(ProductionSupport.Event event, CompletableFuture<Void> future, int generation) {
            this.event = event;
            this.future = future;
            this.generation = generation;
        }
    }

    static class Session {
        final ProductionSupport.ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestElasticChannel channel;
        int generation;
        Session(ProductionSupport.ClientFactory factory) { this.factory = factory; }
        void open() throws Exception {
            SnowflakeStreamingIngestClient fresh = factory.create();
            try {
                channel = fresh.getElasticChannel();
            } catch (RuntimeException error) {
                fresh.close(false, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
                throw error;
            }
            client = fresh;
            generation++;
        }
        void recover(int failedGeneration) throws Exception {
            if (generation != failedGeneration) return;
            close(false);
            open();
        }
        void close(boolean flush) throws Exception {
            if (client == null) return;
            try {
                client.close(flush, Duration.ofSeconds(30)).get(30, TimeUnit.SECONDS);
            } finally {
                client = null;
            }
        }
    }

    static Pending submit(Session session, ProductionSupport.Event event, long deadline) throws Exception {
        for (int attempt = 0; attempt < ProductionSupport.MAX_ATTEMPTS; attempt++) {
            ProductionSupport.remaining(deadline);
            try {
                return new Pending(event, session.channel.appendRowWithWait(event.row, String.valueOf(event.offset)),
                        session.generation);
            } catch (SFException error) {
                if (!ProductionSupport.retryable(error) || attempt == ProductionSupport.MAX_ATTEMPTS - 1) throw error;
                if (ProductionSupport.invalidation(error)) session.recover(session.generation);
                ProductionSupport.backoff(attempt, deadline);
            }
        }
        throw new IllegalStateException("Submission retry budget exhausted");
    }

    static void checkpoint(Session session, List<Pending> pending, ProductionSupport.ReplaySource source,
                           long deadline) throws Exception {
        for (Pending original : pending) {
            Pending item = original;
            int retries = 0;
            while (true) {
                long budget = ProductionSupport.remaining(deadline);
                try {
                    item.future.get(Math.min(TimeUnit.SECONDS.toNanos(1), budget), TimeUnit.NANOSECONDS);
                    break;
                } catch (TimeoutException waiting) {
                    // Keep the original Future; a caller timeout is not an SDK failure.
                } catch (ExecutionException failure) {
                    Throwable cause = failure.getCause();
                    if (!(cause instanceof SFException)) throw failure;
                    SFException error = (SFException) cause;
                    if (!ProductionSupport.retryable(error) || retries >= ProductionSupport.MAX_ATTEMPTS - 1) throw error;
                    if (ProductionSupport.invalidation(error)) session.recover(item.generation);
                    System.err.println("Replaying EVENT_ID=" + item.event.offset + "; duplicates possible");
                    ProductionSupport.backoff(retries++, deadline);
                    item = submit(session, item.event, deadline);
                }
            }
        }
        if (!pending.isEmpty()) {
            source.acknowledge(pending.get(pending.size() - 1).event.offset);
            pending.clear();
        }
    }

    static void run(Session session, ProductionSupport.ReplaySource source) throws Exception {
        List<Pending> pending = new ArrayList<>();
        long checkpointAt = System.nanoTime() + ProductionSupport.CHECKPOINT_NANOS;
        long deadline = System.nanoTime() + ProductionSupport.OUTAGE_NANOS;
        ProductionSupport.Event event;
        while ((event = source.read()) != null) {
            pending.add(submit(session, event, deadline));
            if (pending.stream().anyMatch(item -> item.future.isCompletedExceptionally())
                    || pending.size() >= ProductionSupport.CHECKPOINT_ROWS || System.nanoTime() >= checkpointAt) {
                checkpoint(session, pending, source, deadline);
                checkpointAt = System.nanoTime() + ProductionSupport.CHECKPOINT_NANOS;
                deadline = System.nanoTime() + ProductionSupport.OUTAGE_NANOS;
            }
        }
        checkpoint(session, pending, source, deadline);
    }

    public static void main(String[] args) throws Exception {
        ProductionSupport.ReplaySource source = ProductionSupport.sourceFromEnv();
        Session session = new Session(ProductionSupport::createClient);
        boolean completed = false;
        try {
            session.open();
            run(session, source);
            completed = true;
            System.out.println("Durable source checkpoint: " + source.committed + "; materialization is separate");
        } finally {
            if (!completed) System.err.println("Retain source events after checkpoint " + source.committed);
            session.close(completed);
        }
    }
}
