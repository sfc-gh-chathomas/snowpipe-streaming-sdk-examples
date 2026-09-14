package com.snowflake.example;

import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Single-writer named-channel producer. Stream rows immediately and retain source
 * events until their committed offset is confirmed. Outage pauses intake; only
 * SDK invalidation reopens. Do not share ownership of the same channel.
 */
public class NamedChannelCheckpoint {
    static final String CHANNEL_NAME = ProductionSupport.env("SNOWFLAKE_CHANNEL", "production-source-0");

    static long parseOffset(String token) { return token == null ? 0 : Long.parseLong(token); }

    static class Session {
        final ProductionSupport.ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestChannel channel;
        Session(ProductionSupport.ClientFactory factory) { this.factory = factory; }
        long open() throws Exception {
            if (client == null) client = factory.create();
            OpenChannelResult opened = client.openChannel(CHANNEL_NAME);
            channel = opened.getChannel();
            if (opened.getChannelStatus().getRowsErrorCount() > 0) {
                throw new IllegalStateException("Reconcile row errors before source handoff");
            }
            return parseOffset(opened.getChannelStatus().getLatestCommittedOffsetToken());
        }
        long recover(SFException error) throws Exception {
            if ("InvalidClientError".equals(error.getErrorCodeName())) {
                close(false);
            } else if (channel != null) {
                try {
                    channel.close(false, Duration.ofSeconds(30));
                } catch (SFException alreadyInvalid) {
                    // Reopen the named channel without dropping its committed offset.
                }
            }
            try {
                return open();
            } catch (SFException reopened) {
                if (!"InvalidClientError".equals(reopened.getErrorCodeName())
                        && !"ClosedClientError".equals(reopened.getErrorCodeName())) throw reopened;
                close(false);
                return open();
            }
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

    static void checkpoint(Session session, long target, ProductionSupport.ReplaySource source,
                           long deadline) throws Exception {
        while (true) {
            ProductionSupport.remaining(deadline);
            try {
                ChannelStatus status = session.channel.getChannelStatus();
                if (status.getRowsErrorCount() > 0) throw new IllegalStateException("Reconcile row errors before handoff");
                if (!"SUCCESS".equals(status.getStatusCode())) {
                    throw new SFException("InvalidChannelError", status.getStatusCode(), 409, "Conflict");
                }
                if (parseOffset(status.getLatestCommittedOffsetToken()) >= target) {
                    source.acknowledge(target);
                    return;
                }
            } catch (SFException error) {
                if (ProductionSupport.invalidation(error) || !ProductionSupport.retryable(error)) throw error;
            }
            ProductionSupport.backoff(2, deadline);
        }
    }

    static void run(Session session, ProductionSupport.ReplaySource source) throws Exception {
        source.seek(session.open());
        long submitted = source.committed;
        int outstanding = 0;
        int failures = 0;
        ProductionSupport.Event event = null;
        long deadline = System.nanoTime() + ProductionSupport.OUTAGE_NANOS;
        long checkpointAt = System.nanoTime() + ProductionSupport.CHECKPOINT_NANOS;
        while (true) {
            try {
                if (event == null) event = source.read();
                if (event == null) {
                    if (outstanding > 0) checkpoint(session, submitted, source, deadline);
                    return;
                }
                ProductionSupport.remaining(deadline);
                session.channel.appendRow(event.row, String.valueOf(event.offset));
                submitted = event.offset;
                event = null;
                outstanding++;
                if (outstanding >= ProductionSupport.CHECKPOINT_ROWS || System.nanoTime() >= checkpointAt) {
                    checkpoint(session, submitted, source, deadline);
                    outstanding = 0;
                    failures = 0;
                    deadline = System.nanoTime() + ProductionSupport.OUTAGE_NANOS;
                    checkpointAt = System.nanoTime() + ProductionSupport.CHECKPOINT_NANOS;
                }
            } catch (SFException error) {
                if (!ProductionSupport.retryable(error) || ++failures >= ProductionSupport.MAX_ATTEMPTS) throw error;
                if (ProductionSupport.invalidation(error)) {
                    source.seek(session.recover(error));
                    submitted = source.committed;
                    outstanding = 0;
                    event = null;
                }
                ProductionSupport.backoff(failures - 1, deadline);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ProductionSupport.ReplaySource source = ProductionSupport.sourceFromEnv();
        Session session = new Session(ProductionSupport::createClient);
        boolean completed = false;
        try {
            run(session, source);
            completed = true;
            System.out.println("Committed source checkpoint: " + source.committed);
        } finally {
            if (!completed) System.err.println("Retain source events after checkpoint " + source.committed);
            session.close(completed);
        }
    }
}
