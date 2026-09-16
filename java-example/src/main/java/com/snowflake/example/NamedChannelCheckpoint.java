package com.snowflake.example;

import static com.snowflake.example.ElasticStep3Production.MAX_ATTEMPTS;
import static com.snowflake.example.ElasticStep3Production.MAX_NO_PROGRESS_NANOS;
import static com.snowflake.example.ElasticStep3Production.MAX_PENDING_EVENTS;
import static com.snowflake.example.ElasticStep3Production.POLL_NANOS;
import static com.snowflake.example.ElasticStep3Production.backoff;
import static com.snowflake.example.ElasticStep3Production.isInvalidation;
import static com.snowflake.example.ElasticStep3Production.isRetryable;
import static com.snowflake.example.ElasticStep3Production.remaining;

import com.snowflake.example.ElasticStep3Production.ClientFactory;
import com.snowflake.example.ElasticStep3Production.SampleEventSource;
import com.snowflake.ingest.streaming.ChannelStatus;
import com.snowflake.ingest.streaming.OpenChannelResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Coordinate retained source offsets with one stable named channel.
 *
 * <p>The offset token is checkpoint metadata, not a deduplication key. Give
 * each channel name one owner and retain source events until committed progress
 * is confirmed.
 */
public class NamedChannelCheckpoint {
    static final String CHANNEL_NAME = ElasticStep3Production.env(
            "SNOWFLAKE_CHANNEL", "production-source-0");
    static final int CHECKPOINT_ROWS = 1_000;
    static final long CHECKPOINT_NANOS = TimeUnit.SECONDS.toNanos(5);

    static long parseOffset(String token) {
        return token == null ? 0 : Long.parseLong(token);
    }

    static class NamedProducer {
        final ClientFactory factory;
        SnowflakeStreamingIngestClient client;
        SnowflakeStreamingIngestChannel channel;

        NamedProducer(ClientFactory factory) {
            this.factory = factory;
        }

        long open() throws Exception {
            if (client == null) {
                client = factory.create();
            }
            OpenChannelResult opened = client.openChannel(CHANNEL_NAME);
            channel = opened.getChannel();
            if (opened.getChannelStatus().getRowsErrorCount() > 0) {
                throw new IllegalStateException(
                        "Row errors require reconciliation before source handoff");
            }
            return parseOffset(
                    opened.getChannelStatus().getLatestCommittedOffsetToken());
        }

        long recover(SFException error) throws Exception {
            // Reopen without replacing the server-side committed offset.
            if ("InvalidClientError".equals(error.getErrorCodeName())) {
                close(false);
            } else if (channel != null) {
                try {
                    channel.close(false, Duration.ZERO);
                } catch (SFException alreadyClosed) {
                    // The local handle is already unusable; the named channel still exists.
                }
            }

            try {
                return open();
            } catch (SFException reopened) {
                String code = reopened.getErrorCodeName();
                if (!"InvalidClientError".equals(code)
                        && !"ClosedClientError".equals(code)) {
                    throw reopened;
                }
                close(false);
                return open();
            }
        }

        void close(boolean flush) throws Exception {
            if (client == null) {
                return;
            }
            try {
                client.close(flush, Duration.ofSeconds(30)).get(
                        30, TimeUnit.SECONDS);
            } finally {
                client = null;
                channel = null;
            }
        }
    }

    static void collectProgress(
            NamedProducer producer, long submitted, SampleEventSource source) {
        ChannelStatus status = producer.channel.getChannelStatus();
        if (status.getRowsErrorCount() > 0) {
            throw new IllegalStateException(
                    "Row errors require reconciliation before source handoff");
        }
        if (!"SUCCESS".equals(status.getStatusCode())) {
            throw new SFException(
                    "InvalidChannelError", status.getStatusCode(), 409, "Conflict");
        }

        long committed = Math.min(
                submitted, parseOffset(status.getLatestCommittedOffsetToken()));
        if (committed > source.committed) {
            source.acknowledge(committed);
        }
    }

    static void run(NamedProducer producer, SampleEventSource source) throws Exception {
        // Snowflake's committed token determines where this retained source resumes.
        source.seek(producer.open());
        long submitted = source.committed;
        ElasticStep3Production.Event event = null;
        boolean exhausted = false;
        int failures = 0;
        int rowsSincePoll = 0;
        long nextPoll = System.nanoTime() + CHECKPOINT_NANOS;
        long deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;

        while (true) {
            try {
                boolean outstanding = submitted > source.committed;
                boolean shouldPoll = outstanding
                        && (exhausted
                        || event != null
                        || rowsSincePoll >= CHECKPOINT_ROWS
                        || System.nanoTime() >= nextPoll
                        || submitted - source.committed >= MAX_PENDING_EVENTS);
                if (shouldPoll) {
                    long previous = source.committed;
                    collectProgress(producer, submitted, source);
                    if (source.committed > previous) {
                        deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                        failures = 0;
                    }
                    rowsSincePoll = 0;
                    nextPoll = System.nanoTime() + CHECKPOINT_NANOS;
                }

                if (submitted == source.committed && event == null) {
                    deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                    if (exhausted) {
                        return;
                    }
                }

                remaining(deadline);
                if (exhausted
                        || submitted - source.committed >= MAX_PENDING_EVENTS) {
                    TimeUnit.NANOSECONDS.sleep(
                            Math.min(POLL_NANOS, remaining(deadline)));
                    continue;
                }

                if (event == null) {
                    event = source.read();
                }
                if (event == null) {
                    exhausted = true;
                    continue;
                }

                producer.channel.appendRow(
                        event.row, String.valueOf(event.offset));
                submitted = event.offset;
                rowsSincePoll++;
                event = null;
            } catch (SFException error) {
                if (!isRetryable(error)) {
                    throw error;
                }
                if (error.getHttpStatusCode() != 429
                        && ++failures >= MAX_ATTEMPTS) {
                    throw error;
                }
                if (isInvalidation(error)) {
                    long previous = source.committed;
                    source.seek(producer.recover(error));
                    if (source.committed > previous) {
                        deadline = System.nanoTime() + MAX_NO_PROGRESS_NANOS;
                    }
                    submitted = source.committed;
                    event = null;
                    exhausted = false;
                }
                backoff(2, deadline);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        SampleEventSource source = new SampleEventSource(
                Long.parseLong(ElasticStep3Production.env(
                        "SNOWFLAKE_TEST_ROWS", "10000")),
                0);
        NamedProducer producer = new NamedProducer(
                ElasticStep3Production::createClient);
        boolean completed = false;
        try {
            run(producer, source);
            completed = true;
            System.out.println(
                    "Committed source checkpoint: " + source.committed);
        } finally {
            if (!completed) {
                System.err.println(
                        "Retain source events after checkpoint " + source.committed);
            }
            producer.close(completed);
        }
    }
}
