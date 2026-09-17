package com.snowflake.example;

import com.snowflake.ingest.streaming.ErrorDetail;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SuccessDetail;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

/**
 * Elastic step 3 using {@code appendRow} plus success/error handlers.
 *
 * <p>The handlers only stamp the pending event; they never checkpoint, retry,
 * or close a client. The ingest loop inspects those stamps without waiting
 * during intake.
 */
public class ElasticStep3Callbacks {
    static final class Pending {
        final ElasticStep3.Event event;
        final SnowflakeStreamingIngestClient submittedBy;
        volatile boolean ok;
        volatile SFException error;

        Pending(ElasticStep3.Event event, SnowflakeStreamingIngestClient submittedBy) {
            this.event = event;
            this.submittedBy = submittedBy;
        }

        boolean succeeded() {
            return ok && error == null;
        }

        SFException error() {
            return error;
        }
    }

    static void installHandlers(ElasticStep3.Session session) {
        session.channel.setSuccessHandler(ElasticStep3Callbacks::onSuccess);
        session.channel.setErrorHandler(ElasticStep3Callbacks::onError);
    }

    static void onSuccess(SuccessDetail detail) {
        // Stamp only. Checkpointing and retries stay on the ingest thread.
        for (Object token : detail.getAppendTokens()) {
            ((Pending) token).ok = true;
        }
    }

    static void onError(ErrorDetail detail) {
        for (Object token : detail.getAppendTokens()) {
            ((Pending) token).error = detail.getError();
        }
    }

    static ElasticStep3.Session open(ElasticStep3.ClientFactory factory) throws Exception {
        ElasticStep3.Session session = ElasticStep3.open(factory);
        try {
            installHandlers(session);
            return session;
        } catch (Exception error) {
            try {
                ElasticStep3.closeClient(session.client, false);
            } catch (Exception closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
    }

    static ElasticStep3.Session replace(
            ElasticStep3.ClientFactory factory,
            ElasticStep3.Session current,
            SnowflakeStreamingIngestClient old) throws Exception {
        ElasticStep3.Session session = ElasticStep3.replace(factory, current, old);
        if (session == current) {
            return session;
        }
        try {
            installHandlers(session);
            return session;
        } catch (Exception error) {
            try {
                ElasticStep3.closeClient(session.client, false);
            } catch (Exception closeError) {
                error.addSuppressed(closeError);
            }
            throw error;
        }
    }

    static Pending submit(ElasticStep3.Session session, ElasticStep3.Event event) {
        Pending pending = new Pending(event, session.client);
        session.channel.appendRow(event.row, pending);
        return pending;
    }

    static void resubmit(
            ElasticStep3.Session session,
            Deque<Pending> pending,
            SnowflakeStreamingIngestClient old) {
        Deque<Pending> next = new ArrayDeque<>();
        for (Pending item : pending) {
            if (item.succeeded() || item.submittedBy != old) {
                next.addLast(item);
            } else {
                next.addLast(submit(session, item.event));
            }
        }
        pending.clear();
        pending.addAll(next);
    }

    static SFException invalidationOfCurrent(
            Deque<Pending> pending, SnowflakeStreamingIngestClient current) {
        for (Pending item : pending) {
            SFException error = item.error();
            if (error != null
                    && ElasticStep3.isInvalidation(error)
                    && item.submittedBy == current) {
                return error;
            }
        }
        return null;
    }

    static int collectPrefix(
            Deque<Pending> pending, ElasticStep3.SampleEventSource source) {
        int confirmed = 0;
        while (!pending.isEmpty() && pending.peekFirst().succeeded()) {
            source.acknowledge(pending.removeFirst().event.position);
            confirmed++;
        }
        return confirmed;
    }

    static void run(
            ElasticStep3.ClientFactory factory,
            ElasticStep3.SampleEventSource source) throws Exception {
        run(factory, source, ElasticStep3.MAX_PENDING_EVENTS, ElasticStep3.MAX_NO_PROGRESS_NANOS);
    }

    static void run(
            ElasticStep3.ClientFactory factory,
            ElasticStep3.SampleEventSource source,
            int maxPending,
            long stallNanos) throws Exception {
        ElasticStep3.Session session = open(factory);
        Deque<Pending> pending = new ArrayDeque<>();
        ElasticStep3.Event event = null;
        boolean atFront = false;
        boolean exhausted = false;
        int failures = 0;
        long deadline = System.nanoTime() + stallNanos;
        boolean completed = false;
        try {
            while (true) {
                // Never wait on an unfinished ack during intake.
                if (collectPrefix(pending, source) > 0) {
                    deadline = System.nanoTime() + stallNanos;
                    failures = 0;
                }

                SFException invalidation = invalidationOfCurrent(pending, session.client);
                if (invalidation != null) {
                    if (++failures >= ElasticStep3.MAX_ATTEMPTS) {
                        throw invalidation;
                    }
                    SnowflakeStreamingIngestClient old = session.client;
                    session = replace(factory, session, old);
                    resubmit(session, pending, old);
                    ElasticStep3.backoff(failures, deadline);
                    continue;
                }

                Pending head = pending.peekFirst();
                SFException headError = head == null ? null : head.error();
                if (headError != null) {
                    if (!ElasticStep3.isRetryable(headError)
                            || ++failures >= ElasticStep3.MAX_ATTEMPTS) {
                        throw headError;
                    }
                    ElasticStep3.backoff(failures, deadline);
                    // Re-submit this event in front of later accepted work.
                    event = head.event;
                    pending.removeFirst();
                    atFront = true;
                    continue;
                }

                if (pending.isEmpty() && event == null) {
                    deadline = System.nanoTime() + stallNanos;
                    if (exhausted) {
                        completed = true;
                        return;
                    }
                }

                ElasticStep3.remaining(deadline);
                if (pending.size() >= maxPending || (exhausted && event == null)) {
                    TimeUnit.NANOSECONDS.sleep(
                            Math.min(ElasticStep3.POLL_NANOS, ElasticStep3.remaining(deadline)));
                    continue;
                }

                if (event == null) {
                    event = source.read();
                    if (event == null) {
                        exhausted = true;
                        continue;
                    }
                }

                try {
                    Pending submitted = submit(session, event);
                    if (atFront) {
                        pending.addFirst(submitted);
                    } else {
                        pending.addLast(submitted);
                    }
                    event = null;
                    atFront = false;
                } catch (SFException error) {
                    if (!ElasticStep3.isRetryable(error)) {
                        throw error;
                    }
                    if (ElasticStep3.isBackpressure(error)) {
                        ElasticStep3.backoff(1, deadline);
                        continue;
                    }
                    if (ElasticStep3.isInvalidation(error)) {
                        if (++failures >= ElasticStep3.MAX_ATTEMPTS) {
                            throw error;
                        }
                        SnowflakeStreamingIngestClient old = session.client;
                        session = replace(factory, session, old);
                        resubmit(session, pending, old);
                        ElasticStep3.backoff(failures, deadline);
                        continue;
                    }
                    if (++failures >= ElasticStep3.MAX_ATTEMPTS) {
                        throw error;
                    }
                    ElasticStep3.backoff(failures, deadline);
                }
            }
        } finally {
            ElasticStep3.closeClient(session.client, completed);
        }
    }

    public static void main(String[] args) throws Exception {
        ElasticStep3.SampleEventSource source = new ElasticStep3.SampleEventSource(
                Long.parseLong(ElasticStep3.env("SNOWFLAKE_TEST_ROWS", "10000")),
                Long.parseLong(ElasticStep3.env("SNOWFLAKE_SOURCE_CHECKPOINT", "0")));
        try {
            run(ElasticStep3::createClient, source);
            System.out.println("Committed source checkpoint: " + source.committed);
        } catch (Exception error) {
            System.err.println(
                    "Retain source events after checkpoint " + source.committed);
            throw error;
        }
    }
}
