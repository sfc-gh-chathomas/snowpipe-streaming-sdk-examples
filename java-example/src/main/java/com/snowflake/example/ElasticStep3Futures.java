package com.snowflake.example;

import com.snowflake.ingest.streaming.SFException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Elastic step 3 using {@code appendRowWithWait} Futures.
 *
 * <p>Append one retained event at a time and keep its Future until durability
 * is confirmed. Checkpoint only a contiguous successful prefix. The SDK owns
 * transport batching.
 */
public class ElasticStep3Futures {
    static final class Pending {
        final ElasticStep3.Event event;
        CompletableFuture<Void> ack;

        Pending(ElasticStep3.Event event) {
            this.event = event;
        }

        boolean succeeded() {
            return ack.isDone() && !ack.isCompletedExceptionally();
        }

        SFException error() {
            return ack.isDone() && ack.isCompletedExceptionally()
                    ? ElasticStep3.failure(ack)
                    : null;
        }
    }

    static Pending submit(ElasticStep3.Session session, ElasticStep3.Event event) {
        Pending pending = new Pending(event);
        // Null token: we wait on the Future, not a success/error handler.
        pending.ack = session.channel.appendRowWithWait(event.row, null);
        return pending;
    }

    static void resubmit(ElasticStep3.Session session, Deque<Pending> pending) {
        Deque<Pending> next = new ArrayDeque<>();
        for (Pending item : pending) {
            // Durability already confirmed for this row; replay would only duplicate it.
            if (item.succeeded()) {
                next.addLast(item);
            } else {
                next.addLast(submit(session, item.event));
            }
        }
        pending.clear();
        pending.addAll(next);
    }

    static SFException findInvalidation(Deque<Pending> pending) {
        for (Pending item : pending) {
            SFException error = item.error();
            // The live handle is dead even if this error is not at the checkpoint head.
            if (error != null && ElasticStep3.isInvalidation(error)) {
                return error;
            }
        }
        return null;
    }

    static void recover(
            ElasticStep3.ClientFactory factory,
            ElasticStep3.Session session,
            Deque<Pending> pending,
            int failures,
            long deadline) throws Exception {
        ElasticStep3.reopen(factory, session);
        resubmit(session, pending);
        ElasticStep3.backoff(failures, deadline);
    }

    static int collectPrefix(
            Deque<Pending> pending, ElasticStep3.SampleEventSource source) {
        int confirmed = 0;
        // Stop at the first gap. A later success cannot skip an earlier unresolved event.
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
        ElasticStep3.Session session = ElasticStep3.open(factory);
        Deque<Pending> pending = new ArrayDeque<>();
        ElasticStep3.Event event = null; // held until append returns; 429 retries this same row
        boolean atFront = false; // retry must stay ahead of later pending acks
        boolean exhausted = false;
        int failures = 0;
        long deadline = System.nanoTime() + stallNanos;
        boolean completed = false;
        try {
            while (true) {
                // Collect finished acks without waiting. Waiting happens only if intake is paused.
                if (collectPrefix(pending, source) > 0) {
                    // Stall timer follows checkpoint progress, not successful submits.
                    deadline = System.nanoTime() + stallNanos;
                    failures = 0;
                }

                SFException invalidation = findInvalidation(pending);
                if (invalidation != null) {
                    if (++failures >= ElasticStep3.MAX_ATTEMPTS) {
                        throw invalidation;
                    }
                    recover(factory, session, pending, failures, deadline);
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
                    // Re-enter the submit path so 429/invalidation handling stays in one place.
                    event = head.event;
                    pending.removeFirst();
                    atFront = true;
                    continue;
                }

                if (pending.isEmpty() && event == null) {
                    // Caught up: no outstanding or held event, so the stall timer does not apply.
                    deadline = System.nanoTime() + stallNanos;
                    if (exhausted) {
                        completed = true;
                        return;
                    }
                }

                ElasticStep3.failIfStalled(deadline);
                // Pause at the pending cap, or after EOF while acks are still in flight.
                // event != null means we still hold a row to retry, so keep going.
                if (pending.size() >= maxPending || (exhausted && event == null)) {
                    TimeUnit.NANOSECONDS.sleep(
                            Math.min(ElasticStep3.POLL_NANOS, ElasticStep3.nanosLeft(deadline)));
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
                        // Keep this event and every earlier Future. Do not open a new client.
                        ElasticStep3.backoff(1, deadline); // first delay step; 429 is not a failed attempt
                        continue;
                    }
                    if (ElasticStep3.isInvalidation(error)) {
                        if (++failures >= ElasticStep3.MAX_ATTEMPTS) {
                            throw error;
                        }
                        recover(factory, session, pending, failures, deadline);
                        continue;
                    }
                    if (++failures >= ElasticStep3.MAX_ATTEMPTS) {
                        throw error;
                    }
                    ElasticStep3.backoff(failures, deadline);
                }
            }
        } finally {
            // Flush only when every accepted event was checkpointed.
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
