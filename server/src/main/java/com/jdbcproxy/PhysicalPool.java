package com.jdbcproxy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Per-session pool of physical JDBC connections.
 *
 * <p>Concurrency model (each borrowed connection is exclusively owned by one
 * worker thread until released, so JDBC thread-safety rules are never violated):
 * <ul>
 *   <li><b>Reads first:</b> when connections are contended, the reader wait queue
 *       is served before the writer queue.</li>
 *   <li><b>Transaction pinning:</b> {@code begin} pins one physical connection
 *       until commit/rollback. At most {@code maxSize - reserveReads} connections
 *       can be pinned, so concurrent autocommit reads always have capacity.</li>
 *   <li><b>Global cap:</b> a shared semaphore bounds physical connections across
 *       all sessions.</li>
 *   <li><b>Statement cache:</b> per-connection LRU of PreparedStatements
 *       (avoids re-parsing on PostgreSQL etc.).</li>
 * </ul>
 */
final class PhysicalPool {

    interface ConnFactory {
        Connection create(); // throws ApiException (RuntimeException) on failure
    }

    private static final class Waiter {
        final boolean read;
        final boolean pin;
        PooledConn handed;

        Waiter(boolean read, boolean pin) {
            this.read = read;
            this.pin = pin;
        }
    }

    private final int maxSize;
    private final int pinLimit;          // maxSize - reserveReads
    private final long borrowTimeoutMs;
    private final long staleAfterMs;     // keepalive interval: probe idle conns before reuse (0 = off)
    private final Semaphore globalPermits;
    private final AtomicInteger livePhysical;
    private final ConnFactory factory;
    private final Consumer<Connection> stateApplier;
    private final java.util.function.Predicate<PooledConn> validator; // dialect ping, from the session

    private final Deque<PooledConn> idle = new ArrayDeque<>();
    private final Deque<Waiter> readers = new ArrayDeque<>();
    private final Deque<Waiter> writers = new ArrayDeque<>();
    private int created;                 // live physical connections owned by this pool
    private int pinned;                  // connections currently pinned by transactions
    private boolean closed;

    PhysicalPool(int maxSize, int reserveReads, long borrowTimeoutMs,
                 Semaphore globalPermits, AtomicInteger livePhysical,
                 ConnFactory factory, Consumer<Connection> stateApplier,
                 long staleAfterMs, java.util.function.Predicate<PooledConn> validator) {
        this.maxSize = Math.max(1, maxSize);
        int reserve = Math.max(0, Math.min(reserveReads, this.maxSize - 1));
        this.pinLimit = this.maxSize - reserve;
        this.borrowTimeoutMs = Math.max(1000, borrowTimeoutMs);
        this.staleAfterMs = Math.max(0, staleAfterMs);
        this.globalPermits = globalPermits;
        this.livePhysical = livePhysical;
        this.factory = factory;
        this.stateApplier = stateApplier;
        this.validator = validator;
    }

    // ---------------------------------------------------------------- borrow / release

    /**
     * Borrows a connection exclusively. {@code pin=true} is for transaction
     * owners and additionally respects the read-reservation limit.
     *
     * @throws ApiException 503 on timeout / closed pool / global cap
     */
    PooledConn borrow(boolean read, boolean pin) {
        long deadline = System.currentTimeMillis() + borrowTimeoutMs;
        boolean permitHeld = false;
        try {
            if (globalPermits != null) {
                long remain = Math.max(1, deadline - System.currentTimeMillis());
                try {
                    permitHeld = globalPermits.tryAcquire(remain, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new ApiException(503, "interrupted while waiting for connection capacity");
                }
                if (!permitHeld) {
                    throw new ApiException(503, "global physical connection limit reached ("
                            + livePhysical.get() + " live); close idle sessions or raise --max-physical");
                }
            }

            // take an idle connection, probing ones that went stale while unused
            PooledConn pc = null;
            while (true) {
                synchronized (this) {
                    if (closed) throw new ApiException(503, "session closed");
                    pc = pollEligible(pin);
                }
                if (pc == null) break;
                if (staleAfterMs > 0
                        && System.currentTimeMillis() - pc.idleSince >= staleAfterMs
                        && !validateQuietly(pc)) {
                    closePhysical(pc); // dead while idle: drop and try the next one
                    continue;
                }
                releasePermit(permitHeld);
                permitHeld = false;
                return pc;
            }
            boolean grow;
            synchronized (this) {
                if (closed) throw new ApiException(503, "session closed");
                grow = created < maxSize && (!pin || pinned < pinLimit);
                if (grow) created++; // reserve the slot before the (slow) connect
            }
            if (grow) {
                try {
                    PooledConn fresh = new PooledConn(factory.create());
                    if (livePhysical != null) livePhysical.incrementAndGet();
                    // the held permit now backs this long-lived connection
                    permitHeld = false;
                    return fresh;
                } catch (RuntimeException e) {
                    synchronized (this) { created--; }
                    throw e;
                }
            }

            // nothing available: wait in the priority queue (readers served first)
            Waiter w = new Waiter(read, pin);
            synchronized (this) {
                if (closed) throw new ApiException(503, "session closed");
                (read ? readers : writers).addLast(w);
                long remain = deadline - System.currentTimeMillis();
                while (w.handed == null && remain > 0 && !closed) {
                    try {
                        this.wait(Math.max(1, remain));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    remain = deadline - System.currentTimeMillis();
                }
                if (w.handed == null) {
                    readers.remove(w);
                    writers.remove(w);
                }
            }
            if (w.handed != null) {
                releasePermit(permitHeld);
                permitHeld = false;
                return w.handed;
            }
            if (closed) throw new ApiException(503, "session closed");
            throw new ApiException(503, "connection pool exhausted: " + maxSize
                    + " connection(s) busy for over " + borrowTimeoutMs + " ms (reads are served first)");
        } finally {
            releasePermit(permitHeld);
        }
    }

    /** Returns a connection to the pool; hands it straight to the next waiter (readers first). */
    void release(PooledConn pc) {
        boolean doClose;
        synchronized (this) {
            if (closed) {
                doClose = true;
            } else {
                Waiter w = takeEligibleWaiter();
                pc.idleSince = System.currentTimeMillis();
                if (w != null) {
                    if (w.pin) pinned++;
                    w.handed = pc;
                    notifyAll();
                    return;
                }
                idle.addFirst(pc); // LIFO keeps hot connections hot
                return;
            }
        }
        closePhysical(pc);
    }

    /** Release with defensive transaction cleanup (drivers may leave state behind). */
    void releaseSafely(PooledConn pc) {
        try {
            if (!pc.conn.getAutoCommit()) {
                pc.conn.rollback();
                pc.conn.setAutoCommit(true);
            }
        } catch (SQLException ignored) {
            // conn may be dead; release() / close paths cope with that
        }
        release(pc);
    }

    /** Destroys a connection (dead, session closing, ...). Safe to call twice. */
    void discard(PooledConn pc) {
        closePhysical(pc);
    }

    // ---------------------------------------------------------------- lifecycle / stats

    void closeAll() {
        List<PooledConn> toClose = new ArrayList<>();
        synchronized (this) {
            if (closed) return;
            closed = true;
            toClose.addAll(idle);
            idle.clear();
            notifyAll(); // wake waiters so they fail fast with "session closed"
        }
        for (PooledConn pc : toClose) {
            closePhysical(pc);
        }
        // connections currently borrowed out are closed by their owner via release()/discard()
    }

    /** Re-applies session state (isolation etc.) to idle connections. */
    void applyStateToIdle() {
        List<PooledConn> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(idle);
        }
        for (PooledConn pc : snapshot) {
            try {
                stateApplier.accept(pc.conn);
            } catch (RuntimeException ignored) {
            }
        }
    }

    /**
     * Background keepalive: probes idle connections untouched for at least
     * {@code staleAfterMillis}; drops the dead ones so later borrows never see
     * them. Runs outside the pool lock except for atomic take/put of each probe.
     *
     * @return number of dead connections removed
     */
    int sweepIdle() {
        if (staleAfterMs <= 0 || closed) return 0;
        long cutoff = System.currentTimeMillis() - staleAfterMs;
        int dropped = 0;
        PooledConn pc;
        while ((pc = takeIdleOlderThan(cutoff)) != null) {
            if (validateQuietly(pc)) {
                synchronized (this) {
                    if (closed) {
                        closePhysical(pc);
                    } else {
                        pc.idleSince = System.currentTimeMillis();
                        idle.addFirst(pc);
                    }
                }
            } else {
                closePhysical(pc);
                dropped++;
            }
        }
        return dropped;
    }

    private synchronized PooledConn takeIdleOlderThan(long cutoffMillis) {
        if (closed) return null;
        Iterator<PooledConn> it = idle.iterator();
        while (it.hasNext()) {
            PooledConn pc = it.next();
            if (pc.idleSince <= cutoffMillis) {
                it.remove();
                return pc;
            }
        }
        return null;
    }

    private boolean validateQuietly(PooledConn pc) {
        try {
            if (validator != null && validator.test(pc)) {
                pc.lastValidatedAt = System.currentTimeMillis();
                return true;
            }
        } catch (RuntimeException ignored) {
            // validator failure counts as a dead connection
        }
        return false;
    }

    synchronized int idleCount() {
        return idle.size();
    }

    synchronized int createdCount() {
        return created;
    }

    synchronized int pinnedCount() {
        return pinned;
    }

    int inUseCount() {
        return Math.max(0, createdCount() - idleCount());
    }

    // ---------------------------------------------------------------- internals

    private PooledConn pollEligible(boolean pin) { // caller holds monitor
        if (idle.isEmpty()) return null;
        if (pin && pinned >= pinLimit) return null; // keep read reservation intact
        PooledConn pc = idle.pollFirst();
        if (pin) pinned++;
        return pc;
    }

    private Waiter takeEligibleWaiter() { // caller holds monitor
        Waiter w = readers.pollFirst(); // reads first
        if (w != null) return w;
        Iterator<Waiter> it = writers.iterator();
        while (it.hasNext()) {
            Waiter c = it.next();
            if (!c.pin || pinned < pinLimit) {
                it.remove();
                return c;
            }
        }
        return null;
    }

    private void closePhysical(PooledConn pc) {
        synchronized (this) {
            if (pc.isDiscarded()) return;
            pc.markDiscarded();
            created--;
        }
        for (PreparedStatement ps : pc.psCache.values()) {
            closeQuietly(ps);
        }
        pc.psCache.clear();
        try {
            pc.conn.close();
        } catch (SQLException ignored) {
        }
        releasePermit(true);
        if (livePhysical != null) livePhysical.decrementAndGet();
    }

    private void releasePermit(boolean held) {
        if (held && globalPermits != null) {
            globalPermits.release();
        }
    }

    private static void closeQuietly(PreparedStatement ps) {
        try {
            ps.close();
        } catch (SQLException ignored) {
        }
    }
}
