# Design decisions

Line references below refer to the current source files.

1. **Atomic claim.** `JobStore.java:30-41` is one `UPDATE` whose nested selection finds
   the oldest pending or eligible failed job. Line 41 checks `executeUpdate() != 1`.
   SQLite allows one writer at a time even in WAL mode, so concurrent processes serialize
   at that statement: after the first commits, the second no longer sees that row as
   pending/failed. A select-then-update would have a TOCTOU window and is not used.

2. **SIGKILL recovery.** A claimed row is `processing` with a lease ending at now + 20s
   (`WorkerManager.java:16`, claim call in its loop). SIGKILL leaves that row unchanged;
   no PID inspection is needed. Every worker reaps expired leases on a 5s tick and
   `JobStore.java:61-73` increments attempts and makes it failed or dead. Worst case is
   20s lease + 5s tick = **25 seconds**, under one minute. Healthy long commands renew
   their lease every two seconds (`WorkerManager.java:43-45`).

3. **DLQ retry resets attempts.** `JobStore.java:75` sets attempts to zero. This is an
   operator-initiated fresh attempt, not continuation of the old failure streak; keeping
   old attempts would make it exhaust its new budget prematurely.

4. **Worker stop.** `WorkerManager.java:28-32` reads PID files and calls
   `ProcessHandle.destroy()` (SIGTERM-equivalent), then waits for the files to vanish.
   A control socket was rejected: it is another component that can crash-leak. A DB
   `stop_requested` flag was rejected: it adds up-to-poll-interval latency, whereas
   SIGTERM is the OS-native immediate signal. Shutdown hooks set the volatile flag, so
   the command is allowed to finish before the worker exits.

5. **Priority queues.** WAL, the single atomic `UPDATE`, leases, heartbeat, reaper,
   retries and PID signaling all survive. The selection at `JobStore.java:34-35` changes
   from `ORDER BY created_at ASC` to `ORDER BY priority DESC, created_at ASC`, requiring
   a `priority` column and a matching index.
