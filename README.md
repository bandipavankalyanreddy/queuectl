# queuectl

A small, durable CLI job queue for Java 17+. It uses one SQLite file and no framework.
The only dependency is `sqlite-jdbc`, which provides the embedded SQLite driver.

Reference video: [Google Drive](https://drive.google.com/file/d/1JmEqbhOajmXe6g6hbphbACNRdKXZTG4v/view?usp=drive_link)

## Run

Install Maven, then build and run from the project directory:

```powershell
mvn package
java -jar target/queuectl-1.0.0.jar enqueue '{"id":"job1","command":"sleep 2"}'
java -jar target/queuectl-1.0.0.jar worker start --count 3
```

On Windows, use commands valid in `cmd.exe`, for example `timeout /t 2`.

## Commands

```text
queuectl enqueue '{"id":"job1","command":"sleep 2"}'
queuectl worker start --count 3
queuectl worker stop
queuectl status
queuectl list --state pending
queuectl list --state pending --json
queuectl dlq list
queuectl dlq retry job1
queuectl config set max-retries 3
queuectl config set backoff-base 2
```

`--json` writes only the JSON array to stdout. Worker diagnostics go to stderr.

## Architecture

```text
enqueue -> SQLite pending row -> atomic UPDATE claim -> command process
                                              |              |
                                         lease + heartbeat   +-- success: completed
                                              |              +-- failure: delayed failed / dead DLQ
                                         expired lease -> reaper -> failed / dead DLQ
```

SQLite is put in WAL mode. Worker threads are deliberately used instead of child JVMs:
one foreground JVM is easier to stop cleanly, shares no mutable in-memory queue state,
and each operation still uses a separate SQLite connection, so cross-process workers
remain safe.

Configuration is read when a job is claimed. A changed `max-retries` applies to future
claims; its value is snapshotted into `jobs.max_retries` while processing so a failure
and a later lease reaper use the same retry budget. The backoff is `base ^ attempts`
seconds. A manually retried DLQ job resets attempts to zero.

## Manual checks

1. Enqueue `echo ok`, start one worker, then use `status` and `list --state completed`.
2. Enqueue a command that exits nonzero; observe `failed`, wait for its backoff, and
   observe retries until `dead`.
3. Start a long command, kill the worker process forcibly, wait 25 seconds maximum,
   then start a worker and observe recovery.
4. Start workers in one terminal and run `worker stop` in another.

To record a demo, run the commands above in two terminals and capture both panes; link
the resulting video from your submission or README fork.
