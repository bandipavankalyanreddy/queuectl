package com.queuectl.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/** Opens the per-working-directory database and creates its schema on first use. */
public final class Database {
    private final String url;

    public Database(Path queueDirectory) {
        try { Files.createDirectories(queueDirectory); }
        catch (IOException e) { throw new IllegalStateException("Cannot create " + queueDirectory, e); }
        this.url = "jdbc:sqlite:" + queueDirectory.resolve("queue.db").toAbsolutePath();
        initialize();
    }

    public Connection connect() throws SQLException {
        Connection connection = DriverManager.getConnection(url);
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA busy_timeout=5000");
            statement.execute("PRAGMA foreign_keys=ON");
        }
        return connection;
    }

    private void initialize() {
        try (Connection connection = connect(); Statement s = connection.createStatement()) {
            s.execute("PRAGMA journal_mode=WAL");
            s.execute("""
                    CREATE TABLE IF NOT EXISTS jobs (
                      id TEXT PRIMARY KEY, command TEXT NOT NULL, state TEXT NOT NULL,
                      attempts INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL, backoff_base INTEGER NOT NULL DEFAULT 2,
                      created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
                      claimed_by TEXT, lease_expires_at INTEGER, next_retry_at INTEGER
                    )""");
            s.execute("CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs(state, next_retry_at, created_at)");
            s.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL)");
            s.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('max-retries', '3')");
            s.execute("INSERT OR IGNORE INTO config(key, value) VALUES ('backoff-base', '2')");
        } catch (SQLException e) { throw new IllegalStateException("Cannot initialize queue database", e); }
    }
}
