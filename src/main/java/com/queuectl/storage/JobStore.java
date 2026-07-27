package com.queuectl.storage;

import com.queuectl.core.Job;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** All SQL lives here so concurrency-sensitive operations are easy to audit. */
public final class JobStore {
    private final Database database;
    public JobStore(Database database) { this.database = database; }

    public void enqueue(String id, String command, int maxRetries) throws SQLException {
        long now = System.currentTimeMillis();
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement(
                "INSERT INTO jobs(id,command,state,attempts,max_retries,created_at,updated_at) VALUES(?,?, 'pending',0,?,?,?)")) {
            p.setString(1, id); p.setString(2, command); p.setInt(3, maxRetries); p.setLong(4, now); p.setLong(5, now); p.executeUpdate();
        }
    }

    public List<Job> list(String state) throws SQLException {
        String sql = state == null ? "SELECT * FROM jobs ORDER BY created_at" : "SELECT * FROM jobs WHERE state=? ORDER BY created_at";
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement(sql)) {
            if (state != null) p.setString(1, state);
            try (ResultSet r = p.executeQuery()) { List<Job> result = new ArrayList<>(); while (r.next()) result.add(row(r)); return result; }
        }
    }

    public Optional<Job> claimNext(String workerId, long leaseExpiresAt, long now, int maxRetries, int backoffBase) throws SQLException {
        /* One statement, never SELECT then UPDATE: SQLite serializes writers across processes. */
        String sql = """
                UPDATE jobs SET state='processing', claimed_by=?, lease_expires_at=?, updated_at=?, max_retries=?, backoff_base=?
                WHERE id=(SELECT id FROM jobs WHERE state='pending' OR (state='failed' AND next_retry_at<=?)
                          ORDER BY created_at ASC LIMIT 1)
                  AND state IN ('pending','failed')
                """;
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, workerId); p.setLong(2, leaseExpiresAt); p.setLong(3, now); p.setInt(4,maxRetries); p.setInt(5,backoffBase); p.setLong(6, now);
            // A row count of one proves this worker won the atomic claim.
            if (p.executeUpdate() != 1) return Optional.empty();
            try (PreparedStatement q=c.prepareStatement("SELECT * FROM jobs WHERE claimed_by=? AND state='processing' ORDER BY updated_at DESC LIMIT 1")) {
                q.setString(1,workerId); try(ResultSet r=q.executeQuery()){if(!r.next()) throw new SQLException("claimed job disappeared"); return Optional.of(row(r));}
            }
        }
    }

    public void complete(String id) throws SQLException { update("UPDATE jobs SET state='completed',updated_at=?,claimed_by=NULL,lease_expires_at=NULL WHERE id=?", id); }
    /** Heartbeat prevents a healthy long-running command from being mistaken for a crash. */
    public void renewLease(String id, String workerId, long leaseExpiresAt) throws SQLException {
        try(Connection c=database.connect(); PreparedStatement p=c.prepareStatement("UPDATE jobs SET lease_expires_at=?,updated_at=? WHERE id=? AND state='processing' AND claimed_by=?")) {
            p.setLong(1,leaseExpiresAt); p.setLong(2,System.currentTimeMillis()); p.setString(3,id); p.setString(4,workerId); p.executeUpdate();
        }
    }
    public void fail(String id, int attempts, int maxRetries, long retryAt) throws SQLException {
        String state = attempts > maxRetries ? "dead" : "failed";
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement("UPDATE jobs SET state=?,attempts=?,next_retry_at=?,updated_at=?,claimed_by=NULL,lease_expires_at=NULL WHERE id=?")) {
            p.setString(1,state); p.setInt(2,attempts); if (state.equals("dead")) p.setNull(3,Types.INTEGER); else p.setLong(3,retryAt); p.setLong(4,System.currentTimeMillis()); p.setString(5,id); p.executeUpdate();
        }
    }
    public void reapExpired(long now) throws SQLException {
        /* This single UPDATE finds expired leases and applies normal failure/DLQ accounting. */
        String sql = """
                UPDATE jobs SET state=CASE WHEN attempts+1 > max_retries THEN 'dead' ELSE 'failed' END,
                  attempts=attempts+1,
                  next_retry_at=CASE WHEN attempts+1 > max_retries THEN NULL
                    ELSE ? + CAST(POWER(backoff_base, attempts + 1) * 1000 AS INTEGER) END,
                  updated_at=?, claimed_by=NULL, lease_expires_at=NULL
                WHERE state='processing' AND lease_expires_at < ?""";
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setLong(1, now); p.setLong(2, now); p.setLong(3, now); p.executeUpdate();
        }
    }
    public void retryDead(String id) throws SQLException { update("UPDATE jobs SET state='pending',attempts=0,next_retry_at=NULL,updated_at=? WHERE id=? AND state='dead'", id); }
    public int configInt(String key) throws SQLException { try(Connection c=database.connect();PreparedStatement p=c.prepareStatement("SELECT value FROM config WHERE key=?")){p.setString(1,key);try(ResultSet r=p.executeQuery()){if(!r.next()) throw new IllegalArgumentException("missing config "+key);return Integer.parseInt(r.getString(1));}} }
    public int count(String state) throws SQLException { try(Connection c=database.connect(); PreparedStatement p=c.prepareStatement("SELECT COUNT(*) FROM jobs WHERE state=?")){p.setString(1,state);try(ResultSet r=p.executeQuery()){r.next();return r.getInt(1);}} }
    private void update(String sql,String id) throws SQLException { try(Connection c=database.connect();PreparedStatement p=c.prepareStatement(sql)){p.setLong(1,System.currentTimeMillis());p.setString(2,id);p.executeUpdate();} }
    private static Job row(ResultSet r) throws SQLException { Long lease=(Long)r.getObject("lease_expires_at"), retry=(Long)r.getObject("next_retry_at"); return new Job(r.getString("id"),r.getString("command"),r.getString("state"),r.getInt("attempts"),r.getInt("max_retries"),r.getInt("backoff_base"),r.getLong("created_at"),r.getLong("updated_at"),r.getString("claimed_by"),lease,retry); }
}
