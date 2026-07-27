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

    public Optional<Job> claimNext(String workerId, long leaseExpiresAt, long now) throws SQLException {
        /* One statement, never SELECT then UPDATE: SQLite serializes writers across processes. */
        String sql = """
                UPDATE jobs SET state='processing', claimed_by=?, lease_expires_at=?, updated_at=?
                WHERE id=(SELECT id FROM jobs WHERE state='pending' OR (state='failed' AND next_retry_at<=?)
                          ORDER BY created_at ASC LIMIT 1)
                  AND state IN ('pending','failed')
                RETURNING *""";
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement(sql)) {
            p.setString(1, workerId); p.setLong(2, leaseExpiresAt); p.setLong(3, now); p.setLong(4, now);
            try (ResultSet r = p.executeQuery()) { return r.next() ? Optional.of(row(r)) : Optional.empty(); }
        }
    }

    public void complete(String id) throws SQLException { update("UPDATE jobs SET state='completed',updated_at=?,claimed_by=NULL,lease_expires_at=NULL WHERE id=?", id); }
    public void fail(String id, int attempts, int maxRetries, long retryAt) throws SQLException {
        String state = attempts > maxRetries ? "dead" : "failed";
        try (Connection c = database.connect(); PreparedStatement p = c.prepareStatement("UPDATE jobs SET state=?,attempts=?,next_retry_at=?,updated_at=?,claimed_by=NULL,lease_expires_at=NULL WHERE id=?")) {
            p.setString(1,state); p.setInt(2,attempts); if (state.equals("dead")) p.setNull(3,Types.INTEGER); else p.setLong(3,retryAt); p.setLong(4,System.currentTimeMillis()); p.setString(5,id); p.executeUpdate();
        }
    }
    public void reapExpired(long now) throws SQLException {
        try (Connection c = database.connect(); PreparedStatement q = c.prepareStatement("SELECT id,attempts,max_retries FROM jobs WHERE state='processing' AND lease_expires_at < ?")) {
            q.setLong(1,now); try (ResultSet r=q.executeQuery()) { while(r.next()) fail(r.getString(1),r.getInt(2)+1,r.getInt(3),now); }
        }
    }
    public void retryDead(String id) throws SQLException { update("UPDATE jobs SET state='pending',attempts=0,next_retry_at=NULL,updated_at=? WHERE id=? AND state='dead'", id); }
    public int count(String state) throws SQLException { try(Connection c=database.connect(); PreparedStatement p=c.prepareStatement("SELECT COUNT(*) FROM jobs WHERE state=?")){p.setString(1,state);try(ResultSet r=p.executeQuery()){r.next();return r.getInt(1);}} }
    private void update(String sql,String id) throws SQLException { try(Connection c=database.connect();PreparedStatement p=c.prepareStatement(sql)){p.setLong(1,System.currentTimeMillis());p.setString(2,id);p.executeUpdate();} }
    private static Job row(ResultSet r) throws SQLException { Long lease=(Long)r.getObject("lease_expires_at"), retry=(Long)r.getObject("next_retry_at"); return new Job(r.getString("id"),r.getString("command"),r.getString("state"),r.getInt("attempts"),r.getInt("max_retries"),r.getLong("created_at"),r.getLong("updated_at"),r.getString("claimed_by"),lease,retry); }
}
