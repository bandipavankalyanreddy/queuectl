package com.queuectl.core;

/** A database row exposed to command and worker code. Times are epoch milliseconds. */
public record Job(String id, String command, String state, int attempts, int maxRetries,
                  long createdAt, long updatedAt, String claimedBy, Long leaseExpiresAt,
                  Long nextRetryAt) { }
