package com.queuectl.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JobStoreTest {
    @TempDir
    Path tempDir;

    @Test
    void clearAllRemovesEveryStoredJob() throws Exception {
        JobStore store = new JobStore(new Database(tempDir));
        store.enqueue("job-1", "echo one", 3);
        store.enqueue("job-2", "echo two", 3);

        assertEquals(2, store.list(null).size());

        store.clearAll();

        assertEquals(0, store.list(null).size());
    }
}
