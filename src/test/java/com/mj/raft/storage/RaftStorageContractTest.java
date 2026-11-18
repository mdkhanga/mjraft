package com.mj.raft.storage;

import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.Member;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behaviour every RaftStorage must have. Each implementation's test extends this.
 */
abstract class RaftStorageContractTest {

    static final Member A = new Member("localhost", 5001);

    RaftStorage storage;

    abstract RaftStorage open() throws IOException;

    @BeforeEach
    void openStorage() throws IOException {
        storage = open();
    }

    @AfterEach
    void closeStorage() throws IOException {
        storage.close();
    }

    @Test
    void startsEmpty() throws IOException {
        PersistentState s = storage.load();

        assertEquals(0, s.term());
        assertNull(s.votedFor());
        assertTrue(s.log().isEmpty());
    }

    @Test
    void savesTermAndVote() throws IOException {
        storage.saveTermAndVote(3, A);

        PersistentState s = storage.load();
        assertEquals(3, s.term());
        assertEquals(A, s.votedFor());
    }

    @Test
    void laterTermAndVoteReplacesEarlier() throws IOException {
        storage.saveTermAndVote(3, A);
        storage.saveTermAndVote(4, null);

        PersistentState s = storage.load();
        assertEquals(4, s.term());
        assertNull(s.votedFor());
    }

    @Test
    void appendsEntriesInOrder() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b")));
        storage.append(List.of(entry(2, "c")));

        assertLog(storage.load().log(), entry(1, "a"), entry(1, "b"), entry(2, "c"));
    }

    @Test
    void appendingNothingChangesNothing() throws IOException {
        storage.append(List.of(entry(1, "a")));
        storage.append(List.of());

        assertLog(storage.load().log(), entry(1, "a"));
    }

    @Test
    void storesEmptyAndLargeEntries() throws IOException {
        byte[] large = new byte[1024 * 1024];
        large[large.length - 1] = 7;
        storage.append(List.of(new LogEntry(1, new byte[0]), new LogEntry(1, large)));

        assertLog(storage.load().log(), new LogEntry(1, new byte[0]), new LogEntry(1, large));
    }

    @Test
    void truncateFromDropsThatEntryAndAllAfter() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b"), entry(2, "c")));

        storage.truncateFrom(1);

        assertLog(storage.load().log(), entry(1, "a"));
    }

    @Test
    void truncateFromTheEndChangesNothing() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b")));

        storage.truncateFrom(2);

        assertLog(storage.load().log(), entry(1, "a"), entry(1, "b"));
    }

    @Test
    void truncateFromOutsideTheLogFails() throws IOException {
        storage.append(List.of(entry(1, "a")));

        assertThrows(IndexOutOfBoundsException.class, () -> storage.truncateFrom(2));
        assertThrows(IndexOutOfBoundsException.class, () -> storage.truncateFrom(-1));
    }

    @Test
    void appendAfterTruncateContinuesFromTheCut() throws IOException {
        storage.append(List.of(entry(1, "a"), entry(1, "b"), entry(1, "c")));
        storage.truncateFrom(1);

        storage.append(List.of(entry(2, "d")));

        assertLog(storage.load().log(), entry(1, "a"), entry(2, "d"));
    }

    static LogEntry entry(int term, String data) {
        return new LogEntry(term, data.getBytes(StandardCharsets.UTF_8));
    }

    static void assertLog(List<LogEntry> actual, LogEntry... expected) {
        assertEquals(expected.length, actual.size(), "log size");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i].getTerm(), actual.get(i).getTerm(), "term of entry " + i);
            assertArrayEquals(expected[i].getEntry(), actual.get(i).getEntry(), "data of entry " + i);
        }
    }
}
