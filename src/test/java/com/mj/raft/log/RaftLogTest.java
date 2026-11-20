package com.mj.raft.log;

import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.Member;
import com.mj.raft.storage.InMemoryRaftStorage;
import com.mj.raft.storage.PersistentState;
import com.mj.raft.storage.RaftStorage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RaftLogTest {

    @Test
    void addedEntryIsSaved() throws Exception {
        InMemoryRaftStorage storage = new InMemoryRaftStorage();
        RaftLog log = new RaftLog(storage);

        log.addLogEntry(2, new byte[]{7});

        List<LogEntry> saved = storage.load().log();
        assertEquals(1, saved.size());
        assertEquals(2, saved.get(0).getTerm());
        assertArrayEquals(new byte[]{7}, saved.get(0).getEntry());
    }

    @Test
    void entryIsNotAddedIfSavingFails() {
        RaftLog log = new RaftLog(new FailingStorage());

        assertThrows(IOException.class, () -> log.addLogEntry(1, new byte[]{7}));

        assertNull(log.getLastEntry(), "an entry that is not on disk must not be in the log");
    }

    @Test
    void restoreReloadsEntries() {
        RaftLog log = new RaftLog(new InMemoryRaftStorage());

        log.restore(List.of(new LogEntry(1, new byte[]{1}), new LogEntry(3, new byte[]{2})));

        assertEquals(1, log.getLastEntry().getIndex());
        assertEquals(3, log.getLastEntry().getTerm());
        assertArrayEquals(new byte[]{2}, log.getLastEntry().getEntry());
    }

    private static final class FailingStorage implements RaftStorage {

        @Override
        public PersistentState load() {
            return new PersistentState(0, null, List.of());
        }

        @Override
        public void saveTermAndVote(int term, Member votedFor) throws IOException {
            throw new IOException("disk full");
        }

        @Override
        public void append(List<LogEntry> entries) throws IOException {
            throw new IOException("disk full");
        }

        @Override
        public void truncateFrom(int index) throws IOException {
            throw new IOException("disk full");
        }

        @Override
        public void close() {
        }
    }
}
