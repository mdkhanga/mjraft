package com.mj.raft.storage;

import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.Member;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Keeps everything in memory, for tests. Nothing survives the process.
 */
public final class InMemoryRaftStorage implements RaftStorage {

    private int term;
    private Member votedFor;
    private final List<LogEntry> log = new ArrayList<>();

    @Override
    public synchronized PersistentState load() {
        return new PersistentState(term, votedFor, log);
    }

    @Override
    public synchronized void saveTermAndVote(int term, Member votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        log.addAll(entries);
    }

    @Override
    public synchronized void truncateFrom(int index) {
        Objects.checkIndex(index, log.size() + 1);
        log.subList(index, log.size()).clear();
    }

    @Override
    public void close() {
    }
}
