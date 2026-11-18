package com.mj.raft.storage;

import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.Member;

import java.util.List;

/**
 * @param votedFor null if no vote was cast in this term
 */
public record PersistentState(int term, Member votedFor, List<LogEntry> log) {

    public PersistentState {
        log = List.copyOf(log);
    }
}
