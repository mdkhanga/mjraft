package com.mj.raft.storage;

import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.Member;

import java.io.IOException;
import java.util.List;

/**
 * The state Raft requires to survive a restart: the current term, the vote cast in it, and the log.
 * Every write is durable when the method returns, so the caller may then reply to a peer.
 */
public interface RaftStorage extends AutoCloseable {

    /**
     * @return what was last saved; term 0, no vote and an empty log if nothing was
     */
    PersistentState load() throws IOException;

    /**
     * @param votedFor the candidate voted for in this term, or null if none
     */
    void saveTermAndVote(int term, Member votedFor) throws IOException;

    /**
     * Appends after the current last entry.
     */
    void append(List<LogEntry> entries) throws IOException;

    /**
     * Removes the entry at index and every entry after it. An index equal to the log size removes nothing.
     */
    void truncateFrom(int index) throws IOException;

    @Override
    void close() throws IOException;
}
