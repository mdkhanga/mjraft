package com.mj.distributed.model;

import java.nio.ByteBuffer;

public class ServerState {
    private int currentTerm;
    private int lastCommittedIndex;
    private int lastpersistedIndex;
    private String votedFor;

    public ServerState() {


    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int t) {
        currentTerm = t;
    }

    public int getLastCommittedIndex() {
        return lastCommittedIndex;
    }

    public void setLastCommittedIndex(int lci) {
        lastCommittedIndex = lci;
    }

    public int getLastpersistedIndex() {
        return lastpersistedIndex;
    }

    public void setLastpersistedIndex(int lpi) {
        lastpersistedIndex = lpi;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void setVotedFor(String s) {
        votedFor = s;
    }

    public ByteBuffer serialize() {
        return null ;
    }

    public static ServerState deserialize(ByteBuffer b) {
        return new ServerState();
    }
}
