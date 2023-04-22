package com.mj.distributed.model;

public class ServerState {
    private int currentTerm;
    private int lastCommittedIndex;
    private int lastpersistedIndex;
    private String votedFor;

    public ServerState() {


    }

    public void load() {

    }

    public void save() {

    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int t) {
        currentTerm = t;
    }

}
