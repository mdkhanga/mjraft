package com.mj.raft.states;

import com.mj.distributed.model.RaftState;

public interface State {

    RaftState raftState();

    void start();

    void stop();

    void changeState(State newState);

}
