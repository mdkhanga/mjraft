package com.mj.distributed.leader;


public class LeaderStateMachine {

    public enum MemberState {
        LEADER,
        CANDIDATE,
        FOLLOWER
    }

    private MemberState state ;

    public LeaderStateMachine() {
        state = state.FOLLOWER ;
    }

    public void start() {

    }

    /**
     *  method called tp pass data that would change the state of this machine
     *
     *
     */
    public void update(/* params */) {



    }
}
