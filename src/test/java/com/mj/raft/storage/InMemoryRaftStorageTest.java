package com.mj.raft.storage;

class InMemoryRaftStorageTest extends RaftStorageContractTest {

    @Override
    RaftStorage open() {
        return new InMemoryRaftStorage();
    }
}
