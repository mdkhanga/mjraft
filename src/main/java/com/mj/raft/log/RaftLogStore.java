package com.mj.raft.log;

import com.mj.distributed.model.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RaftLogStore {

    private static Logger LOG  = LoggerFactory.getLogger(RaftLogStore.class);

    private File raftLogFile = new File("raftlog.dat");

    private RaftLog raftLog;
    private FileChannel outChannel ;


    private int lastCommittedIndex;
    private int lastPersistedCommittedIndex;

    public RaftLogStore(RaftLog r) {
        raftLog = r;

        if (!raftLogFile.exists()) {
            try {
                raftLogFile.createNewFile();
                FileOutputStream fos = new FileOutputStream(raftLogFile, true);
                outChannel = fos.getChannel();

            } catch(IOException io) {
                LOG.warn("Error creating raftlog.dat. Log will not be persisted", io);
            }
        }
    }

    public void load() {



    }

    public void save(int committedIndex) {
        if (committedIndex <= lastPersistedCommittedIndex) {
            return ;
        }

        for (int i = lastPersistedCommittedIndex ; i <= committedIndex ; i++) {
            LogEntry e = raftLog.rlog.get(i);



        }


    }

}
