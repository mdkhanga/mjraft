package com.mj.raft.log;

import com.mj.distributed.message.AppendEntriesMessage;
import com.mj.distributed.model.LogEntry;
import com.mj.distributed.model.LogEntryWithIndex;
import com.mj.distributed.model.Member;
import com.mj.distributed.peertopeer.server.Peer;
import com.mj.distributed.peertopeer.server.PeerData;
import com.mj.distributed.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftLog {

    private static Logger LOG  = LoggerFactory.getLogger(RaftLog.class);

    List<LogEntry> rlog = Collections.synchronizedList(new ArrayList<>());
    volatile AtomicInteger lastComittedIndex  = new AtomicInteger(-1) ;

    volatile ConcurrentHashMap<Integer,ConcurrentLinkedQueue<Integer>> ackCountMap =
            new ConcurrentHashMap<>(); // key = index, value = queue of commit responses

    File raftLogFile = new File("raftlog.dat");

    public RaftLog() {



    }


    public LogEntryWithIndex getLastEntry() {
        if (rlog.size() > 0) {
            // return new LogEntry(getTerm(),lastComittedIndex.get(),rlog.get(lastComittedIndex.get()));
            synchronized (rlog) {
                int index = rlog.size() - 1;
                LogEntry e = rlog.get(index);
                return new LogEntryWithIndex(e.getTerm(), index, e.getEntry());
            }
        } else {
            return null ;
        }
    }

    public boolean processLogEntry(LogEntryWithIndex e, int prevIndex, int prevTerm, int lastComittedIndex) throws Exception {
        boolean ret = true ;
        if (e != null) {
            byte[] data = e.getEntry();
            int expectedNextEntry = rlog.size();

            // rule 2
            if (prevIndex + 1 == expectedNextEntry) {
                if (prevIndex > 0 && rlog.get(prevIndex).getTerm() != prevTerm) {
                    ret = false ;
                } else {
                    addLogEntry(e.getTerm(), data);
                    ret = true;
                    if (lastComittedIndex <= expectedNextEntry) { // do we need this ?
                        this.lastComittedIndex.set(lastComittedIndex);
                    }
                }
            } else {
                ret = false ;
            }
        }
        // Should we commit if there is no entry in message. is consistency check skipped ?
        if (lastComittedIndex < rlog.size() && lastComittedIndex > this.lastComittedIndex.get()) {
            LOG.info("Setting committed index to "+lastComittedIndex);
            this.lastComittedIndex.set(lastComittedIndex);
        }
        return ret ;
    }

    public void addLogEntry(int term, byte[] value) throws Exception {
        // rlog.add(value);
        rlog.add(new LogEntry(term, value));
    }

    public List<byte[]> getLogEntries(int start, int count) {
        LOG.info("start = " + start + " count = " + count) ;
        ArrayList<byte[]> ret = new ArrayList<>();
        int end = start + count < rlog.size() ? start + count - 1 : rlog.size() -1 ;
        LOG.info("end = " + end);
        for (int i = start; i <= end ; i++) {
            ret.add(rlog.get(i).getEntry());
        }
        return ret ;
    }

    public void sendAppendEntriesMessage(Peer peer, PeerData v, String serverId, int term ) throws Exception {
        Member m = peer.member();
        // PeerData v = memberPeerDataMap.get(m);

        int prevIndex = rlog.size()-1;
        int prevTerm = prevIndex > 0 ? rlog.get(prevIndex).getTerm() : -1;

        AppendEntriesMessage p = new AppendEntriesMessage(term, serverId,
                v.getNextSeq(),
                prevIndex,
                prevTerm,
                lastComittedIndex.get());

        int index = getIndexToReplicate(v) ;
        if (index >= 0 && index < rlog.size()) {
            byte[] data = rlog.get(index).getEntry();
            // LOG.info("Replicating ..." + ByteBuffer.wrap(data).getInt());
            LogEntryWithIndex entry = new LogEntryWithIndex(term,index, data);
            p.addLogEntry(entry);
            p.setPrevIndex(index-1);
        }
        // v.addToSeqIdIndexMap(p); Not being used
        peer.queueSendMessage(p);
        // Questionable code -- need to check if q already exists
        ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<Integer>();
        q.add(1); // self
        ackCountMap.put(index, q);
    }

    private int getIndexToReplicate(PeerData d) {
        int maxIndex = rlog.size() - 1  ;
        return d.getNextIndexToReplicate(maxIndex) ;
    }

    public void updateIndexAckCount(int index, int clustersize) {
        if (lastComittedIndex.get() >= index) {
            ackCountMap.remove(index);
        }
        ConcurrentLinkedQueue<Integer> indexQueue = ackCountMap.get(index) ;
        if (indexQueue == null) {
            return ;
        }
        indexQueue.add(1) ;
        int majority = Utils.majority(clustersize);
        if (indexQueue.size() >= majority ) {
            commit(index);
            /* lastComittedIndex.set(index) ;
            ackCountMap.remove(index) ;
            LOG.info("Last committed index="+lastComittedIndex.get());*/
        }
    }

    private void commit(int index) {
        lastComittedIndex.set(index) ;
        ackCountMap.remove(index) ;
        // TODO : persist to file
        LOG.info("Last committed index="+lastComittedIndex.get());
    }
}
