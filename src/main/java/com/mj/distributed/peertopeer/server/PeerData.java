package com.mj.distributed.peertopeer.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class PeerData {

    private String hostString ;
    private int port ;
    private int serverId ;
    private volatile AtomicInteger seq = new AtomicInteger(0) ;
    // private volatile ConcurrentHashMap<Integer, Integer> seqIdLogIndexMap = new ConcurrentHashMap() ;
    private volatile int lastIndexReplicated = -1;

    private Logger LOG  = LoggerFactory.getLogger(PeerData.class);


    public PeerData(String hostString, int port) {

        this.hostString = hostString ;
        this.port = port ;

    }

    public int getNextSeq() {

        return seq.incrementAndGet() ;
    }

    public String getHostString() {
        return hostString ;
    }


    public int getPort() {
        return port ;
    }

    public void setPort(int p) {
        port = p ;
    }


    public int getNextIndexToReplicate(int maxIndex) {

        // LOG.info("max index is "+ maxIndex);

        int ret = lastIndexReplicated + 1 ;

        if (ret <= maxIndex) {
            ++lastIndexReplicated;
           // LOG.info("next index is "+ ret) ;
            return ret ;
        }


        return -1 ;
    }

    public int getServerId() {
        return serverId;
    }

    // needed to consistency check if peer is out of data out of sync with leades
    public void decrementNextIndexToReplicate() {
        --lastIndexReplicated;
    }

}
