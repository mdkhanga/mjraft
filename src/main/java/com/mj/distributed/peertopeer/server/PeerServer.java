package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.*;
import com.mj.distributed.tcp.nio.NioListener;
import com.mj.distributed.tcp.nio.NioListenerConsumer;
import com.mj.distributed.message.AppendEntriesMessage;
import com.mj.distributed.message.ClusterInfoMessage;
import com.mj.distributed.message.HelloMessage;
import com.mj.distributed.message.Message;
import com.mj.distributed.utils.Utils;
import com.mj.raft.states.Candidate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.*;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PeerServer implements NioListenerConsumer {


    private final ConcurrentMap<Member, Peer> connectedMembersMap = new ConcurrentHashMap<>() ;
    private volatile ConcurrentMap<Member, PeerData> memberPeerDataMap = new ConcurrentHashMap<>() ;
    // reverse index
    private final ConcurrentMap<SocketChannel, Peer> socketChannelPeerMap = new ConcurrentHashMap<>() ;

    private Set<Member> members = new HashSet<>() ;
    private volatile Member leader ;
    // private volatile ClusterInfo clusterInfo ;

    Integer x = 0 ;

    private Logger LOG  = LoggerFactory.getLogger(PeerServer.class) ;

    private String bindHost = "localhost" ;
    private int bindPort ;

    Selector selector ;
    private NioListener listener;
    private Member thisMember ;

    // public static InBoundMessageCreator inBoundMessageCreator = new InBoundMessageCreator() ;

    public InBoundMessageCreator inBoundMessageCreator;

    // public static PeerServer peerServer ;

    List<byte[]> rlog = Collections.synchronizedList(new ArrayList<>());
    volatile AtomicInteger lastComittedIndex  = new AtomicInteger(-1) ;

    volatile ConcurrentHashMap<Integer,ConcurrentLinkedQueue<Integer>> ackCountMap =
            new ConcurrentHashMap<>(); // key = index, value = queue of commit responses

    volatile AtomicInteger currentTerm = new AtomicInteger(0);

    volatile Long lastLeaderHeartBeatTs = 0L ;

    // volatile boolean leaderElection = false ;

    volatile RaftState raftState = RaftState.follower ;

    ExecutorService peerServerExecutor = Executors.newFixedThreadPool(3) ;

    Thread writerThread;
    private volatile boolean stop = false ;

    private volatile int leaderId = -1;
    private volatile AtomicInteger serverIdGenerator = new AtomicInteger(1);
    private volatile boolean electionInProgress = false ;
    private volatile int currentElectionTerm = -1 ;
    private volatile Candidate leaderElection ;

    private volatile int currentVotedTerm = -1;
    private volatile long currentVoteTimeStamp = 0;

    private String[] seeds;


    public PeerServer(int bindPort) {
        _peerServer( bindPort, RaftState.leader);
    }

    public PeerServer(int bindPort, String[] s) {

        seeds = s ;

        if (seeds == null  || seeds.length == 0) {
            _peerServer( bindPort, RaftState.leader);
        } else {
            _peerServer( bindPort, RaftState.follower) ;
        }
    }


    public void _peerServer( int port, RaftState state) {

        bindPort = port;
        // serverId = new AtomicInteger(id) ;

        LOG.info("Starting server on port: " + port + " state: "+ state);


        raftState = state ;
        inBoundMessageCreator = new InBoundMessageCreator(this);
    }

    // public void start(String[] seed) throws Exception {
    public void start() throws Exception {

        thisMember = new Member(bindHost, bindPort, true) ;

        /* if (raftState.equals(RaftState.leader)) {
            // initiate connect to peers
            clusterInfo = new ClusterInfo(thisMember, new ArrayList<Member>());

        } */
        leader = thisMember;
        members.add(thisMember);


        if (seeds != null) {
            for (String s : seeds) {

                String[] remoteaddrAndPort = s.split(":") ;

                LOG.info("Connecting to " + s) ;
                PeerClient peer = new PeerClient(remoteaddrAndPort[0],Integer.parseInt(remoteaddrAndPort[1]),this);
                peer.start();
                HelloMessage m = new HelloMessage(getBindHost(),getBindPort());
                peer.queueSendMessage(m.serialize());
                LOG.info("Done write hello to q") ;

            }
        }

            writerThread = new Thread(new ServerWriteRunnable(this));
            writerThread.start();

           // Thread clientThread = new Thread(new ClientSimulator());
           // clientThread.start();

            // accept() ;
        listener = new NioListener(bindHost, bindPort, this);
        listener.start();

    }

    public void stop() throws Exception {

        LOG.info(getServerId() + ": is stopping") ;
        stop = true;
        listener.stop();
    }

    public String getBindHost() {
        return bindHost ;
    }

    public int getBindPort() {
        return bindPort ;
    }

    public int getTerm() {
        return currentTerm.get();
    }
    public void setTerm(int t) {
        currentTerm.set(t);
    }


    public String getLeaderId() {
        if (leader != null) {
            return leader.getHostString()+":"+leader.getPort();
        } else {
            return "";
        }
    }

    public void setLeader(String leaderId) {

        String[] parts = leaderId.split(":") ;

        leader = new Member(parts[0],Integer.parseInt(parts[1]));

        if (raftState == RaftState.leader || raftState == RaftState.candidate) {
            LOG.info(getServerId() + " Change state from leader to follower. New leader is  "+ leaderId);

        }
    }

    /*
    public void setLeaderId(int s) {
        leaderId = s ;
    } */


    public void setCurrentVotedTerm(int term) {
        currentVotedTerm = term;
        currentVoteTimeStamp = System.currentTimeMillis();
    }

    public int getCurrentVotedTerm() {
        return currentVotedTerm;
    }

    public int getNextElectionTerm() {

        if (currentTerm.get() >= currentElectionTerm) {
            return currentTerm.get() + 1;
        } else {
            return currentElectionTerm + 1;
        }
    }

    public String getServerId() {
        return bindHost+":"+bindPort;
    }

    public List<Member> getMembers() {

        List<Member> m = new ArrayList<>();

        members.forEach((k)->{
            m.add(k);
        });

        return m;
    }

    public LogEntry getLastCommittedEntry() {
        if (lastComittedIndex.get() >= 0) {
            return new LogEntry(lastComittedIndex.get(),rlog.get(lastComittedIndex.get()));
        } else {
            return new LogEntry(lastComittedIndex.get(), new byte[1]);
        }
    }


    public ClusterInfo getClusterInfo() {
            ClusterInfo info ;
            synchronized (members) {
                info = new ClusterInfo(leader, new ArrayList<>(members));
            }
            return info;
    }

    public void setClusterInfo(ClusterInfo c) {
        synchronized (members) {
            members.addAll(c.getMembers());
            leader = c.getLeader();
        }
    }

    public void setElectionInProgress(int term) {
        electionInProgress = true;
        currentElectionTerm = term ;
        currentElectionTerm = term;
    }

    public void clearElectionInProgress() {
        electionInProgress = false;
        currentVotedTerm = -1 ;
        if (leaderElection != null) {
            leaderElection.stop();
        }
    }

    public int getCurrentElectionTerm() {
        return currentElectionTerm;
    }

    public int vote(boolean vote) {
        return leaderElection.vote(vote) ;
    }

    public boolean isElectionInProgress() {
        return electionInProgress;
    }

    public boolean isLeader() {
        return leader.equals(thisMember);
    }

    public Member getLeader() {
        return leader;
    }

    public void queueSendMessage(SocketChannel c, Message m) throws Exception {

        Peer p = socketChannelPeerMap.get(c) ;


        p.queueSendMessage(m);


        /* synchronized(x) {
            x = 1 ;
        }

        selector.wakeup() ; */

    }

    public void setRaftState(RaftState state) {
        raftState = state ;
        if (state == RaftState.leader) {
            leader = thisMember;
        }
    }

    public RaftState getRaftState() {
        return raftState;
    }

    public InBoundMessageCreator getInBoundMessageCreator() {
        return inBoundMessageCreator;
    }

    public boolean processLogEntry(LogEntry e, int prevIndex, int lastComittedIndex) throws Exception {


        boolean ret = true ;

        if (e != null) {

            // LOG.info("We have an entry") ;
            // LOG.info("Received last committed index "+lastComittedIndex);
            // LOG.info("Our last committed index " + this.lastComittedIndex.get()) ;

            int position = e.getIndex();
            byte[] data = e.getEntry();

            // LOG.info("Received log entry " + ByteBuffer.wrap(data).getInt());

            int expectedNextEntry = rlog.size();

            // LOG.info("prev = " + prevIndex + " expectedNext = " + expectedNextEntry) ;
            if (prevIndex + 1 == expectedNextEntry) {
                /* synchronized (rlog) {
                    rlog.add(data);
                } */
                addLogEntry(data);
                ret = true ;
                if (lastComittedIndex <= expectedNextEntry) {
                    this.lastComittedIndex.set(lastComittedIndex);
                }
            } else {
                ret = false ;
                // LOG.info("did not add to rlog return false") ;
            }
        } else {
            // LOG.info("No entry") ;
        }


        if (lastComittedIndex < rlog.size() && lastComittedIndex > this.lastComittedIndex.get()) {
            LOG.info("Setting committed index to "+lastComittedIndex);
            this.lastComittedIndex.set(lastComittedIndex);
        }

        return ret ;
    }

    public List<byte[]> getLogEntries(int start, int count) {

        ArrayList<byte[]> ret = new ArrayList<>();

        for (int i = start; i < start+count ; i++) {

            ret.add(rlog.get(i));

        }

        return ret ;

    }


    public static void main(String args[]) throws Exception {

        if (args.length == 0 ) {
            System.out.println("Need at least 1 argurment") ;
        }

        // int serverId = Integer.parseInt(args[0]) ;
        int port = Integer.parseInt(args[0]) ;

        int size = args.length   ;

        // RaftState state = RaftState.follower ;

        String[] seeds = null ;

        if (size > 1) {
            seeds = new String[args.length-1] ;
            int j = 0 ;
            for (int i = 1 ; i < size ; i++) {
                seeds[j] = args[i] ;
                ++j ;
            }

        } else {
            // state = RaftState.leader;
        }

        System.out.println("Starting server listening on port: " + port) ;

        PeerServer peerServer = new PeerServer( port, seeds) ;
        peerServer.start() ;
    }

    public void addedConnection(SocketChannel s) {

        // do nothing this we get hello message

    }

    public void droppedConnection(SocketChannel s) {

        LOG.info(" connection dropped") ;

        removePeer(s);

    }

    public void addLogEntry(byte[] value) throws Exception {
        rlog.add(value);
        logRlog();
    }

    public void consumeMessage(SocketChannel s, int numBytes, ByteBuffer b) {

        inBoundMessageCreator.submit(s, b, numBytes);
    }

    public void setLastLeaderHeartBeatTs(long ts) {
        synchronized (lastLeaderHeartBeatTs) {
            lastLeaderHeartBeatTs = ts;
        }
    }

    public long getlastLeaderHeartBeatts() {
        synchronized (lastLeaderHeartBeatTs) {
            return lastLeaderHeartBeatTs ;
        }
    }


    public class ServerWriteRunnable implements Runnable {

        PeerServer peerServer;

        ServerWriteRunnable(PeerServer p) {

            peerServer = p;
        }


        public void run() {

            AtomicInteger count = new AtomicInteger(1);

            while (!stop) {

                try {
                    Thread.sleep(200);
                    // Thread.sleep(1000);

                    /* if (count.get() % 60 == 0) {
                        logRlog();
                    } */

                    if (raftState.equals(RaftState.leader)) {

                        // channelPeerMap.forEach((k, v) -> {
                        connectedMembersMap.forEach((k, v) -> {

                            try {

                                sendAppendEntriesMessage(v);

                                if (count.get() % 60 == 0) {
                                    // logRlog();
                                    sendClusterInfoMessage(v);
                                }


                            } catch (Exception e) {
                                LOG.error("error", e);
                            }

                        });

                        synchronized (x) {
                            x = 1;
                        }

                        // selector.wakeup();

                    }
                     else if (raftState.equals(RaftState.candidate)) {

                        // test
                        // LOG.info("We need a leader Election. No heartBeat in ") ;
                        if (!peerServer.isElectionInProgress()) {

                            // peerServer.setElectionInProgress(incrementTerm());

                            // int randomDelay = (int)(900 + 400*serverId);
                            int randomDelay = Utils.getRandomDelay();
                            // LOG.info("Got randoom Delay " + randomDelay);

                            long timeSinceLastLeadetBeat = System.currentTimeMillis() -
                                    peerServer.getlastLeaderHeartBeatts();
                            if ((peerServer.getlastLeaderHeartBeatts() > 0 && timeSinceLastLeadetBeat > randomDelay)
                                    &&
                                    (System.currentTimeMillis() - currentVoteTimeStamp > Candidate.ELECTION_TIMEOUT)) {


                                setElectionInProgress(getNextElectionTerm());
                                LOG.info(getServerId() + ": Starting leader election");
                                leaderElection = new Candidate(peerServer);
                                peerServerExecutor.submit(leaderElection);
                            }

                        }



                    } else if (raftState.equals(RaftState.follower)) {

                        // int randomDelay = (int)(900 + 400*serverId);
                        int randomDelay = Utils.getRandomDelay();
                        // LOG.info("Got random Delay " + randomDelay);

                        long timeSinceLastLeadetBeat = System.currentTimeMillis() -
                            peerServer.getlastLeaderHeartBeatts();
                        if ((peerServer.getlastLeaderHeartBeatts() > 0 && timeSinceLastLeadetBeat > randomDelay)
                                &&
                                (System.currentTimeMillis() - currentVoteTimeStamp > Candidate.ELECTION_TIMEOUT)) {
                            LOG.info(getServerId() + ":We need a leader Election. No heartBeat in ") ;
                            raftState = RaftState.candidate;
                        }

                    }

                    count.incrementAndGet();

                } catch (Exception e) {
                     System.out.println(e) ;
                }
            }

        }

    }

    // public void sendAppendEntriesMessage(PeerData v) throws Exception {
    public void sendAppendEntriesMessage(Peer peer) throws Exception {

        Member m = peer.member();
        PeerData v = memberPeerDataMap.get(m);

        AppendEntriesMessage p = new AppendEntriesMessage(getTerm(), getServerId(),
                v.getNextSeq(),
                rlog.size()-1,
                lastComittedIndex.get());

        int index = getIndexToReplicate(v) ;

        // LOG.info("Index to replicate "+ index);

        if (index >= 0 && index < rlog.size()) {
            byte[] data = rlog.get(index);
            // LOG.info("Replicating ..." + ByteBuffer.wrap(data).getInt());
            LogEntry entry = new LogEntry(index, data);
            p.addLogEntry(entry);
            p.setPrevIndex(index-1);
        }

        // v.addMessageForPeer(p);

        v.addToSeqIdIndexMap(p);


        peer.queueSendMessage(p);

        ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<Integer>();
        q.add(1); // self
        ackCountMap.put(index, q);

    }

    // public void sendClusterInfoMessage(PeerData v) throws Exception {
    public void sendClusterInfoMessage(Peer peer) throws Exception {

        /* if (clusterInfo.getLeader() != null) {
            ClusterInfoMessage cm = new ClusterInfoMessage(clusterInfo);
            // v.addMessageForPeer(cm);
            peer.queueSendMessage(cm);
        } */
        ClusterInfo info;
        synchronized (members) {
            info = new ClusterInfo(leader, new ArrayList<>(members));
        }
        ClusterInfoMessage cm = new ClusterInfoMessage(info);
        peer.queueSendMessage(cm);

    }

    public void redirect(SocketChannel sc, Redirect r) throws Exception {

        LOG.info("redirecting to "+r.getHost() + ":" +r.getHostPort());
        // keep the current connection around
        // As it could be used later start a new connection
        LOG.info("Connecting to") ;
        PeerClient peer = new PeerClient(r.getHost(), r.getHostPort(),this);
        peer.start();
        HelloMessage m = new HelloMessage(getBindHost(),getBindPort());
        peer.queueSendMessage(m.serialize());
        LOG.info("Done write hello to q") ;

    }


    public void removePeer(SocketChannel sc) {

        LOG.info(getServerId() + ": removed peer for dropped connection");

        Peer p = socketChannelPeerMap.get(sc) ;

        Member m = null ;

        if (p != null) {
            m = p.member() ;
        }

        socketChannelPeerMap.remove(sc) ;

        if (m != null) {
            connectedMembersMap.remove(m);
            memberPeerDataMap.remove(m);
            synchronized (members) {
                members.remove(m);
            }
        }

    }

    public void addPeer(SocketChannel sc, Peer p) {

        synchronized (members) {
            members.add(p.member());
        }
        socketChannelPeerMap.put(sc, p) ;
        connectedMembersMap.put(p.member(), p);
        memberPeerDataMap.put(p.member(), new PeerData(p.member().getHostString(), p.member().getPort()));

    }

    public void addPeer(SocketChannel sc, String hostString, int port) {
        Member m = new Member(hostString, port, false);
        ListenerPeer l = new ListenerPeer(listener, m, sc) ;
        // clusterInfo.addMember(m);
        synchronized (members) {
            members.add(m);
        }

        connectedMembersMap.put(m, l) ;
        memberPeerDataMap.put(m, new PeerData(hostString, port));
        socketChannelPeerMap.put(sc, l);

    }

    public void addRaftClient(SocketChannel sc) {
        ListenerPeer l = new ListenerPeer(listener, null, sc) ;
        socketChannelPeerMap.put(sc, l);
    }

    public Peer getPeer(SocketChannel s) {
        return socketChannelPeerMap.get(s) ;
    }

    public PeerData getPeerData(SocketChannel s) {
        Peer p = getPeer(s) ;
        return memberPeerDataMap.get(p.member());
    }

    public Peer getPeer(Member m) throws Exception {

        Peer p = connectedMembersMap.get(m) ;
        if (p != null) {
            return p;
        }
        // fix race condition
        // establish connection
        PeerClient pc = new PeerClient(m.getHostString(), m.getPort(), this) ;
        pc.start();

        HelloMessage hm = new HelloMessage(bindHost, bindPort);
        pc.queueSendMessage(hm.serialize());

        p = connectedMembersMap.get(m) ;

        if (p == null) {
            throw new RuntimeException("Connection not established to "+
                    m.getHostString() +":"+m.getPort() + " from " +bindHost+":"+bindPort );
        }


        return p;



    }

    public void logRlog() throws Exception {

        StringBuilder sb = new StringBuilder("Replicated Log [") ;

        // LOG.info("number of entries "+rlog.size()) ;

        rlog.forEach((k)->{

            try {

                sb.append(ByteBuffer.wrap(k).getInt()) ;
                sb.append(",") ;


            } catch(Exception e) {
                LOG.error("Error getting remote address ",e) ;
            }

        });

        sb.append("]") ;

        LOG.info(sb.toString()) ;
        // LOG.info("Committed index = " + String.valueOf(lastComittedIndex));

    }

    public void consumeMessage(Message message) {


    }



    private int getIndexToReplicate(PeerData d) {

        int maxIndex = rlog.size() - 1  ;

        return d.getNextIndexToReplicate(maxIndex) ;

    }

    public void updateIndexAckCount(int index) {

        if (lastComittedIndex.get() >= index) {
            ackCountMap.remove(index);
        }

        ConcurrentLinkedQueue<Integer> indexQueue = ackCountMap.get(index) ;
        if (indexQueue == null) {
            return ;
        }

        indexQueue.add(1) ;

        int majority = Utils.majority(members.size());
        // if (indexQueue.size() >= connectedMembersMap.size()/2 ) {
        if (indexQueue.size() >= majority ) {
            lastComittedIndex.set(index) ;
            ackCountMap.remove(index) ;
            LOG.info("Last committed index="+lastComittedIndex.get());
        } else {
           //  LOG.info("Not enough votes " + indexQueue.size() + "---" + connectedMembersMap.size()/2 );
        }
    }
}

