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
import com.mj.raft.states.Follower;
import com.mj.raft.states.Leader;
import com.mj.raft.states.State;
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

    private static Logger LOG  = LoggerFactory.getLogger(PeerServer.class) ;

    private String bindHost = "localhost" ;
    private int bindPort ;

    private NioListener listener;
    private Member thisMember ;

    public InBoundMessageCreator inBoundMessageCreator;

    List<byte[]> rlog = Collections.synchronizedList(new ArrayList<>());
    volatile AtomicInteger lastComittedIndex  = new AtomicInteger(-1) ;

    volatile ConcurrentHashMap<Integer,ConcurrentLinkedQueue<Integer>> ackCountMap =
            new ConcurrentHashMap<>(); // key = index, value = queue of commit responses

    volatile AtomicInteger currentTerm = new AtomicInteger(0);
    volatile Long lastLeaderHeartBeatTs = 0L ;
    // volatile RaftState raftState = RaftState.follower ;
    volatile State raftState ;
    ExecutorService peerServerExecutor = Executors.newFixedThreadPool(6) ;
    private volatile boolean stop = false ;
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
        LOG.info("Starting server on port: " + port + " state: "+ state);
        raftState = getStateMachine(state) ;
        inBoundMessageCreator = new InBoundMessageCreator(this);
    }

    public void start() throws Exception {

        thisMember = new Member(bindHost, bindPort, true) ;
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
        // startStateMachine();
        raftState.start();
        listener = new NioListener(bindHost, bindPort, this);
        listener.start();
    }

    public void stop() throws Exception {
        LOG.info(getServerId() + ": is stopping") ;
        stop = true;
        raftState.stop();
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

        if (raftState.raftState() == RaftState.leader || raftState.raftState() == RaftState.candidate) {
            LOG.info(getServerId() + " Change state from leader to follower. New leader is  "+ leaderId);

        }
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

    public void setElectionInProgress(int term, Candidate cd) {
        electionInProgress = true;
        currentElectionTerm = term ;
        leaderElection = cd;
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
    }

    public void setRaftState(State state) {
        raftState = state ;
        if (state.raftState() == RaftState.leader) {
            leader = thisMember;
        }
    }

    public RaftState getRaftState() {
        return raftState.raftState();
    }

    public InBoundMessageCreator getInBoundMessageCreator() {
        return inBoundMessageCreator;
    }

    public boolean processLogEntry(LogEntry e, int prevIndex, int lastComittedIndex) throws Exception {
        boolean ret = true ;
        if (e != null) {
            byte[] data = e.getEntry();
            int expectedNextEntry = rlog.size();
            if (prevIndex + 1 == expectedNextEntry) {
                addLogEntry(data);
                ret = true ;
                if (lastComittedIndex <= expectedNextEntry) {
                    this.lastComittedIndex.set(lastComittedIndex);
                }
            } else {
                ret = false ;
            }
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
        int port = Integer.parseInt(args[0]) ;
        int size = args.length   ;
        String[] seeds = null ;
        if (size > 1) {
            seeds = new String[args.length-1] ;
            int j = 0 ;
            for (int i = 1 ; i < size ; i++) {
                seeds[j] = args[i] ;
                ++j ;
            }

        }
        LOG.info("Starting server listening on port: " + port) ;
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


    public void sendAppendEntriesMessage(Peer peer) throws Exception {
        Member m = peer.member();
        PeerData v = memberPeerDataMap.get(m);
        AppendEntriesMessage p = new AppendEntriesMessage(getTerm(), getServerId(),
                v.getNextSeq(),
                rlog.size()-1,
                lastComittedIndex.get());

        int index = getIndexToReplicate(v) ;
        if (index >= 0 && index < rlog.size()) {
            byte[] data = rlog.get(index);
            // LOG.info("Replicating ..." + ByteBuffer.wrap(data).getInt());
            LogEntry entry = new LogEntry(index, data);
            p.addLogEntry(entry);
            p.setPrevIndex(index-1);
        }
        v.addToSeqIdIndexMap(p);
        peer.queueSendMessage(p);
        ConcurrentLinkedQueue<Integer> q = new ConcurrentLinkedQueue<Integer>();
        q.add(1); // self
        ackCountMap.put(index, q);
    }

    public void sendClusterInfoMessage(Peer peer) throws Exception {
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

    public List<Peer> getPeers() {

        return new ArrayList(connectedMembersMap.values()) ;

    }

    public void logRlog() throws Exception {
        StringBuilder sb = new StringBuilder("Replicated Log [") ;
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
        if (indexQueue.size() >= majority ) {
            lastComittedIndex.set(index) ;
            ackCountMap.remove(index) ;
            LOG.info("Last committed index="+lastComittedIndex.get());
        }
    }

    public State getStateMachine(RaftState raftState) {
        State state = new Follower(this);
        switch (raftState) {
            case leader:
                state = new Leader(this);
                break;
            case follower:
                break;
            case candidate:
                state = new Candidate(this);
                break;
        }

        return state;
    }

    public void startTask(Runnable r) {
        peerServerExecutor.submit(r);
    }
}

