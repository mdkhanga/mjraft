package com.mj.distributed.peertopeer.server;

import com.mj.distributed.model.*;
import com.mj.distributed.tcp.Connection;
import com.mj.distributed.tcp.ConnectionListener;
import com.mj.distributed.tcp.TcpServer;
import com.mj.distributed.message.Message;
import com.mj.raft.log.RaftLog;
import com.mj.raft.states.Follower;
import com.mj.raft.states.Candidate;
import com.mj.raft.states.State;
import com.mj.raft.storage.FileRaftStorage;
import com.mj.raft.storage.PersistentState;
import com.mj.raft.storage.RaftStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class PeerServer implements ConnectionListener {

    // outbound connections to other members, one per member while connected
    private final ConcurrentMap<Member, Peer> connectedMembersMap = new ConcurrentHashMap<>() ;
    private volatile ConcurrentMap<Member, PeerData> memberPeerDataMap = new ConcurrentHashMap<>() ;
    // reverse index; also holds inbound raft client connections
    private final ConcurrentMap<Connection, Peer> connectionPeerMap = new ConcurrentHashMap<>() ;

    private final ClusterConfig config ;
    private volatile Member leader ; // null until a leader is known

    private static Logger LOG  = LoggerFactory.getLogger(PeerServer.class) ;

    private String bindHost = "localhost" ;
    private int bindPort ;

    private TcpServer tcpServer;
    private final Member thisMember ;

    private final RaftStorage storage;
    final RaftLog raftLog;

    // guards storedTerm and votedFor, which must change together with what is on disk
    private final ReentrantLock termLock = new ReentrantLock();
    // the highest term saved to storage: the latest term seen or voted in
    private int storedTerm;
    private volatile Member votedFor; // in storedTerm, or null

    volatile AtomicInteger currentTerm = new AtomicInteger(0);
    volatile Long lastLeaderHeartBeatTs = 0L ;
    volatile State raftState ;
    ExecutorService peerServerExecutor = Executors.newFixedThreadPool(6) ;
    private final AtomicBoolean stop = new AtomicBoolean(false) ;
    private volatile boolean electionInProgress = false ;
    private volatile int currentElectionTerm = -1 ;
    private volatile Candidate leaderElection ;

    private volatile int currentVotedTerm = -1;

    /**
     * @param storage where term, vote and log are kept across restarts. The server closes it on stop.
     */
    public PeerServer(ClusterConfig config, RaftStorage storage) {
        this.config = config ;
        this.storage = storage;
        this.raftLog = new RaftLog(storage);
        this.thisMember = config.self();
        this.bindHost = thisMember.getHostString();
        this.bindPort = thisMember.getPort();
        LOG.info("Starting server on port: " + bindPort + " cluster: " + config.members());
        // every server starts as a follower; the first leader is elected
        raftState = new Follower(this) ;
    }

    public void start() throws Exception {

        PersistentState saved = storage.load();
        storedTerm = saved.term();
        votedFor = saved.votedFor();
        currentTerm.set(saved.term());
        raftLog.restore(saved.log());
        LOG.info(getServerId() + ": loaded term " + saved.term() + ", vote " + saved.votedFor()
                + ", " + saved.log().size() + " log entries");

        // bind first so a port conflict fails start()
        tcpServer = TcpServer.start(bindHost, bindPort, this);

        for (Member m : config.others()) {
            new PeerClient(m, this).start();
        }

        // the election timeout counts from startup until a leader is heard from
        setLastLeaderHeartBeatTs(System.currentTimeMillis());
        raftState.start();
    }

    /**
     * Stops the state machine, closes the listening socket and every connection, and shuts down threads.
     * Safe to call more than once.
     */
    public void stop() throws Exception {
        if (!stop.compareAndSet(false, true)) {
            return;
        }
        LOG.info(getServerId() + ": is stopping") ;
        raftState.stop();
        if (tcpServer != null) {
            tcpServer.close();
        }
        for (Connection c : connectionPeerMap.keySet()) {
            c.close();
        }
        peerServerExecutor.shutdownNow();
        storage.close();
    }

    public boolean isStopped() {
        return stop.get();
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

    /**
     * Saves t first if it is a new term. A term already saved keeps its vote.
     */
    public void setTerm(int t) throws IOException {
        termLock.lock();
        try {
            if (t > storedTerm) {
                storage.saveTermAndVote(t, null);
                storedTerm = t;
                votedFor = null;
            }
            currentTerm.set(t);
        } finally {
            termLock.unlock();
        }
    }

    /**
     * Votes for candidate in term, saving the vote before returning, and marks an election in progress.
     * Refused if this server has already voted in, or seen, term or a later one, so it never votes twice in a term.
     *
     * @param election the election this server is running, if candidate is this server
     * @return true if the vote was cast
     */
    public boolean recordVote(int term, Member candidate, Candidate election) throws IOException {
        termLock.lock();
        try {
            if (term <= storedTerm) {
                return false;
            }
            storage.saveTermAndVote(term, candidate);
            storedTerm = term;
            votedFor = candidate;
            setElectionInProgress(term, election);
            return true;
        } finally {
            termLock.unlock();
        }
    }

    public Member getVotedFor() {
        return votedFor;
    }

    public Member getThisMember() {
        return thisMember;
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

    public ClusterConfig getConfig() {
        return config;
    }

    public ClusterInfo getClusterInfo() {
        return new ClusterInfo(leader, new ArrayList<>(config.members()));
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
        return thisMember.equals(leader);
    }

    public Member getLeader() {
        return leader;
    }

    public void queueSendMessage(Connection c, Message m) throws Exception {
        c.send(m.serialize());
    }

    public State getRaftState() {
        return raftState;
    }

    public void setRaftState(State state) {
        raftState = state ;
        if (state.raftState() == RaftState.leader) {
            leader = thisMember;
        }
    }

    /**
     * Usage: PeerServer port [host:port,host:port,...] [--data-dir dir]
     * The list names every server in the cluster, including this one. Without it, a single-server cluster starts.
     * The data directory defaults to data/port.
     */
    public static void main(String args[]) throws Exception {
        List<String> rest = new ArrayList<>(List.of(args));
        Path dataDir = null;
        int flag = rest.indexOf("--data-dir");
        if (flag >= 0 && flag + 1 < rest.size()) {
            dataDir = Path.of(rest.get(flag + 1));
            rest.subList(flag, flag + 2).clear();
        }
        if (rest.isEmpty() || rest.size() > 2 || rest.contains("--data-dir")) {
            System.out.println("Usage: mjraft <port> [host:port,host:port,...] [--data-dir <dir>]") ;
            System.exit(1);
        }
        int port = Integer.parseInt(rest.get(0)) ;
        if (dataDir == null) {
            dataDir = Path.of("data", String.valueOf(port));
        }
        Member self = new Member("localhost", port);
        ClusterConfig config = rest.size() == 2 ? ClusterConfig.parse(self, rest.get(1)) : ClusterConfig.single(self);
        LOG.info("Starting server listening on port: " + port + " with data in " + dataDir.toAbsolutePath()) ;
        PeerServer peerServer = new PeerServer(config, FileRaftStorage.open(dataDir)) ;
        peerServer.start() ;
    }

    // inbound connections from other members are not registered: we only receive on them and reply

    @Override
    public void onFrame(Connection c, ByteBuffer frame) {
        MessageDispatcher.dispatch(frame, c, this);
    }

    @Override
    public void onClosed(Connection c) {
        LOG.info(getServerId() + ": connection from " + c.remoteAddress() + " dropped") ;
        removePeer(c);
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
        raftLog.sendAppendEntriesMessage(peer, v, getServerId(), getTerm());
    }

    /**
     * Forgets a closed connection. Membership is unaffected: the member still counts towards the majority.
     */
    public void removePeer(Connection c) {
        Peer p = connectionPeerMap.remove(c) ;
        if (p != null && p.member() != null) {
            connectedMembersMap.remove(p.member(), p);
        }
    }

    public void addPeer(Connection c, Peer p) {
        connectionPeerMap.put(c, p) ;
        connectedMembersMap.put(p.member(), p);
        memberPeerDataMap.put(p.member(), new PeerData(p.member().getHostString(), p.member().getPort()));
        if (isStopped()) {
            c.close(); // raced with stop(), which may already have closed the others
        }
    }

    public void decrementNextIndex(Connection c) {
        Peer p = connectionPeerMap.get(c);
        Member m = p.member();
        PeerData v = memberPeerDataMap.get(m);
        v.decrementNextIndexToReplicate();
    }

    public void addRaftClient(Connection c) {
        connectionPeerMap.put(c, new ConnectionPeer(null, c));
    }

    public Peer getPeer(Connection c) {
        return connectionPeerMap.get(c) ;
    }

    public PeerData getPeerData(Connection c) {
        Peer p = getPeer(c) ;
        return memberPeerDataMap.get(p.member());
    }

    /**
     * @return the open connection to m, or null if not currently connected
     */
    public Peer getConnectedPeer(Member m) {
        return connectedMembersMap.get(m) ;
    }

    public List<Peer> getPeers() {

        return new ArrayList<>(connectedMembersMap.values()) ;

    }

    public RaftLog getRaftLog() {
        return raftLog;
    }


    public void updateIndexAckCount(int index) {
        raftLog.updateIndexAckCount(index, config.size());
    }

    public void startTask(Runnable r) {
        try {
            peerServerExecutor.submit(r);
        } catch (RejectedExecutionException e) {
            // a state change raced with stop(); nothing should run after stop
            LOG.debug(getServerId() + ": not starting task after stop");
        }
    }
}
