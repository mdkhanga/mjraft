# mjraft
Java implementation of the raft protocol as described by the paper https://raft.github.io/raft.pdf
This is work in progress.

## Supported features
- Log replication
- Leader election
- Client API to submit log entries
- Embeddable into your distributed application

## Usage

### Building the code

mvn clean install

### command line


### Embedded

#### Start a new cluster by starting a server

PeerServer leader = new PeerServer(5001);
leader.start() ;

#### Add additional servers to the cluster

String[] seeds = new String[1];
seeds[0] = "localhost:5001";

PeerServer server1 = new PeerServer(2, 5002, seeds);
server1.start();

PeerServer server2 = new PeerServer(3, 5003, seeds);
server2.start();

### Client api

#### Send log entries to the leader

RaftClient raftClient = new RaftClient("localhost", 5001);
raftClient.connect();
raftClient.send(23);

#### Get log entries from leader



