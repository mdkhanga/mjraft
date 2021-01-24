# mjraft
Java implementation of the raft protocol as described by the paper https://raft.github.io/raft.pdf
This is work in progress.

## Supported features
- Log replication
- Leader election
- Client API to submit log entries
- Embeddable into your distributed application
- Redirect to leader from follower/candidate

## Usage

### Building the code

mvn clean install

### Command line

....

### Embedded

#### Start a new cluster by starting a server

```
PeerServer leader = new PeerServer(5001);
leader.start() ;
```

#### Add additional servers to the cluster

```
String[] seeds = new String[1];
seeds[0] = "localhost:5001"; // can connect to any server

PeerServer server1 = new PeerServer(5002, seeds);
server1.start();

PeerServer server2 = new PeerServer(5003, seeds);
server2.start();
```

### Client api

#### Send log entries to the leader

```
RaftClient raftClient = new RaftClient("localhost", 5001); // can connect to any server
raftClient.connect();
raftClient.send(23);
```

#### Get log entries from leader

```
List<byte[]> serverValues = raftClient.get(0,1);
```

