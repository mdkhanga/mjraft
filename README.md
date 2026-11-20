# mjraft
Java implementation of the raft protocol as described by the paper https://raft.github.io/raft.pdf

## Status

mjraft was started in 2016 and is being revived: completing core Raft, testing it under failure,
and modernizing it to Java 21 and virtual threads. See [docs/PLAN.md](docs/PLAN.md) for the plan.

**Working today**
- Leader election among a fixed, configured set of servers. A majority of the configured
  cluster is required, so a minority can never elect a leader.
- Log replication from the leader to followers
- Durable state: term, vote and log are written to disk before a server replies, and loaded on restart
- Client API to submit and read log entries, with redirect to the leader
- Networking on blocking sockets and virtual threads, with length-prefixed frames
- Unit tests, plus cluster tests that start real servers and stop some of them

**Not yet implemented**
- Election and replication rules exactly as in Figure 2 of the paper (election restriction,
  commit only entries from the current term, log repair after conflicts)
- Applying committed entries to a state machine and acknowledging clients
- Snapshots, membership changes, linearizable reads, pre-vote

**Known issues**
- After a reconnect or restart, the leader restarts replication to that follower from the
  first entry, which the follower rejects, so the follower may not receive new entries
  (fixed in step 7, replication matching Figure 2).
- A server that voted for a candidate that then failed cannot stand for election itself
  until it hears from a leader. If every remaining server is in this state, no leader is
  elected (fixed in step 6).
- A server refuses to vote in a term it has seen but not voted in. This is safe, but can
  make elections take longer (fixed in step 6).

## Usage
### Building the code

Requires JDK 21. The Gradle wrapper downloads Gradle itself; no separate install is needed.

```sh
./gradlew build
```

To create a runnable distribution under `build/install/mjraft`:

```sh
./gradlew installDist
```

### Running tests

Tests are split into fast unit tests and slower multi-server cluster tests.
Cluster tests are tagged `cluster` and start real servers on free local ports.

Run the unit tests only (seconds):

```sh
./gradlew test
```

Run the cluster tests only (a few minutes):

```sh
./gradlew clusterTest
```

Run everything, as part of the full build:

```sh
./gradlew build
```

Run a single test class or method:

```sh
./gradlew test --tests com.mj.distributed.tcp.FrameCodecTest
./gradlew clusterTest --tests 'com.mj.distributed.peertopeer.server.PeerServerTest.logReplication'
```

Gradle skips a test task whose inputs have not changed since its last run. To force tests to run again:

```sh
./gradlew clusterTest --rerun
```

Test reports are written to `build/reports/tests/test/index.html` and
`build/reports/tests/clusterTest/index.html`.

### Command line cluster

Every server is given the full list of cluster members, including itself.
The membership is fixed: a majority of this list is needed to elect a leader or commit an entry,
so a cluster of 3 keeps working with one server down, but not with two.
All servers start as followers and elect a leader among themselves.

#### Start a three server cluster, one command per terminal

```sh
build/install/mjraft/bin/mjraft 5001 localhost:5001,localhost:5002,localhost:5003
```

```sh
build/install/mjraft/bin/mjraft 5002 localhost:5001,localhost:5002,localhost:5003
```

```sh
build/install/mjraft/bin/mjraft 5003 localhost:5001,localhost:5002,localhost:5003
```

With only a port, a single server cluster starts and elects itself.

Each server keeps its term, vote and log in a data directory, `data/<port>` by default.
A restarted server picks up where it left off. To choose the directory:

```sh
build/install/mjraft/bin/mjraft 5001 localhost:5001,localhost:5002,localhost:5003 --data-dir /var/lib/mjraft
```

#### Use RaftClient to connect to any server and send values to be replicated

```sh
java -cp "build/install/mjraft/lib/*" com.mj.raft.client.RaftClient localhost 5001
```

### Embedded

#### Start a three server cluster

```java
List<Member> members = List.of(
        new Member("localhost", 5001),
        new Member("localhost", 5002),
        new Member("localhost", 5003));

for (Member m : members) {
    ClusterConfig config = new ClusterConfig(m, members);
    RaftStorage storage = FileRaftStorage.open(Path.of("data", String.valueOf(m.getPort())));
    new PeerServer(config, storage).start();
}
```

### Client api

#### Send log entries to the leader

```java
RaftClient raftClient = new RaftClient("localhost", 5001); // can connect to any server
raftClient.connect();
raftClient.send(23);
```

#### Get log entries from leader

```java
List<byte[]> serverValues = raftClient.get(0,1);
```
