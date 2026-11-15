# mjraft
Java implementation of the raft protocol as described by the paper https://raft.github.io/raft.pdf

## Supported features
- Log replication
- Leader election
- Client API to submit log entries
- Embeddable into your distributed application
- Redirect to leader from follower/candidate
- Consistency check

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
    new PeerServer(m.getPort(), members).start();
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
