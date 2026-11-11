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

#### Start a new cluster by starting a server listening on port 5001

```sh
build/install/mjraft/bin/mjraft 5001
```

#### Add a second server listening on port 5002 which connects to the first

```sh
build/install/mjraft/bin/mjraft 5002 localhost:5001
```

#### Add a third server listening on port 5003 which connects to the first

```sh
build/install/mjraft/bin/mjraft 5003 localhost:5001
```

#### Use RaftClient to connect to any server and send values to be replicated

```sh
java -cp "build/install/mjraft/lib/*" com.mj.raft.client.RaftClient localhost 5001
```

### Embedded

#### Start a new cluster by starting a server

```java
PeerServer leader = new PeerServer(5001);
leader.start() ;
```

#### Add additional servers to the cluster

```java
String[] seeds = new String[1];
seeds[0] = "localhost:5001"; // can connect to any server

PeerServer server1 = new PeerServer(5002, seeds);
server1.start();

PeerServer server2 = new PeerServer(5003, seeds);
server2.start();
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
