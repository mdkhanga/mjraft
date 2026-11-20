# Plan

Incremental changes to the existing code, not a rewrite. Core Raft first.

## Core Raft
| Step | What | Status |
|---|---|---|
| 1 | Build with Gradle (project-local wrapper) | Done |
| 2 | Test foundation: free ports, wait helpers, separate cluster tests | Done |
| 3 | Transport on blocking sockets and virtual threads; remove NIO | Done |
| 4 | Fixed, configured cluster membership | Done |
| 5 | Persistence: term, vote and log written to disk before replying, and loaded on startup | Done |
| 6 | Leader election matching Figure 2 | Next |
| 7 | Log replication matching Figure 2 | |
| 8 | Applying entries to a state machine; client acknowledgements | |
| 9 | Fault tests: crashes, restarts, partitions | |
| 10 | Modern Java cleanup | |

## After core Raft
- Snapshots and log compaction (section 7 of the paper)
- Membership changes
- Exactly-once writes and linearizable reads
- Pre-vote
- Configuration file (YAML) for the port, cluster members, data directory and timeouts, so servers
  do not need long command lines

## Hardening
- Deterministic simulation testing
- Linearizability checking of client histories
