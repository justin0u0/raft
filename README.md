# Raft

Author: Justin Chen <<mail@justin0u0.com>>

# Goal

This is a Raft implementation providing with a template code (at branch `template`) with well designed structure, 10 test cases for verifying your implementation.

The implementation is for education/personal training purpose. NOT for any production usage.

## For Students of the NTHU CS5426 Distributed System Course

In the final project, you are asked to implement the Raft consensus algorithm, also a replicated state machine protocol, as the [paper](https://raft.github.io/raft.pdf) described and learn to:

- Use Go to implement the Raft module and learn to use Go to develop a concurrent, distributed applications.
- Use gRPC to communicate between services.
- The first version of your Raft implementation may be buggy. Learn to develop and debug in a large distributed applications. Be careful about deadlock and data race condition. Most of you bugs can be found by logging all the variables and states.
- Learn to design and write tests to verify your implementation.

The implementation will be divided into 2 parts: leader election, log replication.

# Getting Started

Download the Raft template [https://github.com/justin0u0/raft/archive/refs/tags/template-v1.0.2.zip](https://github.com/justin0u0/raft/archive/refs/tags/template-v1.0.2.zip).

Note that for all `TODO`s without a `*`, the description can be found in the paper figure 2.

Read the following sections to understand how to implement the Raft algorithm with this template. It is highly recommended to use a version control system (Git) to manage your implementation.

If you encounter any problem, please feel free to contact me.

# Implementation

## Description

Before you start the implementation, let’s go through the project layout, structure and design.

On the Raft server starts up, the server will changed between 3 states: follower, candidate and leader.

![the Raft state machine](https://user-images.githubusercontent.com/38185674/166299831-26328bf8-8cae-45e3-90e7-8069c2211574.png)

The following code describes the main running loop of the Raft server:

```go
// raft/raft.go

func (r *Raft) Run(ctx context.Context) {
	// ignore some lines ...

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("raft server stopped gracefully")
			return
		default:
		}

		switch r.state {
		case Follower:
			r.runFollower(ctx)
		case Candidate:
			r.runCandidate(ctx)
		case Leader:
			r.runLeader(ctx)
		}
	}
}
```

The Raft server listens to 2 RPCs: `AppendEntries` and `RequestVote`. In the implementation, the `ApplyCommand` RPC is added, providing an interface for the client to apply new log to the Raft leader.  The implementation rely on gRPC for the communication, all related codes are already generated in the `pb` folder.

![The Raft RPCs](https://user-images.githubusercontent.com/38185674/166299945-5955e588-52d7-45df-a7dd-b52f185fe4b4.png)

To handle incoming RPCs, the functions `ApplyCommand`, `AppendEntries` and `RequestVote` must be implemented. Since gRPC automatically handle each request in a goroutine, each call to these functions may be concurrent. This complicates the implementation if every things can be run in parallel. Locks need to be used everywhere.

So the implementation collects incoming RPCs inside a channel, then wait for the request to be handled then returned. See `~/raft/rpc.go` for detailed implementation. However, all you need to know is that each request is now collected in the `rpcCh`, and each call to the `handleRPCRequest` function handle a RPC from the `rpcCh`.

Now, lets see what each state is responsible for:

### Follower

Either heartbeat timeout then do `handleFollowerHeartbeatTimeout` or handle an incoming request.

> 👉 Note that you can see that although timeout and incoming requests may occur concurrently, we serialize them using Go’s *select-case* statement and to them one by one. Again, to reduce unnecessary locks and complexity of preventing from data race. The candidate and leader state use the same strategy to serialize concurrent operations too.

```go
func (r *Raft) runFollower(ctx context.Context) {
	r.logger.Info("running follower")

	timeoutCh := randomTimeout(r.config.HeartbeatTimeout)

	for r.state == Follower {
		select {
		case <-ctx.Done():
			return

		case <-timeoutCh:
			timeoutCh = randomTimeout(r.config.HeartbeatTimeout)
			if time.Now().Sub(r.lastHeartbeat) > r.config.HeartbeatTimeout {
				r.handleFollowerHeartbeatTimeout()
			}

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}
```

### Candidate

Candidate first vote for itself, then request vote from peers **in parallel**. After that, it either handle a vote response, timeout then rerun candidate state or handle an incoming request.

```go
func (r *Raft) runCandidate(ctx context.Context) {
	// ignore some lines ...

	timeoutCh := randomTimeout(r.config.ElectionTimeout)

	for r.state == Candidate {
		select {
		case <-ctx.Done():
			return

		case vote := <-voteCh:
			r.handleVoteResult(vote, &grantedVotes, votesNeeded)

		case <-timeoutCh:
			r.logger.Info("election timeout reached, restarting election")
			return

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}
```

### Leader

On leader starts up, it first reset `nextIndex` and `matchIndex` as the paper mentioned. After that, it either timeout then send heartbeat, handle an append entries response or handle an incoming request.

```go
func (r *Raft) runLeader(ctx context.Context) {
	// ignore some lines ...

	for r.state == Leader {
		select {
		case <-ctx.Done():
			return

		case <-timeoutCh:
			timeoutCh = randomTimeout(r.config.HeartbeatInterval)

			r.broadcastAppendEntries(ctx, appendEntriesResultCh)

		case result := <-appendEntriesResultCh:
			r.handleAppendEntriesResult(result)

		case rpc := <-r.rpcCh:
			r.handleRPCRequest(rpc)
		}
	}
}
```

## Leader Election (Part A)

Finish TODO A.1 ~ A.15.

You should pass tests `TestInitialElection`, `TestElectionAfterLeaderDisconnect`, `TestElectionAfterLeaderDisconnectLoop`, and `TestFollowerDisconnect` after part A is finished.

## Log Replication (Part B)

Finish TODO B.1 ~ B.9.

You should pass all tests after part A and B are finished.

# Verification

```go
go test -timeout 60s -race -count 1 ./...
```

> 💡 You can add `-v` flag when testing to show all logs even if the test pass.

If the test does not pass, it is suggested to understand what is the test testing for, then using the log to find out bugs and errors. For example, the `TestLogReplicationWithFollowerFailure` test is testing for “a disconnected follower should not affect the log replication to other followers” and “after the follower comes back, the missing logs should be replicated to the follower”. If you have hard time understanding the test cases, please feel free to contact me 😊。

# Future Work

There are many other works can be done to improve the Raft we designed, the following are some:

1. Log compaction: It is not practical for a long-running Raft server to store the complete logs forever. Instead, store a snapshot of the state from time. So Raft can discards log entries that precede the snapshot.
2. The paper mentioned that **"if AppendEntries fails because of log inconsistency: decrement nextIndex and retry"**. In the implementation, `nextEntry` decrease by 1 at once, it may need to retry too many times if the replication lag is huge. By adding more information to the `AppendEntries` RPC's response, can you find a way to know how much should `nextEntry` decrease if log inconsistency occur?
