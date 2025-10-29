package raft

import (
    "fmt"
    "log"
    "os"
    "sync"
    "time"
    "math/rand"
)

const Follower = 0
const Candidate = 1
const Leader = 2
const Dead = 3

type LogEntry struct {
    Command any
    Term int
}

type RequestVoteArgs struct {
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

type RequestVoteReply struct {
    Term int
    VoteGranted bool
}

type AppendEntriesArgs struct {
    Term int
    LeaderId int
    PrevLogIndex int
    PrevLogTerm int
    Entries []LogEntry
    LeaderCommit int
}

type AppendEntriesReply struct {
    Term int
    Success bool
}

type ConsensusModule struct {
    mu sync.Mutex

    /* persistent raft state */
    currentTerm int
    votedFor int
    log []LogEntry

    /* volatile raft state */
    state int
    electionResetEvent time.Time


    /* server related */
    id int
    peerIds []int

    server *Server
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
    cm.mu.Lock()

    if cm.state != Leader {
        cm.mu.Unlock()
        return
    }
    savedCurrentTerm := cm.currentTerm
    cm.mu.Unlock()

    for _, peerId := range cm.peerIds {
        args := AppendEntriesArgs{
            Term: savedCurrentTerm,
            LeaderId: cm.id,
        }
        
        go func() {
            var reply AppendEntriesReply

            if err := cm.server.Call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
                cm.mu.Lock()
                defer cm.mu.Unlock()

                if reply.Term > savedCurrentTerm {
                    log.Println("term out of date")
                    cm.becomeFollower(reply.Term)
                    return
                }
            }
        }()
    }
}

func (cm *ConsensusModule) startLeader() {
    cm.state = Leader
    
    go func() {
        ticker := time.newTicker(50 * time.Millisecond)
        defer ticker.Stop()

        for {
            cm.leaderSendHeartbeats()
            <-ticker.C

            cm.mu.Lock()
            if cm.state != Leader {
                cm.mu.Unlock()
                return
            }
            cm.mu.Unlock()
        }
    }()
}

func (cm *ConsensusModule) becomeFollower(term int) {
    cm.state = Follower
    cm.currentTerm = term
    cm.votedFor = -1
    cm.electionResetEvent = time.Now()

    go cm.runElectionTimer()
}

func (cm *ConsensusModule) electionTimeout() time.Duration {
    if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
        return time.duration(150) * time.Millisecond
    }

    return time.Duration(150 + rand.Intn(150)) * time.Millisecond
}

func (cm *ConsensusModule) startElection() {
    cm.state = Candidate
    cm.currentTerm += 1
    savedCurrentTerm := cm.currentTerm
    cm.electionResetEvent = time.Now()
    cm.votedFor = cm.id
    
    votesReceived := 1

    for _, peerId := range cm.PeerIds {
        go func() {
            args := RequestVoteArgs{
                Term: savedCurrentTerm,
                CandidateId: cm.id,
            }

            var reply RequestVoteReply

            if err := cm.server.Call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
                cm.mu.Lock()
                defer cm.mu.Unlock()

                // In the time since the RPC has succeeded, our state can change.
                // So we have to confirm we're still a Candidate.
                if cm.state != Candidate {
                    return
                }

                if reply.Term > savedCurrentTerm {
                    log.Println("term out of date")
                    cm.becomeFollower(reply.term)
                    return
                } else if reply.Term == savedCurrentTerm {
                    if reply.VoteGranded {
                        votesReceived += 1
                        if votesReceived > len(cm.peerIds) {
                            log.Println("won election")
                            cm.startLeader()
                            return
                        }
                    }
                }
            }
        }()
    }

    go cm.runElectionTimer()
}

func (cm *ConsensusModule) runElectionTimer() {
    timeoutDuration := cm.electionTimeout()

    cm.mu.Lock()
    termStarted := cm.currentTerm
    cm.mu.Unlock()

    log.Println("election timer started")

    ticker := time.NewTicker(10 * time.Millisecond)
    defer ticker.Stop()

    for {
        <-ticker.C

        cm.mu.Lock()
        if cm.state != Candidate && cm.state != Follower {
            log.Println("not candidate or follower state, no op")
            cm.mu.Unlock()
            return
        }

        if termStarted != cm.currentTerm {
            log.Println("election timer term has changed, no op")
            cm.mu.Unlock()
            return
        }
        
        elapsed := time.Since(cm.electionResetEvent())
        if elapsed >= timeoutDuration {
            cm.startElection()
            cm.mu.Unlock()
            return
        }
        cm.mu.Unlock()
    }
}

