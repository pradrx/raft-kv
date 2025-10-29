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

func (cm *ConsensusModule) electionTimeout() time.Duration {
    if len(os.Getenv("RAFT_FORCE_MORE_REELECTION")) > 0 && rand.Intn(3) == 0 {
        return time.duration(150) * time.Millisecond
    }

    return time.Duration(150 + rand.Intn(150)) * time.Millisecond
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

