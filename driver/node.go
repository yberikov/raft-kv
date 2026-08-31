package driver

import (
	"math/rand"

	"raft-kv/kv"
	"raft-kv/raft"
	"raft-kv/storage"
)

type Config struct {
	Id               int
	Peers            []int
	MinElectionTicks int
	MaxElectionTicks int
	ReplicatePeriod  int
	Rng              *rand.Rand
}

type Applied struct {
	Index  int
	Entry  raft.Entry
	Result kv.Result
}

// outboundBatch is a set of messages that must not leave the node until the
// entries they depend on are durable. A leader that sends AppendEntries for
// entries it has not yet fsynced can lose them to a crash while followers
// keep them, and a vote it has not persisted can be cast twice across a
// restart.
type outboundBatch struct {
	requiredIndex int
	messages      []raft.Message
}

type Node struct {
	CompactThreshold int

	cfg     Config
	core    *raft.Core
	storage storage.Storage
	machine *kv.StateMachine
	pending *pendingSet
	outbox  []outboundBatch
}

func NewNode(cfg Config, st storage.Storage) *Node {
	return &Node{
		cfg:     cfg,
		core:    raft.NewCore(cfg.Id, cfg.Peers, cfg.MinElectionTicks, cfg.MaxElectionTicks, cfg.Rng, cfg.ReplicatePeriod),
		storage: st,
		machine: kv.NewStateMachine(),
		pending: &pendingSet{waiters: make(map[int]*Pending)},
	}
}

func (n *Node) Id() int                  { return n.cfg.Id }
func (n *Node) Status() raft.Status      { return n.core.Status() }
func (n *Node) Storage() storage.Storage { return n.storage }
func (n *Node) Step(m raft.Message)      { n.core.Step(m) }

// Propose appends commands to the log and returns one Pending per command, in
// the order given. The returned bool is false if this node is not the leader,
// in which case Status().LeaderId is the (advisory) redirect hint.
func (n *Node) Propose(cmds []raft.Command) ([]*Pending, bool) {
	index, term, ok := n.core.Propose(cmds)
	if !ok {
		return nil, false
	}
	out := make([]*Pending, len(cmds))
	for i := range cmds {
		out[i] = n.pending.add(index+i, term)
	}
	return out, true
}

// Tick advances the node by one unit of time and performs everything Ready
// asks for. It returns the messages that are now cleared to send (their
// entries are durable) and the entries that reached the state machine.
func (n *Node) Tick() ([]raft.Message, []Applied) {
	n.core.Tick()
	n.storage.Tick()

	ready := n.core.Ready()

	if ready.StateChanged {
		n.storage.PersistState(ready.Term, ready.VotedFor)
	}

	if ready.TruncateFrom != 0 {
		n.storage.TruncateFrom(ready.TruncateFrom)
		n.pending.failFrom(ready.TruncateFrom, "truncated")
	}

	if ready.SnapshotIndex != 0 {
		n.storage.PersistSnapshot(ready.SnapshotIndex, ready.SnapshotTerm, ready.SnapshotData)
		n.storage.ReclaimSegments()
		n.machine.Restore(ready.SnapshotData)
		n.pending.failUpTo(ready.SnapshotIndex, "snapshot installed")
	}

	if len(ready.EntriesToPersist) > 0 {
		n.storage.AppendEntries(ready.EntriesToPersist)
	}

	if len(ready.Messages) > 0 {
		requiredIndex := 0
		if len(ready.EntriesToPersist) > 0 {
			requiredIndex = n.storage.LastIndex()
		}
		n.outbox = append(n.outbox, outboundBatch{requiredIndex: requiredIndex, messages: ready.Messages})
	}

	var applied []Applied
	if len(ready.EntriesToApply) > 0 {
		results := n.machine.Apply(ready.EntriesToApply)
		applied = make([]Applied, len(results))
		for i, res := range results {
			index := ready.ApplyFrom + i
			applied[i] = Applied{Index: index, Entry: ready.EntriesToApply[i], Result: res}
			n.pending.resolve(index, ready.EntriesToApply[i].Term, res)
		}
	}

	var send []raft.Message
	durable := n.storage.LastFsyncedIndex()
	for len(n.outbox) > 0 && n.outbox[0].requiredIndex <= durable {
		send = append(send, n.outbox[0].messages...)
		n.outbox = n.outbox[1:]
	}

	n.maybeCompact()
	return send, applied
}

func (n *Node) maybeCompact() {
	if n.CompactThreshold <= 0 {
		return
	}
	status := n.core.Status()
	if status.CommitIndex-status.StartIndex < n.CompactThreshold {
		return
	}
	index := status.CommitIndex
	n.core.CompactLog(index, status.Log[index-status.StartIndex].Term, n.machine.Snapshot())
}

// Restart rebuilds the node from persisted state alone, the way a process
// restart would: the in-memory Core is discarded and reconstructed from
// storage, and anything that was only in memory is gone.
func (n *Node) Restart() {
	term, votedFor := n.storage.State()
	snapIndex, snapTerm, snapData := n.storage.SnapshotState()
	log := n.storage.Entries(snapIndex, n.storage.LastIndex()+1)

	core := raft.NewCore(n.cfg.Id, n.cfg.Peers, n.cfg.MinElectionTicks, n.cfg.MaxElectionTicks, n.cfg.Rng, n.cfg.ReplicatePeriod)
	core.Restore(term, votedFor, log, snapIndex, snapTerm, snapData)
	n.core = core
	n.outbox = nil

	n.machine = kv.NewStateMachine()
	if snapIndex > 0 {
		n.machine.Restore(snapData)
	}

	n.pending.failAll("restarted")
}

func (n *Node) Crash() {
	n.storage.TruncateFrom(n.storage.LastFsyncedIndex() + 1)
	n.Restart()
}

func (n *Node) Snapshot() any { return n.machine.Snapshot() }

func (n *Node) Compact(index int, term uint64) any {
	snap := n.machine.Snapshot()
	n.core.CompactLog(index, term, snap)
	return snap
}
