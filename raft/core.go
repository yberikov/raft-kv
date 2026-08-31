package raft

import "math/rand"

type Core struct {
	id          int
	leaderId    int
	peers       []int
	currentTerm uint64
	votedFor    int
	state       stateType
	log         []Entry
	commitIndex int

	minElectionTicks int
	maxElectionTicks int
	electionTimeout  int
	rng              *rand.Rand
	electionElapsed  int
	votesGranted     map[int]bool

	replicatePeriod  int
	replicateElapsed int
	nextIndex        map[int]int // nodeID - index
	matchIndex       map[int]int

	startIndex   int
	startTerm    uint64
	snapshotData any

	msgs []Message

	persistedTerm     uint64
	persistedVotedFor int

	persistedIndex      int
	truncatedFrom       int
	persistedStartIndex int

	appliedIndex int
}

type (
	Entry struct {
		Cmd  Command
		Term uint64
	}

	Command struct {
		Op       OpType
		Key      string
		Value    string
		Expected string
		ClientID string
		Seq      uint64
	}

	OpType    string
	stateType string
	MsgType   string
)

const (
	FollowerState  stateType = "follower"
	LeaderState    stateType = "leader"
	CandidateState stateType = "candidate"
)

const (
	OpGet OpType = "get"
	OpPut OpType = "put"
	OpDel OpType = "del"
	OpCas OpType = "cas"
)

func NewCore(id int, peers []int, minElectionTicks, maxElectionTicks int, rng *rand.Rand, replicatePeriod int) *Core {
	c := &Core{
		id:               id,
		peers:            peers,
		currentTerm:      0,
		votedFor:         0,
		votesGranted:     map[int]bool{},
		state:            FollowerState,
		log:              make([]Entry, 0),
		commitIndex:      0,
		minElectionTicks: minElectionTicks,
		maxElectionTicks: maxElectionTicks,
		rng:              rng,
		replicatePeriod:  replicatePeriod,
		nextIndex:        map[int]int{},
		matchIndex:       map[int]int{},
	}
	c.resetElectionTimer()
	c.log = append(c.log, Entry{Cmd: Command{}, Term: 0})
	c.persistedTerm = c.currentTerm
	c.persistedVotedFor = c.votedFor
	return c
}

func (c *Core) Restore(term uint64, votedFor int, log []Entry, startIndex int, startTerm uint64, snapshotData any) {
	c.currentTerm = term
	c.votedFor = votedFor
	c.appliedIndex = startIndex
	c.log = log
	c.startIndex = startIndex
	c.startTerm = startTerm
	c.snapshotData = snapshotData
	c.persistedTerm = term
	c.persistedVotedFor = votedFor
	c.persistedIndex = c.lastIndex()
	c.persistedStartIndex = startIndex
}

func (c *Core) Status() Status {
	return Status{
		Id:           c.id,
		Term:         c.currentTerm,
		State:        c.state,
		CommitIndex:  c.commitIndex,
		Log:          append([]Entry(nil), c.log...),
		StartIndex:   c.startIndex,
		AppliedIndex: c.appliedIndex,
		LeaderId:     c.leaderId,
	}
}

func (c *Core) CompactLog(index int, term uint64, data any) {
	if index <= c.startIndex || index > c.lastIndex() {
		return
	}
	keep := append([]Entry(nil), c.log[index-c.startIndex:]...)
	keep[0] = Entry{Term: term}
	c.log = keep
	c.startIndex = index
	c.startTerm = term
	c.snapshotData = data
}

func (c *Core) Step(m Message) {
	switch m.Type {
	case MsgVoteRequest:
		c.handleVoteRequest(m)
	case MsgVoteResponse:
		c.handleVoteResponse(m)
	case MsgAppendRequest:
		c.handleAppendEntriesRequest(m)
	case MsgAppendResponse:
		c.handleAppendEntriesResponse(m)
	case MsgInstallSnapshotRequest:
		c.handleInstallSnapshotRequest(m)
	case MsgInstallSnapshotResponse:
		c.handleInstallSnapshotResponse(m)
	}
}

func (c *Core) Tick() {
	c.electionElapsed++
	c.replicateElapsed++

	// Start election
	if c.state != LeaderState && c.electionElapsed > c.electionTimeout {
		c.leaderId = 0
		c.state = CandidateState
		c.currentTerm++
		c.votesGranted = map[int]bool{c.id: true}
		c.votedFor = c.id
		c.resetElectionTimer()

		for _, peer := range c.peers {
			if peer == c.id {
				continue
			}
			resp := Message{
				FromId:       c.id,
				ToId:         peer,
				Type:         MsgVoteRequest,
				Term:         c.currentTerm,
				LastLogTerm:  c.lastTerm(),
				LastLogIndex: c.lastIndex(),
			}

			c.msgs = append(c.msgs, resp)
		}
	}

	// Start log replication
	if c.state == LeaderState && c.replicateElapsed > c.replicatePeriod {
		c.replicateLog()
		c.replicateElapsed = 0
	}
}

func (c *Core) Ready() ReadyState {
	msgs := c.msgs
	c.msgs = make([]Message, 0)

	ready := ReadyState{Messages: msgs}
	if c.currentTerm != c.persistedTerm || c.votedFor != c.persistedVotedFor {
		ready.StateChanged = true
		ready.Term = c.currentTerm
		ready.VotedFor = c.votedFor
		c.persistedTerm = c.currentTerm
		c.persistedVotedFor = c.votedFor
	}

	if c.truncatedFrom != 0 {
		ready.TruncateFrom = c.truncatedFrom
		if c.truncatedFrom-1 < c.persistedIndex {
			c.persistedIndex = c.truncatedFrom - 1
		}
		c.truncatedFrom = 0
	}

	if c.startIndex > c.persistedStartIndex {
		ready.SnapshotIndex = c.startIndex
		ready.SnapshotTerm = c.startTerm
		ready.SnapshotData = c.snapshotData
		c.persistedStartIndex = c.startIndex
		if c.persistedIndex < c.startIndex {
			c.persistedIndex = c.startIndex
		}
	}
	if last := c.lastIndex(); last > c.persistedIndex {
		lo := c.persistedIndex + 1 - c.startIndex
		hi := last + 1 - c.startIndex
		ready.EntriesToPersist = append([]Entry(nil), c.log[lo:hi]...)
		c.persistedIndex = last
	}

	if c.appliedIndex < c.startIndex {
		c.appliedIndex = c.startIndex
	}

	if c.commitIndex > c.appliedIndex {
		ready.ApplyFrom = c.appliedIndex + 1
		lo := c.appliedIndex + 1 - c.startIndex
		hi := c.commitIndex + 1 - c.startIndex
		ready.EntriesToApply = append([]Entry(nil), c.log[lo:hi]...)
		c.appliedIndex = c.commitIndex
	}

	return ready
}

type ReadyState struct {
	Messages         []Message
	StateChanged     bool
	Term             uint64
	VotedFor         int
	EntriesToPersist []Entry
	EntriesToApply   []Entry
	TruncateFrom     int
	ApplyFrom        int

	// SnapshotIndex is nonzero exactly when a snapshot boundary has advanced
	// (via CompactLog or a received InstallSnapshot) since the last Ready()
	// call and hasn't been reported yet. When set, the driver must persist
	// SnapshotData as the new snapshot before it can safely reclaim any log
	// storage below SnapshotIndex.
	SnapshotIndex int
	SnapshotTerm  uint64
	SnapshotData  any
}

func (c *Core) handleVoteRequest(m Message) {
	resp := Message{
		FromId: c.id,
		ToId:   m.FromId,
		Type:   MsgVoteResponse,
		Term:   c.currentTerm,
	}

	if m.Term < c.currentTerm {
		resp.Success = false
		c.msgs = append(c.msgs, resp)
		return
	}

	if c.currentTerm < m.Term {
		resp.Term = m.Term
		c.becomeFollower(m.Term)
	}

	if c.votedFor == 0 || c.votedFor == m.FromId {
		TermCond := c.lastTerm() < m.LastLogTerm
		indexCond := c.lastTerm() == m.LastLogTerm && c.lastIndex() <= m.LastLogIndex
		if TermCond || indexCond {
			c.votedFor = m.FromId
			resp.Success = true
		}
	}
	c.msgs = append(c.msgs, resp)
}

func (c *Core) handleVoteResponse(m Message) {
	if m.Term > c.currentTerm {
		c.becomeFollower(m.Term)
		return
	}
	if !m.Success || m.Term != c.currentTerm {
		return
	}

	c.votesGranted[m.FromId] = true
	if len(c.votesGranted)*2 > len(c.peers) && c.state == CandidateState {
		c.state = LeaderState
		c.leaderId = c.id
		for _, peer := range c.peers {
			c.nextIndex[peer] = c.lastIndex() + 1
			c.matchIndex[peer] = 0
		}
		c.log = append(c.log, Entry{Term: c.currentTerm})
	}
}

func (c *Core) handleAppendEntriesRequest(m Message) {
	resp := Message{
		FromId: c.id,
		ToId:   m.FromId,
		Type:   MsgAppendResponse,
		Term:   c.currentTerm,
	}

	if m.Term > c.currentTerm {
		resp.Term = m.Term
		c.becomeFollower(m.Term)
	}
	if m.Term == c.currentTerm && c.state == CandidateState {
		c.state = FollowerState
		c.resetElectionTimer()
	}

	if m.Term < c.currentTerm {
		resp.Success = false
		c.msgs = append(c.msgs, resp)
		return
	}
	c.leaderId = m.FromId

	if m.LastLogIndex < c.startIndex {
		c.resetElectionTimer()
		resp.Success = false
		resp.LastLogIndex = c.startIndex
		c.msgs = append(c.msgs, resp)
		return
	}
	if c.lastIndex() < m.LastLogIndex {
		c.resetElectionTimer()
		resp.Success = false
		c.msgs = append(c.msgs, resp)
		return
	}

	if c.lastIndex() >= m.LastLogIndex && c.log[m.LastLogIndex-c.startIndex].Term != m.LastLogTerm {
		c.resetElectionTimer()
		resp.Success = false
		c.msgs = append(c.msgs, resp)
		return
	}

	startingPoint := 0
	for i := 0; i < len(m.Entries); i++ {
		entry := m.Entries[i]
		index := m.LastLogIndex + 1 + i
		if index-c.startIndex >= len(c.log) {
			startingPoint = i
			break
		}
		if c.log[index-c.startIndex].Term != entry.Term {
			if c.truncatedFrom == 0 || index < c.truncatedFrom {
				c.truncatedFrom = index
			}
			c.log = c.log[:index-c.startIndex]
			startingPoint = i
			break
		}
		startingPoint = i + 1
	}

	for ; startingPoint < len(m.Entries); startingPoint++ {
		c.log = append(c.log, m.Entries[startingPoint])
	}
	resp.Success = true
	resp.LastLogIndex = m.LastLogIndex + len(m.Entries)
	if m.CommitIndex > c.commitIndex {
		c.commitIndex = min(m.CommitIndex, c.lastIndex())
	}
	c.resetElectionTimer()
	c.msgs = append(c.msgs, resp)
}

func (c *Core) handleAppendEntriesResponse(m Message) {
	if m.Term > c.currentTerm {
		c.becomeFollower(m.Term)
		return
	}

	if c.state != LeaderState || m.Term != c.currentTerm {
		return
	}

	if !m.Success {
		c.nextIndex[m.FromId] = max(c.nextIndex[m.FromId]-1, 1)
		return
	}
	c.nextIndex[m.FromId] = m.LastLogIndex + 1
	c.matchIndex[m.FromId] = m.LastLogIndex
	index := m.LastLogIndex

	if index <= c.commitIndex {
		return
	}
	if c.log[index-c.startIndex].Term != c.currentTerm {
		return
	}

	counter := 1
	for _, peer := range c.peers {
		if peer == c.id {
			continue
		}
		if c.matchIndex[peer] >= index {
			counter++
		}
	}
	if counter*2 > len(c.peers) {
		c.commitIndex = index
	}
}

func (c *Core) Propose(cmds []Command) (index int, term uint64, ok bool) {
	if c.state != LeaderState {
		return 0, 0, false
	}
	if len(cmds) == 0 {
		return 0, 0, false
	}
	for _, cmd := range cmds {
		c.log = append(c.log, Entry{Cmd: cmd, Term: c.currentTerm})
	}
	c.resetElectionTimer()
	c.replicateLog()
	return c.lastIndex() - len(cmds) + 1, c.currentTerm, true
}

func (c *Core) replicateLog() {

	for _, peer := range c.peers {
		if peer == c.id {
			continue
		}
		if c.state != LeaderState {
			return
		}
		if c.nextIndex[peer]-1 < c.startIndex {
			c.sendInstallSnapshot(peer)
			continue
		}
		log := c.log[c.nextIndex[peer]-c.startIndex:]
		prevLogEntry := c.log[c.nextIndex[peer]-1-c.startIndex]
		message := Message{
			Term:         c.currentTerm,
			Type:         MsgAppendRequest,
			FromId:       c.id,
			ToId:         peer,
			LastLogTerm:  prevLogEntry.Term,
			LastLogIndex: c.nextIndex[peer] - 1,
			Entries:      log,
			CommitIndex:  c.commitIndex,
		}
		c.msgs = append(c.msgs, message)
	}
}

func (c *Core) sendInstallSnapshot(peer int) {
	c.msgs = append(c.msgs, Message{
		Term:          c.currentTerm,
		Type:          MsgInstallSnapshotRequest,
		FromId:        c.id,
		ToId:          peer,
		SnapshotIndex: c.startIndex,
		SnapshotTerm:  c.startTerm,
		SnapshotData:  c.snapshotData,
	})
}

func (c *Core) handleInstallSnapshotRequest(m Message) {
	resp := Message{
		FromId: c.id,
		ToId:   m.FromId,
		Type:   MsgInstallSnapshotResponse,
		Term:   c.currentTerm,
	}

	if m.Term < c.currentTerm {
		c.msgs = append(c.msgs, resp)
		return
	}
	if m.Term > c.currentTerm {
		resp.Term = m.Term
		c.becomeFollower(m.Term)
	}
	c.leaderId = m.FromId
	if c.state == CandidateState {
		c.state = FollowerState
	}
	c.resetElectionTimer()

	if m.SnapshotIndex > c.startIndex {
		if m.SnapshotIndex <= c.lastIndex() && c.log[m.SnapshotIndex-c.startIndex].Term == m.SnapshotTerm {

			c.log = append([]Entry(nil), c.log[m.SnapshotIndex-c.startIndex:]...)
		} else {
			c.log = []Entry{{Term: m.SnapshotTerm}}
		}
		c.startIndex = m.SnapshotIndex
		c.startTerm = m.SnapshotTerm
		c.snapshotData = m.SnapshotData
		if m.SnapshotIndex > c.commitIndex {
			c.commitIndex = m.SnapshotIndex
		}
	}

	// Acknowledge the snapshot point, not this node's own last index. The
	// branch above verifies only the entry *at* SnapshotIndex; anything this
	// follower kept beyond it is an unverified tail from an earlier term that
	// the leader may not have at all. Reporting it would let the leader set
	// nextIndex past the end of its own log (a panic in replicateLog) and,
	// worse, count this node's fictional entries toward a commit quorum. Any
	// tail that really does match gets re-confirmed by the normal
	// AppendEntries consistency check, which is what that check is for.
	//
	// This matches the AppendEntries response, where LastLogIndex likewise
	// means "the last index I have confirmed matches yours".
	resp.LastLogIndex = m.SnapshotIndex
	c.msgs = append(c.msgs, resp)
}

func (c *Core) handleInstallSnapshotResponse(m Message) {
	if m.Term > c.currentTerm {
		c.becomeFollower(m.Term)
		return
	}
	if c.state != LeaderState || m.Term != c.currentTerm {
		return
	}
	c.nextIndex[m.FromId] = m.LastLogIndex + 1
	c.matchIndex[m.FromId] = m.LastLogIndex
}

func (c *Core) becomeFollower(newTerm uint64) {
	c.state = FollowerState
	c.currentTerm = newTerm
	c.votedFor = 0
	c.votesGranted = make(map[int]bool)
	c.leaderId = 0
	c.resetElectionTimer()
}

func (c *Core) lastTerm() uint64 {
	return c.log[len(c.log)-1].Term
}

func (c *Core) lastIndex() int {
	return c.startIndex + len(c.log) - 1
}

func (c *Core) resetElectionTimer() {
	c.electionElapsed = 0
	c.electionTimeout = c.minElectionTicks + c.rng.Intn(c.maxElectionTicks-c.minElectionTicks+1)
}
