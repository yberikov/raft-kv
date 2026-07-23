package raft

import "math/rand"

type Core struct {
	id          int
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

	// init values after recovery
	startIndex int
	startTerm  uint64

	msgs []Message
}

type (
	Entry struct {
		Cmd  any
		Term uint64
	}

	stateType string
	MsgType   string
)

var (
	FollowerState  stateType = "follower"
	LeaderState    stateType = "leader"
	CandidateState stateType = "candidate"
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
	c.log = append(c.log, Entry{Cmd: nil, Term: 0})
	return c
}

func (c *Core) Status() Status {
	return Status{
		Id:          c.id,
		Term:        c.currentTerm,
		State:       c.state,
		CommitIndex: c.commitIndex,
		Log:         append([]Entry(nil), c.log...),
	}
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
	case MsgProposeRequest:
		c.handleProposeRequest(m)
	}
}

func (c *Core) Tick() {
	c.electionElapsed++
	c.replicateElapsed++

	// Start election
	if c.state != LeaderState && c.electionElapsed > c.electionTimeout {
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

func (c *Core) Ready() []Message {
	msgs := c.msgs
	c.msgs = make([]Message, 0)
	return msgs

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
		for _, peer := range c.peers {
			c.nextIndex[peer] = c.lastIndex() + 1
			c.matchIndex[peer] = 0
		}
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
	if c.lastIndex() < m.LastLogIndex {
		c.resetElectionTimer()
		resp.Success = false
		c.msgs = append(c.msgs, resp)
		return
	}

	if c.lastIndex() >= m.LastLogIndex && c.log[m.LastLogIndex].Term != m.LastLogTerm {
		c.resetElectionTimer()
		resp.Success = false
		c.msgs = append(c.msgs, resp)
		return
	}

	startingPoint := 0
	for i := 0; i < len(m.Entries); i++ {
		entry := m.Entries[i]
		index := m.LastLogIndex + 1 + i
		if index >= len(c.log) {
			startingPoint = i
			break
		}
		if c.log[index].Term != entry.Term {
			c.log = c.log[:index]
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

	if m.Term != c.currentTerm {
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
	if c.log[index].Term != c.currentTerm {
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

func (c *Core) handleProposeRequest(msg Message) {
	if c.state != LeaderState {
		return
	}
	for _, cmd := range msg.ProposeCmd {
		c.log = append(c.log, Entry{Cmd: cmd, Term: c.currentTerm})
	}
}

func (c *Core) replicateLog() {

	for _, peer := range c.peers {
		if peer == c.id {
			continue
		}
		if c.state != LeaderState {
			return
		}
		log := c.log[c.nextIndex[peer]:]
		prevLogEntry := c.log[c.nextIndex[peer]-1]
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

func (c *Core) becomeFollower(newTerm uint64) {
	c.state = FollowerState
	c.currentTerm = newTerm
	c.votedFor = 0
	c.votesGranted = make(map[int]bool)
	c.resetElectionTimer()
}

func (c *Core) lastTerm() uint64 {
	if len(c.log) == 0 {
		return c.startTerm
	}
	return c.log[len(c.log)-1].Term
}

func (c *Core) lastIndex() int {
	if len(c.log) == 0 {
		return 0
	}
	return len(c.log) - 1
}

func (c *Core) resetElectionTimer() {
	c.electionElapsed = 0
	c.electionTimeout = c.minElectionTicks + c.rng.Intn(c.maxElectionTicks-c.minElectionTicks+1)
}
