package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// NodeState 节点状态枚举：Raft节点有三种状态
type NodeState int

const (
	Follower  NodeState = iota // 跟随者：接收Leader的心跳和日志复制
	Candidate                  // 候选者：选举超时后转为候选者，发起投票
	Leader                     // 领导者：获得多数派投票后成为领导者
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// MsgType 消息类型枚举：Raft协议使用的RPC消息
type MsgType int

const (
	RequestVote    MsgType = iota // 请求投票：Candidate向其他节点发起投票请求
	VoteResponse                  // 投票响应：其他节点对投票请求的回复
	AppendEntries                 // 追加条目：Leader向Follower复制日志/发送心跳
	AppendResponse                // 追加响应：Follower对日志复制的确认
	ClientRequest                 // 客户端请求：客户端向Leader提交命令
)

// LogEntry 日志条目：每个条目包含任期号、索引和命令
type LogEntry struct {
	Term    int    // 条目被Leader接收时的任期号
	Index   int    // 条目在日志中的索引位置
	Command string // 客户端提交的命令
}

// Message RPC消息结构：用于节点间通信
type Message struct {
	Type         MsgType    // 消息类型
	FromID       int        // 发送节点ID
	ToID         int        // 接收节点ID
	Term         int        // 发送节点的当前任期号
	CandidateID  int        // Candidate的ID（仅用于RequestVote）
	LastLogIndex int        // 候选者日志的最后索引（用于RequestVote）
	LastLogTerm  int        // 候选者日志最后条目的任期号（用于RequestVote）
	Entries      []LogEntry // 要复制的日志条目（用于AppendEntries）
	LeaderCommit int        // Leader的commitIndex（用于AppendEntries）
	Success      bool       // 请求是否成功（用于响应消息）
	Command      string     // 客户端命令（仅用于ClientRequest）
}

var nodeColors = []string{"\033[32m", "\033[31m", "\033[33m", "\033[34m", "\033[35m", "\033[36m"}

// Node Raft节点结构体：包含持久化状态、易失性状态和通信通道
type Node struct {
	mu             sync.Mutex    // 互斥锁：保护并发访问
	ID             int           // 节点唯一标识
	state          NodeState     // 当前状态：Follower/Candidate/Leader
	currentTerm    int           // 当前任期号：每次选举递增
	votedFor       int           // 当前任期投票给的Candidate ID，-1表示未投票
	voteCount      int           // 候选者获得的票数
	log            []LogEntry    // 日志条目：索引从1开始，索引0为哨兵条目
	commitIndex    int           // 最高已知已提交的日志索引
	lastApplied    int           // 最高已应用到状态机的日志索引
	nextIndex      map[int]int   // 每个Follower的nextIndex：下一个要发送的日志索引
	matchIndex     map[int]int   // 每个Follower的matchIndex：已知复制成功的最高索引
	msgChan        chan Message  // 接收消息的通道
	doneChan       chan struct{} // 终止信号通道
	nodes          []*Node       // 集群中所有节点的引用
	electionTimer  *time.Timer   // 选举超时计时器
	heartbeatTimer *time.Timer   // 心跳计时器（仅Leader使用）
	color          string        // 节点颜色
}

func NewNode(id int, nodes []*Node) *Node {
	node := &Node{
		ID:          id,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         []LogEntry{{Term: 0, Index: 0, Command: ""}},
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make(map[int]int),
		matchIndex:  make(map[int]int),
		msgChan:     make(chan Message, 100),
		doneChan:    make(chan struct{}),
		nodes:       nodes,
		color:       nodeColors[(id-1)%len(nodeColors)],
	}
	return node
}

// resetElectionTimer 重置选举超时计时器
// 设置随机超时时间（150-300ms），超时后如果仍为Follower则转为Candidate发起选举
// 随机超时是Raft防止选举分裂的关键机制
func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	// 随机超时：3000-6000ms，大幅放慢选举速度以便观察候选者之间的竞争
	timeout := time.Duration(3000+rand.Intn(3000)) * time.Millisecond
	n.electionTimer = time.AfterFunc(timeout, func() {
		// 选举超时后会检查当前状态是否为Follower，如果是则转为Candidate发起选举
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.state == Follower {
			fmt.Printf("%s[Node %d] 选举超时, 转为Candidate, Term=%d\033[0m\n", n.color, n.ID, n.currentTerm+1)
			n.startElection()
		}
	})
}

// startElection 发起选举：Candidate向所有节点发送投票请求
// 步骤：1. 转为Candidate状态 2. 增加任期号 3. 投自己一票 4. 向所有Peer发送RequestVote
func (n *Node) startElection() {
	n.state = Candidate // 转为候选者状态
	n.currentTerm++     // 增加任期号
	n.votedFor = n.ID   // 投自己一票
	n.voteCount = 1     // 初始票数为1

	fmt.Printf("%s[Node %d] 开始选举, Term=%d, state=%s\033[0m\n", n.color, n.ID, n.currentTerm, n.state)

	for _, peer := range n.nodes {
		if peer.ID == n.ID {
			continue
		}
		// 获取自己日志的最后条目信息，用于日志完整性检查
		lastLogIdx := len(n.log) - 1
		lastLogTerm := n.log[lastLogIdx].Term
		msg := Message{
			Type:         RequestVote,
			FromID:       n.ID,
			ToID:         peer.ID,
			Term:         n.currentTerm,
			CandidateID:  n.ID,
			LastLogIndex: lastLogIdx,  // 自己日志的最后索引
			LastLogTerm:  lastLogTerm, // 自己日志最后条目的任期号
		}
		fmt.Printf("%s[Node %d] 发送RequestVote(Term=%d) 到 Node %d\033[0m\n", n.color, n.ID, n.currentTerm, peer.ID)
		go func(p *Node, m Message) {
			time.Sleep(time.Duration(300+rand.Intn(500)) * time.Millisecond)
			p.msgChan <- m
		}(peer, msg)
	}
}

// becomeLeader 成为Leader：初始化日志复制状态并开始发送心跳
// nextIndex初始化为Leader日志长度，matchIndex初始化为0
func (n *Node) becomeLeader() {
	n.state = Leader
	fmt.Printf("\n=== \033[32m[Node %d] 成为Leader, Term=%d\033[0m ===\n", n.ID, n.currentTerm)

	// 初始化每个Follower的日志复制状态
	for _, peer := range n.nodes {
		if peer.ID == n.ID {
			continue
		}
		n.nextIndex[peer.ID] = len(n.log) // 下一个要发送的日志索引
		n.matchIndex[peer.ID] = 0         // 已知复制成功的最高索引
	}

	// 立即发送心跳，防止其他节点超时发起选举
	n.sendHeartbeat()
}

// sendHeartbeat 发送心跳/日志复制消息：Leader每100ms向所有Follower发送
// 心跳的作用：1. 维持Leader地位 2. 复制日志条目 3. 通知Follower已提交的索引
func (n *Node) sendHeartbeat() {
	if n.state != Leader {
		return
	}

	for _, peer := range n.nodes {
		if peer.ID == n.ID {
			continue
		}
		// prevLogIdx：发送给该Follower的日志条目的前一个索引
		prevLogIdx := n.nextIndex[peer.ID] - 1
		var prevLogTerm int
		if prevLogIdx >= 0 && prevLogIdx < len(n.log) {
			prevLogTerm = n.log[prevLogIdx].Term
		}

		// 获取需要发送的日志条目
		var entries []LogEntry
		if n.nextIndex[peer.ID] < len(n.log) {
			entries = n.log[n.nextIndex[peer.ID]:]
		}

		msg := Message{
			Type:         AppendEntries,
			FromID:       n.ID,
			ToID:         peer.ID,
			Term:         n.currentTerm,
			LastLogIndex: prevLogIdx,    // 前一条日志的索引
			LastLogTerm:  prevLogTerm,   // 前一条日志的任期号
			Entries:      entries,       // 要复制的日志条目（心跳时为空）
			LeaderCommit: n.commitIndex, // Leader已提交的最高索引
		}
		peer.msgChan <- msg
	}

	// 500ms后再次发送心跳，放慢心跳频率以便观察选举过程
	n.heartbeatTimer = time.AfterFunc(500*time.Millisecond, func() {
		n.mu.Lock()
		defer n.mu.Unlock()
		n.sendHeartbeat()
	})
}

// handleRequestVote 处理投票请求：决定是否投票给Candidate
// 投票规则：1. 请求任期必须 >= 当前任期 2. 当前任期未投票或已投给该Candidate
// 3. Candidate的日志必须至少和自己的一样新（日志完整性检查）
func (n *Node) handleRequestVote(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 规则1：如果请求任期小于当前任期，拒绝投票
	if msg.Term < n.currentTerm {
		response := Message{
			Type:    VoteResponse,
			FromID:  n.ID,
			ToID:    msg.CandidateID,
			Term:    n.currentTerm,
			Success: false,
		}
		fmt.Printf("%s[Node %d] 拒绝投票(Term=%d < %d)\033[0m\n", n.color, n.ID, msg.Term, n.currentTerm)
		n.nodes[msg.CandidateID-1].msgChan <- response
		return
	}

	// 规则2：如果请求任期大于当前任期，更新任期并转为Follower
	if msg.Term > n.currentTerm {
		n.currentTerm = msg.Term
		n.state = Follower
		n.votedFor = -1
	}

	// 日志完整性检查：Candidate的日志是否至少和自己一样新
	// 比较规则：最后条目的任期号更大，或任期号相同但索引更大
	lastLogIdx := len(n.log) - 1
	lastLogTerm := n.log[lastLogIdx].Term
	upToDate := msg.LastLogTerm > lastLogTerm ||
		(msg.LastLogTerm == lastLogTerm && msg.LastLogIndex >= lastLogIdx)

	// 规则3：满足条件则投票
	if (n.votedFor == -1 || n.votedFor == msg.CandidateID) && upToDate {
		n.votedFor = msg.CandidateID
		n.resetElectionTimer() // 收到投票请求也重置选举计时器
		response := Message{
			Type:    VoteResponse,
			FromID:  n.ID,
			ToID:    msg.CandidateID,
			Term:    n.currentTerm,
			Success: true,
		}
		fmt.Printf("%s[Node %d] 投票给 Candidate %d, Term=%d\033[0m\n", n.color, n.ID, msg.CandidateID, n.currentTerm)
		n.nodes[msg.CandidateID-1].msgChan <- response
	} else {
		response := Message{
			Type:    VoteResponse,
			FromID:  n.ID,
			ToID:    msg.CandidateID,
			Term:    n.currentTerm,
			Success: false,
		}
		fmt.Printf("%s[Node %d] 拒绝投票(已投:%d, upToDate:%v)\033[0m\n", n.color, n.ID, n.votedFor, upToDate)
		n.nodes[msg.CandidateID-1].msgChan <- response
	}
}

// handleVoteResponse 处理投票响应：Candidate统计收到的票数
// 如果收到的票数达到多数派，则成为Leader
func (n *Node) handleVoteResponse(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 如果响应任期大于当前任期，说明存在更高任期的Leader，转为Follower
	if msg.Term > n.currentTerm {
		n.currentTerm = msg.Term
		n.state = Follower
		n.votedFor = -1
		n.resetElectionTimer()
		return
	}

	// 如果是当前任期的Candidate且收到肯定投票
	if n.state == Candidate && msg.Term == n.currentTerm && msg.Success {
		n.voteCount++
		fmt.Printf("%s[Node %d] 收到投票, voteCount=%d\033[0m\n", n.color, n.ID, n.voteCount)
		// 获得多数派投票（(N+1)/2）则成为Leader
		if n.voteCount >= (len(n.nodes)+1)/2 {
			n.becomeLeader()
		}
	}
}

// handleAppendEntries 处理追加条目消息：Follower接收Leader的日志复制/心跳
// 步骤：1. 任期检查 2. 日志一致性检查 3. 追加日志条目 4. 更新commitIndex 5. 应用到状态机
func (n *Node) handleAppendEntries(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 规则1：如果消息任期小于当前任期，拒绝
	if msg.Term < n.currentTerm {
		response := Message{
			Type:    AppendResponse,
			FromID:  n.ID,
			ToID:    msg.FromID,
			Term:    n.currentTerm,
			Success: false,
		}
		n.nodes[msg.FromID-1].msgChan <- response
		return
	}

	// 规则2：如果消息任期大于当前任期，更新任期并转为Follower
	if msg.Term > n.currentTerm {
		n.currentTerm = msg.Term
		n.state = Follower
		n.votedFor = -1
	}

	// 规则3：如果是Candidate且收到同任期的AppendEntries，转为Follower（发现Leader）
	if n.state == Candidate && msg.Term == n.currentTerm {
		n.state = Follower
		n.votedFor = -1
	}

	// 重置选举计时器：收到Leader消息，说明Leader存活
	n.resetElectionTimer()

	// 日志一致性检查：检查前一条日志是否匹配
	// 如果前一条日志不存在或任期号不匹配，则拒绝
	if msg.LastLogIndex >= len(n.log) || n.log[msg.LastLogIndex].Term != msg.LastLogTerm {
		response := Message{
			Type:    AppendResponse,
			FromID:  n.ID,
			ToID:    msg.FromID,
			Term:    n.currentTerm,
			Success: false,
		}
		n.nodes[msg.FromID-1].msgChan <- response
		return
	}

	// 追加日志条目：跳过已有的，追加新的
	for i, entry := range msg.Entries {
		if entry.Index >= len(n.log) || n.log[entry.Index].Term != entry.Term {
			n.log = n.log[:entry.Index]               // 删除冲突的条目
			n.log = append(n.log, msg.Entries[i:]...) // 追加新条目
			break
		}
	}

	// 更新commitIndex：如果Leader已提交的索引大于自己的，则更新
	if msg.LeaderCommit > n.commitIndex {
		n.commitIndex = min(msg.LeaderCommit, len(n.log)-1)
		// 将已提交但未应用的日志应用到状态机
		for n.lastApplied < n.commitIndex {
			n.lastApplied++
			fmt.Printf("%s[Node %d] 应用日志 Entry[%d]='%s', Term=%d\033[0m\n",
				n.color, n.ID, n.lastApplied, n.log[n.lastApplied].Command, n.log[n.lastApplied].Term)
		}
	}

	response := Message{
		Type:    AppendResponse,
		FromID:  n.ID,
		ToID:    msg.FromID,
		Term:    n.currentTerm,
		Success: true,
	}
	n.nodes[msg.FromID-1].msgChan <- response
}

// handleAppendResponse 处理追加响应：Leader更新日志复制状态并决定是否提交
// 成功：更新nextIndex和matchIndex，检查是否可以提交
// 失败：回退nextIndex（日志不一致，需要重新发送更早的日志）
func (n *Node) handleAppendResponse(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 如果不再是Leader，忽略响应
	if n.state != Leader {
		return
	}

	if msg.Success {
		// 更新该Follower的复制状态
		n.nextIndex[msg.FromID] = len(n.log)
		n.matchIndex[msg.FromID] = len(n.log) - 1

		// 检查是否有新的日志可以提交（当前任期的日志获得多数派确认）
		for i := n.commitIndex + 1; i < len(n.log); i++ {
			if n.log[i].Term == n.currentTerm {
				count := 1 // 自己算一票
				for _, peer := range n.nodes {
					if peer.ID == n.ID {
						continue
					}
					if n.matchIndex[peer.ID] >= i {
						count++
					}
				}
				// 获得多数派确认则提交
				if count >= (len(n.nodes)+1)/2 {
					n.commitIndex = i
					fmt.Printf("\033[32m[Node %d] Leader提交日志 Index=%d, Value='%s'\033[0m\n",
						n.ID, i, n.log[i].Command)
				}
			}
		}
	} else {
		// 日志不一致，回退nextIndex，下次发送更早的日志
		if n.nextIndex[msg.FromID] > 1 {
			n.nextIndex[msg.FromID]--
		}
	}
}

// handleClientRequest 处理客户端请求：只有Leader可以接收客户端命令
// 将命令封装为日志条目添加到本地日志，等待通过心跳复制到Follower
func (n *Node) handleClientRequest(msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 只有Leader可以处理客户端请求
	if n.state != Leader {
		fmt.Printf("%s[Node %d] 非Leader, 拒绝客户端请求\033[0m\n", n.color, n.ID)
		return
	}

	// 创建日志条目并添加到本地日志
	entry := LogEntry{
		Term:    n.currentTerm, // 当前任期号
		Index:   len(n.log),    // 新条目的索引
		Command: msg.Command,   // 客户端命令
	}
	n.log = append(n.log, entry)
	fmt.Printf("\033[32m[Node %d] Leader接收客户端请求: '%s', Entry[%d]\033[0m\n", n.ID, msg.Command, entry.Index)
}

// Run 节点主循环：持续接收并处理消息
// 启动时重置选举计时器，然后进入消息处理循环
func (n *Node) Run() {
	n.resetElectionTimer() // 启动选举超时计时器

	for {
		select {
		case msg := <-n.msgChan: // 接收消息
			switch msg.Type {
			case RequestVote:
				n.handleRequestVote(msg)
			case VoteResponse:
				n.handleVoteResponse(msg)
			case AppendEntries:
				n.handleAppendEntries(msg)
			case AppendResponse:
				n.handleAppendResponse(msg)
			case ClientRequest:
				n.handleClientRequest(msg)
			}
		case <-n.doneChan: // 收到终止信号
			return
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// main 主函数：创建Raft集群并演示选举和日志复制流程
func main() {
	nodeCount := 3 // 集群节点数：3个节点（需要2个多数派）
	nodes := make([]*Node, nodeCount)

	// 创建节点
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewNode(i+1, nodes)
	}

	// 设置每个节点的集群引用
	for i := 0; i < nodeCount; i++ {
		nodes[i].nodes = nodes
	}

	// 启动所有节点
	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go func(n *Node) {
			defer wg.Done()
			n.Run()
		}(node)
	}

	// 模拟客户端请求
	go func() {
		time.Sleep(8 * time.Second) // 等待选举完成（延长时间）

		// 查找Leader
		var leader *Node
		for _, n := range nodes {
			n.mu.Lock()
			if n.state == Leader {
				leader = n
			}
			n.mu.Unlock()
		}

		// 向Leader发送客户端请求
		if leader != nil {
			fmt.Printf("\n========== 客户端请求 ==========\n")
			// 第一个请求
			leader.msgChan <- Message{
				Type:    ClientRequest,
				FromID:  0,
				ToID:    leader.ID,
				Command: "Set key=value",
			}

			time.Sleep(500 * time.Millisecond)

			// 第二个请求
			leader.msgChan <- Message{
				Type:    ClientRequest,
				FromID:  0,
				ToID:    leader.ID,
				Command: "Update status=running",
			}

			time.Sleep(1 * time.Second)
		}

		time.Sleep(2 * time.Second)

		// 停止所有节点
		for _, n := range nodes {
			if n.electionTimer != nil {
				n.electionTimer.Stop()
			}
			if n.heartbeatTimer != nil {
				n.heartbeatTimer.Stop()
			}
			close(n.doneChan)
		}
	}()

	wg.Wait()

	// 打印最终状态
	fmt.Printf("\n========== 最终状态 ==========\n")
	for _, n := range nodes {
		n.mu.Lock()
		fmt.Printf("%s[Node %d] State=%s, Term=%d, CommitIndex=%d, LogLen=%d\033[0m\n",
			n.color, n.ID, n.state, n.currentTerm, n.commitIndex, len(n.log)-1)
		for i := 1; i < len(n.log); i++ {
			fmt.Printf("  Log[%d] = '%s' (Term=%d)\n", i, n.log[i].Command, n.log[i].Term)
		}
		n.mu.Unlock()
	}
	fmt.Printf("===============================\n")
}
