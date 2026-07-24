package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ParticipantState int

const (
	Idle ParticipantState = iota
	Prepared
	Committed
	Aborted
)

func (s ParticipantState) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Prepared:
		return "Prepared"
	case Committed:
		return "Committed"
	case Aborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

type MsgType int

const (
	Prepare MsgType = iota
	VoteYes
	VoteNo
	Commit
	Abort
)

func (m MsgType) String() string {
	switch m {
	case Prepare:
		return "Prepare"
	case VoteYes:
		return "VoteYes"
	case VoteNo:
		return "VoteNo"
	case Commit:
		return "Commit"
	case Abort:
		return "Abort"
	default:
		return "Unknown"
	}
}

type Message struct {
	Type          MsgType
	FromID        int
	ToID          int
	TransactionID string
}

type Participant struct {
	mu         sync.Mutex
	ID         int
	state      ParticipantState
	msgChan    chan Message
	doneChan   chan struct{}
	canCommit  bool
	txnData    string
	persistent bool
}

func NewParticipant(id int, canCommit bool) *Participant {
	return &Participant{
		ID:         id,
		state:      Idle,
		msgChan:    make(chan Message, 10),
		doneChan:   make(chan struct{}),
		canCommit:  canCommit,
		persistent: false,
	}
}

func (p *Participant) Run() {
	for {
		select {
		case msg := <-p.msgChan:
			p.handleMessage(msg)
		case <-p.doneChan:
			return
		}
	}
}

func (p *Participant) handleMessage(msg Message) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fmt.Printf("[Participant %d] 收到消息: %s, 当前状态: %s\n", p.ID, msg.Type, p.state)

	switch msg.Type {
	case Prepare:
		time.Sleep(time.Duration(500+rand.Intn(500)) * time.Millisecond)
		if p.canCommit {
			p.state = Prepared
			p.persistent = true
			fmt.Printf("[Participant %d] 状态变更: Idle -> Prepared (持久化成功)\n", p.ID)
			p.sendVote(msg.TransactionID, VoteYes)
		} else {
			p.state = Aborted
			fmt.Printf("[Participant %d] 状态变更: Idle -> Aborted (无法提交)\n", p.ID)
			p.sendVote(msg.TransactionID, VoteNo)
		}

	case Commit:
		if p.state == Prepared {
			p.state = Committed
			p.txnData = "已提交的数据"
			fmt.Printf("[Participant %d] 状态变更: Prepared -> Committed\n", p.ID)
		}

	case Abort:
		if p.state == Prepared {
			p.state = Aborted
			fmt.Printf("[Participant %d] 状态变更: Prepared -> Aborted\n", p.ID)
		}
	}
}

func (p *Participant) sendVote(txnID string, vote MsgType) {
	fmt.Printf("[Participant %d] 发送投票: %s\n", p.ID, vote)
	coordinator.msgChan <- Message{
		Type:          vote,
		FromID:        p.ID,
		ToID:          0,
		TransactionID: txnID,
	}
}

type Coordinator struct {
	mu           sync.Mutex
	msgChan      chan Message
	doneChan     chan struct{}
	participants []*Participant
	voteCount    int
	voteYesCount int
	txnID        string
	phase        int
}

var coordinator *Coordinator

func NewCoordinator(participants []*Participant) *Coordinator {
	return &Coordinator{
		msgChan:      make(chan Message, 10),
		doneChan:     make(chan struct{}),
		participants: participants,
	}
}

func (c *Coordinator) Run() {
	for {
		select {
		case msg := <-c.msgChan:
			c.handleMessage(msg)
		case <-c.doneChan:
			return
		}
	}
}

func (c *Coordinator) handleMessage(msg Message) {
	c.mu.Lock()

	fmt.Printf("[Coordinator] 收到消息: %s 来自 Participant %d\n", msg.Type, msg.FromID)

	switch msg.Type {
	case VoteYes:
		c.voteYesCount++
		c.voteCount++
		fmt.Printf("[Coordinator] 累计投票: Yes=%d, Total=%d\n", c.voteYesCount, c.voteCount)

	case VoteNo:
		c.voteCount++
		fmt.Printf("[Coordinator] 累计投票: Yes=%d, Total=%d (收到No)\n", c.voteYesCount, c.voteCount)
	}

	allVoted := c.voteCount == len(c.participants)
	allYes := c.voteYesCount == len(c.participants)
	hasNo := !allYes && allVoted || msg.Type == VoteNo

	c.mu.Unlock()

	if allVoted {
		if allYes {
			c.phase2Commit()
		} else {
			c.phase2Abort()
		}
	} else if hasNo {
		c.phase2Abort()
	}
}

func (c *Coordinator) phase1Prepare(txnID string) {
	c.mu.Lock()
	c.txnID = txnID
	c.phase = 1
	c.voteCount = 0
	c.voteYesCount = 0
	c.mu.Unlock()

	fmt.Println("\n========== 第一阶段: Prepare 阶段 ==========")
	fmt.Printf("[Coordinator] 发起事务 %s，向所有参与者发送 Prepare\n", txnID)

	for _, p := range c.participants {
		go func(participant *Participant) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message{
				Type:          Prepare,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: txnID,
			}
		}(p)
	}
}

func (c *Coordinator) checkVotes() {
	if c.voteCount == len(c.participants) {
		if c.voteYesCount == len(c.participants) {
			c.phase2Commit()
		} else {
			c.phase2Abort()
		}
	}
}

func (c *Coordinator) phase2Commit() {
	c.mu.Lock()
	c.phase = 2
	c.mu.Unlock()

	fmt.Println("\n========== 第二阶段: Commit 阶段 ==========")
	fmt.Println("[Coordinator] 所有参与者都同意，发送 Commit 命令")

	for _, p := range c.participants {
		go func(participant *Participant) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message{
				Type:          Commit,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: c.txnID,
			}
		}(p)
	}
}

func (c *Coordinator) phase2Abort() {
	c.mu.Lock()
	c.phase = 2
	c.mu.Unlock()

	fmt.Println("\n========== 第二阶段: Abort 阶段 ==========")
	fmt.Println("[Coordinator] 至少一个参与者拒绝，发送 Abort 命令")

	for _, p := range c.participants {
		go func(participant *Participant) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message{
				Type:          Abort,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: c.txnID,
			}
		}(p)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("========== 2PC 协议演示 ==========")
	fmt.Println("场景1: 所有参与者都同意提交")
	fmt.Println("==================================")

	p1 := NewParticipant(1, true)
	p2 := NewParticipant(2, true)
	p3 := NewParticipant(3, true)

	participants := []*Participant{p1, p2, p3}
	coordinator = NewCoordinator(participants)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		coordinator.Run()
	}()

	for _, p := range participants {
		wg.Add(1)
		go func(participant *Participant) {
			defer wg.Done()
			participant.Run()
		}(p)
	}

	coordinator.phase1Prepare("TXN-001")

	time.Sleep(4 * time.Second)

	fmt.Println("\n========== 最终状态 ==========")
	for _, p := range participants {
		p.mu.Lock()
		fmt.Printf("[Participant %d] 状态: %s, 数据持久化: %v\n", p.ID, p.state, p.persistent)
		p.mu.Unlock()
	}

	time.Sleep(1 * time.Second)

	for _, p := range participants {
		close(p.doneChan)
	}
	close(coordinator.doneChan)

	wg.Wait()

	fmt.Println("\n\n========== 2PC 协议演示 ==========")
	fmt.Println("场景2: 有参与者拒绝提交")
	fmt.Println("==================================")

	p4 := NewParticipant(1, true)
	p5 := NewParticipant(2, false)
	p6 := NewParticipant(3, true)

	participants2 := []*Participant{p4, p5, p6}
	coordinator2 := NewCoordinator(participants2)
	coordinator = coordinator2

	var wg2 sync.WaitGroup

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		coordinator2.Run()
	}()

	for _, p := range participants2 {
		wg2.Add(1)
		go func(participant *Participant) {
			defer wg2.Done()
			participant.Run()
		}(p)
	}

	coordinator2.phase1Prepare("TXN-002")

	time.Sleep(4 * time.Second)

	fmt.Println("\n========== 最终状态 ==========")
	for _, p := range participants2 {
		p.mu.Lock()
		fmt.Printf("[Participant %d] 状态: %s, 数据持久化: %v\n", p.ID, p.state, p.persistent)
		p.mu.Unlock()
	}

	time.Sleep(1 * time.Second)

	for _, p := range participants2 {
		close(p.doneChan)
	}
	close(coordinator2.doneChan)

	wg2.Wait()

	fmt.Println("\n\n========== 2PC 特点总结 ==========")
	fmt.Println("1. 两阶段提交保证了分布式事务的原子性")
	fmt.Println("2. 第一阶段(Prepare): 协调者询问参与者是否可以提交，参与者进行预提交")
	fmt.Println("3. 第二阶段(Commit/Abort): 协调者根据投票结果决定提交或回滚")
	fmt.Println("4. 阻塞问题: 如果协调者崩溃，参与者会一直停留在Prepared状态")
	fmt.Println("5. 单点故障: 协调者是单点，一旦崩溃整个事务无法继续")
	fmt.Println("6. 一致性保证: 只有所有参与者都同意，事务才会提交")
}
