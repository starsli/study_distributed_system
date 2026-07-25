package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type ParticipantState3PC int

const (
	Idle3PC ParticipantState3PC = iota
	Waiting
	Prepared
	Committed
	Aborted
)

func (s ParticipantState3PC) String() string {
	switch s {
	case Idle3PC:
		return "Idle"
	case Waiting:
		return "Waiting"
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

type MsgType3PC int

const (
	CanCommit MsgType3PC = iota
	Yes
	No
	PreCommit
	Ready
	DoCommit
	Abort3PC
)

func (m MsgType3PC) String() string {
	switch m {
	case CanCommit:
		return "CanCommit"
	case Yes:
		return "Yes"
	case No:
		return "No"
	case PreCommit:
		return "PreCommit"
	case Ready:
		return "Ready"
	case DoCommit:
		return "DoCommit"
	case Abort3PC:
		return "Abort"
	default:
		return "Unknown"
	}
}

type Message3PC struct {
	Type          MsgType3PC
	FromID        int
	ToID          int
	TransactionID string
}

var participantColors3PC = []string{"\033[32m", "\033[31m", "\033[33m", "\033[34m", "\033[35m", "\033[36m"}

type Participant3PC struct {
	mu         sync.Mutex
	ID         int
	state      ParticipantState3PC
	msgChan    chan Message3PC
	doneChan   chan struct{}
	canCommit  bool
	txnData    string
	persistent bool
	color      string
}

func NewParticipant3PC(id int, canCommit bool) *Participant3PC {
	return &Participant3PC{
		ID:         id,
		state:      Idle3PC,
		msgChan:    make(chan Message3PC, 10),
		doneChan:   make(chan struct{}),
		canCommit:  canCommit,
		persistent: false,
		color:      participantColors3PC[(id-1)%len(participantColors3PC)],
	}
}

func (p *Participant3PC) Run() {
	for {
		select {
		case msg := <-p.msgChan:
			p.handleMessage(msg)
		case <-p.doneChan:
			return
		}
	}
}

func (p *Participant3PC) handleMessage(msg Message3PC) {
	p.mu.Lock()
	defer p.mu.Unlock()

	fmt.Printf("%s[Participant %d] 收到消息: %s, 当前状态: %s\033[0m\n", p.color, p.ID, msg.Type, p.state)

	switch msg.Type {
	case CanCommit:
		time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
		if p.canCommit {
			p.state = Waiting
			fmt.Printf("%s[Participant %d] 状态变更: Idle -> Waiting\033[0m\n", p.color, p.ID)
			p.sendResponse(msg.TransactionID, Yes)
		} else {
			p.state = Aborted
			fmt.Printf("%s[Participant %d] 状态变更: Idle -> Aborted\033[0m\n", p.color, p.ID)
			p.sendResponse(msg.TransactionID, No)
		}

	case PreCommit:
		if p.state == Waiting {
			time.Sleep(time.Duration(300+rand.Intn(300)) * time.Millisecond)
			p.state = Prepared
			p.persistent = true
			fmt.Printf("%s[Participant %d] 状态变更: Waiting -> Prepared (持久化成功)\033[0m\n", p.color, p.ID)
			p.sendResponse(msg.TransactionID, Ready)
		}

	case DoCommit:
		if p.state == Prepared {
			p.state = Committed
			p.txnData = "已提交的数据"
			fmt.Printf("%s[Participant %d] 状态变更: Prepared -> Committed\033[0m\n", p.color, p.ID)
		}

	case Abort3PC:
		if p.state == Waiting || p.state == Prepared {
			p.state = Aborted
			fmt.Printf("%s[Participant %d] 状态变更: %s -> Aborted\033[0m\n", p.color, p.ID, p.state)
		}
	}
}

func (p *Participant3PC) sendResponse(txnID string, response MsgType3PC) {
	fmt.Printf("%s[Participant %d] 发送响应: %s\033[0m\n", p.color, p.ID, response)
	coordinator3PC.msgChan <- Message3PC{
		Type:          response,
		FromID:        p.ID,
		ToID:          0,
		TransactionID: txnID,
	}
}

type Coordinator3PC struct {
	mu           sync.Mutex
	msgChan      chan Message3PC
	doneChan     chan struct{}
	participants []*Participant3PC
	voteCount    int
	yesCount     int
	readyCount   int
	txnID        string
	phase        int
}

var coordinator3PC *Coordinator3PC

func NewCoordinator3PC(participants []*Participant3PC) *Coordinator3PC {
	return &Coordinator3PC{
		msgChan:      make(chan Message3PC, 10),
		doneChan:     make(chan struct{}),
		participants: participants,
	}
}

func (c *Coordinator3PC) Run() {
	for {
		select {
		case msg := <-c.msgChan:
			c.handleMessage(msg)
		case <-c.doneChan:
			return
		}
	}
}

func (c *Coordinator3PC) handleMessage(msg Message3PC) {
	c.mu.Lock()

	fmt.Printf("\033[34m[Coordinator] 收到消息: %s 来自 Participant %d\033[0m\n", msg.Type, msg.FromID)

	switch msg.Type {
	case Yes:
		c.yesCount++
		c.voteCount++
		fmt.Printf("\033[34m[Coordinator] 累计投票: Yes=%d, Total=%d\033[0m\n", c.yesCount, c.voteCount)

	case No:
		c.voteCount++
		fmt.Printf("\033[34m[Coordinator] 累计投票: Yes=%d, Total=%d (收到No)\033[0m\n", c.yesCount, c.voteCount)

	case Ready:
		c.readyCount++
		fmt.Printf("\033[34m[Coordinator] 累计Ready: %d/%d\033[0m\n", c.readyCount, len(c.participants))
	}

	allVoted := c.voteCount == len(c.participants)
	allYes := c.yesCount == len(c.participants)
	hasNo := !allYes && allVoted || msg.Type == No
	allReady := c.readyCount == len(c.participants)

	c.mu.Unlock()

	if c.phase == 1 && allVoted {
		if allYes {
			c.phase2PreCommit()
		} else {
			c.phaseAbort()
		}
	} else if c.phase == 1 && hasNo {
		c.phaseAbort()
	} else if c.phase == 2 && allReady {
		c.phase3DoCommit()
	}
}

func (c *Coordinator3PC) phase1CanCommit(txnID string) {
	c.mu.Lock()
	c.txnID = txnID
	c.phase = 1
	c.voteCount = 0
	c.yesCount = 0
	c.readyCount = 0
	c.mu.Unlock()

	fmt.Println("\n========== 第一阶段: CanCommit 阶段 ==========")
	fmt.Printf("\033[34m[Coordinator] 发起事务 %s，向所有参与者发送 CanCommit\033[0m\n", txnID)

	for _, p := range c.participants {
		go func(participant *Participant3PC) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message3PC{
				Type:          CanCommit,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: txnID,
			}
		}(p)
	}
}

func (c *Coordinator3PC) phase2PreCommit() {
	c.mu.Lock()
	c.phase = 2
	c.mu.Unlock()

	fmt.Println("\n========== 第二阶段: PreCommit 阶段 ==========")
	fmt.Println("\033[34m[Coordinator] 所有参与者都同意，发送 PreCommit 命令\033[0m")

	for _, p := range c.participants {
		go func(participant *Participant3PC) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message3PC{
				Type:          PreCommit,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: c.txnID,
			}
		}(p)
	}
}

func (c *Coordinator3PC) phase3DoCommit() {
	c.mu.Lock()
	c.phase = 3
	c.mu.Unlock()

	fmt.Println("\n========== 第三阶段: DoCommit 阶段 ==========")
	fmt.Println("\033[34m[Coordinator] 所有参与者都Ready，发送 DoCommit 命令\033[0m")

	for _, p := range c.participants {
		go func(participant *Participant3PC) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message3PC{
				Type:          DoCommit,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: c.txnID,
			}
		}(p)
	}
}

func (c *Coordinator3PC) phaseAbort() {
	c.mu.Lock()
	c.phase = 99
	c.mu.Unlock()

	fmt.Println("\n========== Abort 阶段 ==========")
	fmt.Println("\033[34m[Coordinator] 至少一个参与者拒绝，发送 Abort 命令\033[0m")

	for _, p := range c.participants {
		go func(participant *Participant3PC) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			participant.msgChan <- Message3PC{
				Type:          Abort3PC,
				FromID:        0,
				ToID:          participant.ID,
				TransactionID: c.txnID,
			}
		}(p)
	}
}

func run3PCDemo(canCommitList []bool, txnID string) {
	var participants []*Participant3PC
	for i, canCommit := range canCommitList {
		participants = append(participants, NewParticipant3PC(i+1, canCommit))
	}

	coordinator3PC = NewCoordinator3PC(participants)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		coordinator3PC.Run()
	}()

	for _, p := range participants {
		wg.Add(1)
		go func(participant *Participant3PC) {
			defer wg.Done()
			participant.Run()
		}(p)
	}

	coordinator3PC.phase1CanCommit(txnID)

	time.Sleep(4 * time.Second)

	fmt.Println("\n========== 最终状态 ==========")
	for _, p := range participants {
		p.mu.Lock()
		fmt.Printf("%s[Participant %d] 状态: %s, 数据持久化: %v\033[0m\n", p.color, p.ID, p.state, p.persistent)
		p.mu.Unlock()
	}

	time.Sleep(500 * time.Millisecond)

	for _, p := range participants {
		close(p.doneChan)
	}
	close(coordinator3PC.doneChan)

	wg.Wait()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("========== 3PC 协议演示 ==========")
	fmt.Println("场景1: 所有参与者都同意提交")
	fmt.Println("==================================")

	run3PCDemo([]bool{true, true, true}, "TXN-001")

	fmt.Println("\n\n========== 3PC 协议演示 ==========")
	fmt.Println("场景2: 有参与者拒绝提交")
	fmt.Println("==================================")

	run3PCDemo([]bool{true, false, true}, "TXN-002")

	fmt.Println("\n\n========== 3PC 特点总结 ==========")
	fmt.Println("1. 三阶段提交是2PC的改进，增加了CanCommit阶段")
	fmt.Println("2. 第一阶段(CanCommit): 协调者询问参与者是否可以提交")
	fmt.Println("3. 第二阶段(PreCommit): 协调者通知参与者准备提交")
	fmt.Println("4. 第三阶段(DoCommit): 协调者通知参与者执行提交")
	fmt.Println("5. 减少阻塞: 参与者在Waiting状态时不阻塞资源")
	fmt.Println("6. 超时机制: 如果超时未收到DoCommit，参与者自动提交")
	fmt.Println("7. 仍然存在单点故障问题")
	fmt.Println("8. 可能导致数据不一致（网络分区时）")
}
