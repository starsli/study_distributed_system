package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type TCCResource struct {
	ID       int
	Name     string
	Quantity int
	Reserved int
}

type TCCTransaction struct {
	ID        string
	Status    string
	Resources []*TCCResource
}

var tccServiceColors = []string{"\033[32m", "\033[31m", "\033[33m", "\033[34m", "\033[35m", "\033[36m"}

type TCCService struct {
	mu         sync.Mutex
	ID         int
	Name       string
	Resources  map[int]*TCCResource
	msgChan    chan TCCOperation
	doneChan   chan struct{}
	canConfirm bool
	txnResults map[string]bool
	color      string
}

type TCCOperation struct {
	Type         string
	TxnID        string
	ServiceID    int
	ResourceID   int
	Quantity     int
	Success      bool
	ErrorMessage string
}

func NewTCCService(id int, name string) *TCCService {
	return &TCCService{
		ID:         id,
		Name:       name,
		Resources:  make(map[int]*TCCResource),
		msgChan:    make(chan TCCOperation, 10),
		doneChan:   make(chan struct{}),
		canConfirm: true,
		txnResults: make(map[string]bool),
		color:      tccServiceColors[(id-1)%len(tccServiceColors)],
	}
}

func (s *TCCService) AddResource(id int, name string, quantity int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Resources[id] = &TCCResource{
		ID:       id,
		Name:     name,
		Quantity: quantity,
		Reserved: 0,
	}
}

func (s *TCCService) Run() {
	for {
		select {
		case op := <-s.msgChan:
			s.handleOperation(op)
		case <-s.doneChan:
			return
		}
	}
}

func (s *TCCService) handleOperation(op TCCOperation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("%s[%s] 收到操作: %s, TxnID=%s\033[0m\n", s.color, s.Name, op.Type, op.TxnID)

	switch op.Type {
	case "Try":
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
		resource, exists := s.Resources[op.ResourceID]
		if !exists {
			fmt.Printf("%s[%s] Try失败: 资源不存在\033[0m\n", s.color, s.Name)
			tccCoordinator.msgChan <- TCCOperation{Type: "TryResult", TxnID: op.TxnID, ServiceID: s.ID, Success: false, ErrorMessage: "资源不存在"}
			return
		}
		if !s.canConfirm {
			fmt.Printf("%s[%s] Try失败: 服务不可用\033[0m\n", s.color, s.Name)
			tccCoordinator.msgChan <- TCCOperation{Type: "TryResult", TxnID: op.TxnID, ServiceID: s.ID, Success: false, ErrorMessage: "服务不可用"}
			return
		}
		if resource.Quantity < op.Quantity+resource.Reserved {
			fmt.Printf("%s[%s] Try失败: 资源不足 (可用:%d, 预留:%d, 请求:%d)\033[0m\n", s.color, s.Name, resource.Quantity, resource.Reserved, op.Quantity)
			tccCoordinator.msgChan <- TCCOperation{Type: "TryResult", TxnID: op.TxnID, ServiceID: s.ID, Success: false, ErrorMessage: "资源不足"}
			return
		}
		resource.Reserved += op.Quantity
		fmt.Printf("%s[%s] Try成功: 预留资源 %s x%d (剩余:%d, 预留:%d)\033[0m\n", s.color, s.Name, resource.Name, op.Quantity, resource.Quantity, resource.Reserved)
		s.txnResults[op.TxnID] = true
		tccCoordinator.msgChan <- TCCOperation{Type: "TryResult", TxnID: op.TxnID, ServiceID: s.ID, Success: true}

	case "Confirm":
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
		resource, exists := s.Resources[op.ResourceID]
		if exists && resource.Reserved >= op.Quantity {
			resource.Quantity -= op.Quantity
			resource.Reserved -= op.Quantity
			fmt.Printf("%s[%s] Confirm成功: 扣减资源 %s x%d (剩余:%d)\033[0m\n", s.color, s.Name, resource.Name, op.Quantity, resource.Quantity)
			tccCoordinator.msgChan <- TCCOperation{Type: "ConfirmResult", TxnID: op.TxnID, ServiceID: s.ID, Success: true}
		} else {
			fmt.Printf("%s[%s] Confirm失败\033[0m\n", s.color, s.Name)
			tccCoordinator.msgChan <- TCCOperation{Type: "ConfirmResult", TxnID: op.TxnID, ServiceID: s.ID, Success: false}
		}

	case "Cancel":
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
		resource, exists := s.Resources[op.ResourceID]
		if exists && resource.Reserved >= op.Quantity {
			resource.Reserved -= op.Quantity
			fmt.Printf("%s[%s] Cancel成功: 释放预留资源 %s x%d (预留:%d)\033[0m\n", s.color, s.Name, resource.Name, op.Quantity, resource.Reserved)
			tccCoordinator.msgChan <- TCCOperation{Type: "CancelResult", TxnID: op.TxnID, ServiceID: s.ID, Success: true}
		} else {
			fmt.Printf("%s[%s] Cancel失败\033[0m\n", s.color, s.Name)
			tccCoordinator.msgChan <- TCCOperation{Type: "CancelResult", TxnID: op.TxnID, ServiceID: s.ID, Success: false}
		}
	}
}

type TCCCoordinator struct {
	mu             sync.Mutex
	msgChan        chan TCCOperation
	doneChan       chan struct{}
	services       []*TCCService
	tryCount       int
	trySuccess     int
	confirmCount   int
	confirmSuccess int
	cancelCount    int
	cancelSuccess  int
	currentTxnID   string
	phase          string
}

var tccCoordinator *TCCCoordinator

func NewTCCCoordinator(services []*TCCService) *TCCCoordinator {
	return &TCCCoordinator{
		msgChan:  make(chan TCCOperation, 10),
		doneChan: make(chan struct{}),
		services: services,
		phase:    "Init",
	}
}

func (c *TCCCoordinator) Run() {
	for {
		select {
		case op := <-c.msgChan:
			c.handleOperation(op)
		case <-c.doneChan:
			return
		}
	}
}

func (c *TCCCoordinator) handleOperation(op TCCOperation) {
	c.mu.Lock()

	switch op.Type {
	case "TryResult":
		c.tryCount++
		if op.Success {
			c.trySuccess++
		}
		fmt.Printf("\033[36m[Coordinator] Try结果: Service %d = %v, 成功=%d/%d\033[0m\n", op.ServiceID, op.Success, c.trySuccess, c.tryCount)

	case "ConfirmResult":
		c.confirmCount++
		if op.Success {
			c.confirmSuccess++
		}
		fmt.Printf("\033[36m[Coordinator] Confirm结果: Service %d = %v, 成功=%d/%d\033[0m\n", op.ServiceID, op.Success, c.confirmSuccess, c.confirmCount)

	case "CancelResult":
		c.cancelCount++
		if op.Success {
			c.cancelSuccess++
		}
		fmt.Printf("\033[36m[Coordinator] Cancel结果: Service %d = %v, 成功=%d/%d\033[0m\n", op.ServiceID, op.Success, c.cancelSuccess, c.cancelCount)
	}

	allTried := c.tryCount == len(c.services)
	allTrySuccess := c.trySuccess == len(c.services)
	allConfirmed := c.confirmCount == len(c.services)
	allCancelled := c.cancelCount == len(c.services)

	c.mu.Unlock()

	if c.phase == "Try" && allTried {
		if allTrySuccess {
			c.phaseConfirm()
		} else {
			c.phaseCancel()
		}
	} else if c.phase == "Confirm" && allConfirmed {
		fmt.Printf("\n\033[36m[Coordinator] 事务 %s 提交成功\033[0m\n", c.currentTxnID)
		c.phase = "Done"
	} else if c.phase == "Cancel" && allCancelled {
		fmt.Printf("\n\033[36m[Coordinator] 事务 %s 回滚成功\033[0m\n", c.currentTxnID)
		c.phase = "Done"
	}
}

func (c *TCCCoordinator) phaseTry(txnID string, resourceID int, quantity int) {
	c.mu.Lock()
	c.currentTxnID = txnID
	c.phase = "Try"
	c.tryCount = 0
	c.trySuccess = 0
	c.mu.Unlock()

	fmt.Println("\n========== Try 阶段 ==========")
	fmt.Printf("\033[36m[Coordinator] 发起事务 %s，向所有服务发送 Try 请求\033[0m\n", txnID)

	for _, service := range c.services {
		go func(s *TCCService) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			s.msgChan <- TCCOperation{Type: "Try", TxnID: txnID, ServiceID: s.ID, ResourceID: resourceID, Quantity: quantity}
		}(service)
	}
}

func (c *TCCCoordinator) phaseConfirm() {
	c.mu.Lock()
	c.phase = "Confirm"
	c.confirmCount = 0
	c.confirmSuccess = 0
	c.mu.Unlock()

	fmt.Println("\n========== Confirm 阶段 ==========")
	fmt.Println("\033[36m[Coordinator] 所有服务Try成功，发送 Confirm 请求\033[0m")

	for _, service := range c.services {
		go func(s *TCCService) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			s.msgChan <- TCCOperation{Type: "Confirm", TxnID: c.currentTxnID, ServiceID: s.ID, ResourceID: 1, Quantity: 1}
		}(service)
	}
}

func (c *TCCCoordinator) phaseCancel() {
	c.mu.Lock()
	c.phase = "Cancel"
	c.cancelCount = 0
	c.cancelSuccess = 0
	c.mu.Unlock()

	fmt.Println("\n========== Cancel 阶段 ==========")
	fmt.Println("\033[36m[Coordinator] 至少一个服务Try失败，发送 Cancel 请求\033[0m")

	for _, service := range c.services {
		go func(s *TCCService) {
			time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
			s.msgChan <- TCCOperation{Type: "Cancel", TxnID: c.currentTxnID, ServiceID: s.ID, ResourceID: 1, Quantity: 1}
		}(service)
	}
}

func runTCCDemo(canConfirmList []bool, txnID string) {
	services := []*TCCService{
		NewTCCService(1, "库存服务"),
		NewTCCService(2, "支付服务"),
		NewTCCService(3, "订单服务"),
	}

	services[0].AddResource(1, "商品A", 10)
	services[1].AddResource(1, "余额", 100)
	services[2].AddResource(1, "订单号", 1000)

	for i, canConfirm := range canConfirmList {
		services[i].canConfirm = canConfirm
	}

	tccCoordinator = NewTCCCoordinator(services)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		tccCoordinator.Run()
	}()

	for _, service := range services {
		wg.Add(1)
		go func(s *TCCService) {
			defer wg.Done()
			s.Run()
		}(service)
	}

	tccCoordinator.phaseTry(txnID, 1, 1)

	time.Sleep(3 * time.Second)

	fmt.Println("\n========== 最终状态 ==========")
	for _, service := range services {
		service.mu.Lock()
		for _, resource := range service.Resources {
			fmt.Printf("%s[%s] 资源: %s, 数量:%d, 预留:%d\033[0m\n", service.color, service.Name, resource.Name, resource.Quantity, resource.Reserved)
		}
		service.mu.Unlock()
	}

	time.Sleep(500 * time.Millisecond)

	for _, service := range services {
		close(service.doneChan)
	}
	close(tccCoordinator.doneChan)

	wg.Wait()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("========== TCC 协议演示 ==========")
	fmt.Println("场景1: 所有服务都成功")
	fmt.Println("==================================")

	runTCCDemo([]bool{true, true, true}, "TXN-TCC-001")

	fmt.Println("\n\n========== TCC 协议演示 ==========")
	fmt.Println("场景2: 支付服务失败")
	fmt.Println("==================================")

	runTCCDemo([]bool{true, false, true}, "TXN-TCC-002")

	fmt.Println("\n\n========== TCC 特点总结 ==========")
	fmt.Println("1. TCC是一种补偿事务模式，不依赖数据库事务")
	fmt.Println("2. Try阶段: 预留资源，检查业务条件")
	fmt.Println("3. Confirm阶段: 确认提交，执行业务操作")
	fmt.Println("4. Cancel阶段: 取消操作，释放预留资源")
	fmt.Println("5. 最终一致性: 通过补偿机制实现")
	fmt.Println("6. 高可用: 不依赖单点协调者")
	fmt.Println("7. 幂等性: 需要保证Confirm/Cancel操作可重复执行")
	fmt.Println("8. 业务侵入性强: 需要在业务代码中实现三个接口")
}
