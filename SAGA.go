// SAGA 模式
// saga 长篇叙事、一连串连贯事件
package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type SagaStep struct {
	ID          int
	Name        string
	ServiceName string
	Operation   string
	Compensate  string
	Status      string
}

type SagaTransaction struct {
	ID      string
	Steps   []*SagaStep
	Current int
	Status  string
}

var sagaServiceColors = []string{"\033[32m", "\033[31m", "\033[33m", "\033[34m", "\033[35m", "\033[36m"}

type SagaService struct {
	mu         sync.Mutex
	ID         int
	Name       string
	Resources  map[string]int
	msgChan    chan SagaMessage
	doneChan   chan struct{}
	canSucceed bool
	color      string
}

type SagaMessage struct {
	Type      string
	TxnID     string
	StepID    int
	ServiceID int
	Data      map[string]interface{}
	Success   bool
	Error     string
}

func NewSagaService(id int, name string) *SagaService {
	return &SagaService{
		ID:         id,
		Name:       name,
		Resources:  make(map[string]int),
		msgChan:    make(chan SagaMessage, 10),
		doneChan:   make(chan struct{}),
		canSucceed: true,
		color:      sagaServiceColors[(id-1)%len(sagaServiceColors)],
	}
}

func (s *SagaService) AddResource(name string, quantity int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Resources[name] = quantity
}

func (s *SagaService) Run() {
	for {
		select {
		case msg := <-s.msgChan:
			s.handleMessage(msg)
		case <-s.doneChan:
			return
		}
	}
}

func (s *SagaService) handleMessage(msg SagaMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Printf("%s[%s] 收到消息: %s, TxnID=%s, Step=%d\033[0m\n", s.color, s.Name, msg.Type, msg.TxnID, msg.StepID)

	switch msg.Type {
	case "Execute":
		time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
		if !s.canSucceed {
			fmt.Printf("%s[%s] 执行失败: 模拟业务异常\033[0m\n", s.color, s.Name)
			sagaCoordinator.msgChan <- SagaMessage{Type: "ExecuteResult", TxnID: msg.TxnID, StepID: msg.StepID, ServiceID: s.ID, Success: false, Error: "业务异常"}
			return
		}
		switch msg.Data["operation"] {
		case "create_order":
			s.Resources["orders"]++
			s.Resources["order_ids"] = msg.StepID * 1000
			fmt.Printf("%s[%s] 执行成功: 创建订单 #%d\033[0m\n", s.color, s.Name, s.Resources["order_ids"])

		case "reserve_inventory":
			if s.Resources["inventory"] >= msg.Data["quantity"].(int) {
				s.Resources["inventory"] -= msg.Data["quantity"].(int)
				s.Resources["reserved"] += msg.Data["quantity"].(int)
				fmt.Printf("%s[%s] 执行成功: 扣减库存 %d, 剩余:%d\033[0m\n", s.color, s.Name, msg.Data["quantity"].(int), s.Resources["inventory"])
			} else {
				fmt.Printf("%s[%s] 执行失败: 库存不足\033[0m\n", s.color, s.Name)
				sagaCoordinator.msgChan <- SagaMessage{Type: "ExecuteResult", TxnID: msg.TxnID, StepID: msg.StepID, ServiceID: s.ID, Success: false, Error: "库存不足"}
				return
			}

		case "process_payment":
			if s.Resources["balance"] >= msg.Data["amount"].(int) {
				s.Resources["balance"] -= msg.Data["amount"].(int)
				s.Resources["paid"] += msg.Data["amount"].(int)
				fmt.Printf("%s[%s] 执行成功: 扣款 %d, 余额:%d\033[0m\n", s.color, s.Name, msg.Data["amount"].(int), s.Resources["balance"])
			} else {
				fmt.Printf("%s[%s] 执行失败: 余额不足\033[0m\n", s.color, s.Name)
				sagaCoordinator.msgChan <- SagaMessage{Type: "ExecuteResult", TxnID: msg.TxnID, StepID: msg.StepID, ServiceID: s.ID, Success: false, Error: "余额不足"}
				return
			}

		case "send_notification":
			fmt.Printf("%s[%s] 执行成功: 发送通知\033[0m\n", s.color, s.Name)
		}
		sagaCoordinator.msgChan <- SagaMessage{Type: "ExecuteResult", TxnID: msg.TxnID, StepID: msg.StepID, ServiceID: s.ID, Success: true}

	case "Compensate":
		time.Sleep(time.Duration(300+rand.Intn(400)) * time.Millisecond)
		switch msg.Data["compensate"] {
		case "cancel_order":
			s.Resources["orders"]--
			fmt.Printf("%s[%s] 补偿成功: 取消订单\033[0m\n", s.color, s.Name)

		case "release_inventory":
			s.Resources["inventory"] += msg.Data["quantity"].(int)
			s.Resources["reserved"] -= msg.Data["quantity"].(int)
			fmt.Printf("%s[%s] 补偿成功: 释放库存 %d, 剩余:%d\033[0m\n", s.color, s.Name, msg.Data["quantity"].(int), s.Resources["inventory"])

		case "refund_payment":
			s.Resources["balance"] += msg.Data["amount"].(int)
			s.Resources["paid"] -= msg.Data["amount"].(int)
			fmt.Printf("%s[%s] 补偿成功: 退款 %d, 余额:%d\033[0m\n", s.color, s.Name, msg.Data["amount"].(int), s.Resources["balance"])

		case "cancel_notification":
			fmt.Printf("%s[%s] 补偿成功: 取消通知\033[0m\n", s.color, s.Name)
		}
		sagaCoordinator.msgChan <- SagaMessage{Type: "CompensateResult", TxnID: msg.TxnID, StepID: msg.StepID, ServiceID: s.ID, Success: true}
	}
}

type SagaCoordinator struct {
	mu           sync.Mutex
	msgChan      chan SagaMessage
	doneChan     chan struct{}
	services     map[string]*SagaService
	txn          *SagaTransaction
	compensating bool
}

var sagaCoordinator *SagaCoordinator

func NewSagaCoordinator(services []*SagaService) *SagaCoordinator {
	serviceMap := make(map[string]*SagaService)
	for _, s := range services {
		serviceMap[s.Name] = s
	}
	return &SagaCoordinator{
		msgChan:  make(chan SagaMessage, 10),
		doneChan: make(chan struct{}),
		services: serviceMap,
	}
}

func (c *SagaCoordinator) Run() {
	for {
		select {
		case msg := <-c.msgChan:
			c.handleMessage(msg)
		case <-c.doneChan:
			return
		}
	}
}

func (c *SagaCoordinator) handleMessage(msg SagaMessage) {
	c.mu.Lock()

	if c.txn == nil {
		c.mu.Unlock()
		return
	}

	var nextStep int
	var needCompensate bool
	var compensateStart int

	switch msg.Type {
	case "ExecuteResult":
		step := c.txn.Steps[msg.StepID]
		if msg.Success {
			step.Status = "Success"
			fmt.Printf("\033[35m[Coordinator] Step %d (%s) 执行成功\033[0m\n", msg.StepID, step.Name)

			if msg.StepID < len(c.txn.Steps)-1 {
				nextStep = msg.StepID + 1
			} else {
				c.txn.Status = "Completed"
				fmt.Printf("\n\033[35m[Coordinator] 事务 %s 完成\033[0m\n", c.txn.ID)
			}
		} else {
			step.Status = "Failed"
			fmt.Printf("\033[35m[Coordinator] Step %d (%s) 执行失败: %s\033[0m\n", msg.StepID, step.Name, msg.Error)
			needCompensate = true
			compensateStart = msg.StepID
		}

	case "CompensateResult":
		step := c.txn.Steps[msg.StepID]
		step.Status = "Compensated"
		fmt.Printf("\033[35m[Coordinator] Step %d (%s) 补偿成功\033[0m\n", msg.StepID, step.Name)

		if msg.StepID > 0 {
			nextStep = msg.StepID - 1
			needCompensate = true
			compensateStart = -1
		} else {
			c.txn.Status = "RolledBack"
			fmt.Printf("\n\033[35m[Coordinator] 事务 %s 回滚完成\033[0m\n", c.txn.ID)
		}
	}

	c.mu.Unlock()

	if nextStep >= 0 && !needCompensate {
		c.executeStep(nextStep)
	} else if needCompensate && compensateStart >= 0 {
		c.startCompensation(compensateStart)
	} else if needCompensate && compensateStart < 0 {
		c.compensateStep(nextStep)
	}
}

func (c *SagaCoordinator) startTransaction(txnID string) {
	c.mu.Lock()
	c.txn = &SagaTransaction{
		ID:     txnID,
		Status: "Running",
		Steps: []*SagaStep{
			{ID: 0, Name: "创建订单", ServiceName: "订单服务", Operation: "create_order", Compensate: "cancel_order", Status: "Pending"},
			{ID: 1, Name: "扣减库存", ServiceName: "库存服务", Operation: "reserve_inventory", Compensate: "release_inventory", Status: "Pending"},
			{ID: 2, Name: "处理支付", ServiceName: "支付服务", Operation: "process_payment", Compensate: "refund_payment", Status: "Pending"},
			{ID: 3, Name: "发送通知", ServiceName: "通知服务", Operation: "send_notification", Compensate: "cancel_notification", Status: "Pending"},
		},
		Current: 0,
	}
	c.compensating = false
	c.mu.Unlock()

	fmt.Println("\n========== Saga 事务开始 ==========")
	fmt.Printf("\033[35m[Coordinator] 发起事务 %s，包含 %d 个步骤\033[0m\n", txnID, len(c.txn.Steps))
	c.executeStep(0)
}

func (c *SagaCoordinator) executeStep(stepIndex int) {
	step := c.txn.Steps[stepIndex]
	service := c.services[step.ServiceName]

	fmt.Printf("\n--- 执行步骤 %d: %s ---\n", stepIndex, step.Name)

	go func(s *SagaService, st *SagaStep, idx int) {
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
		data := map[string]interface{}{
			"operation": st.Operation,
			"quantity":  1,
			"amount":    100,
		}
		s.msgChan <- SagaMessage{Type: "Execute", TxnID: c.txn.ID, StepID: idx, ServiceID: s.ID, Data: data}
	}(service, step, stepIndex)
}

func (c *SagaCoordinator) startCompensation(failedStepIndex int) {
	c.mu.Lock()
	c.compensating = true
	c.mu.Unlock()

	fmt.Println("\n========== Saga 补偿开始 ==========")
	fmt.Printf("\033[35m[Coordinator] 步骤 %d 失败，开始补偿步骤 %d-%d\033[0m\n", failedStepIndex, failedStepIndex-1, 0)

	c.compensateStep(failedStepIndex - 1)
}

func (c *SagaCoordinator) compensateStep(stepIndex int) {
	step := c.txn.Steps[stepIndex]
	if step.Status != "Success" {
		if stepIndex > 0 {
			c.compensateStep(stepIndex - 1)
		} else {
			c.txn.Status = "RolledBack"
			fmt.Printf("\n\033[35m[Coordinator] 事务 %s 回滚完成\033[0m\n", c.txn.ID)
		}
		return
	}

	service := c.services[step.ServiceName]

	fmt.Printf("\n--- 补偿步骤 %d: %s ---\n", stepIndex, step.Name)

	go func(s *SagaService, st *SagaStep, idx int) {
		time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
		data := map[string]interface{}{
			"compensate": st.Compensate,
			"quantity":   1,
			"amount":     100,
		}
		s.msgChan <- SagaMessage{Type: "Compensate", TxnID: c.txn.ID, StepID: idx, ServiceID: s.ID, Data: data}
	}(service, step, stepIndex)
}

func runSagaDemo(failStepIndex int, txnID string) {
	services := []*SagaService{
		NewSagaService(1, "订单服务"),
		NewSagaService(2, "库存服务"),
		NewSagaService(3, "支付服务"),
		NewSagaService(4, "通知服务"),
	}

	services[0].AddResource("orders", 0)
	services[0].AddResource("order_ids", 0)
	services[1].AddResource("inventory", 10)
	services[1].AddResource("reserved", 0)
	services[2].AddResource("balance", 1000)
	services[2].AddResource("paid", 0)

	if failStepIndex >= 0 && failStepIndex < len(services) {
		services[failStepIndex].canSucceed = false
	}

	sagaCoordinator = NewSagaCoordinator(services)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		sagaCoordinator.Run()
	}()

	for _, service := range services {
		wg.Add(1)
		go func(s *SagaService) {
			defer wg.Done()
			s.Run()
		}(service)
	}

	sagaCoordinator.startTransaction(txnID)

	time.Sleep(5 * time.Second)

	fmt.Println("\n========== 最终状态 ==========")
	for _, service := range services {
		service.mu.Lock()
		fmt.Printf("%s[%s]:\033[0m\n", service.color, service.Name)
		for k, v := range service.Resources {
			fmt.Printf("  %s: %d\n", k, v)
		}
		service.mu.Unlock()
	}

	time.Sleep(500 * time.Millisecond)

	for _, service := range services {
		close(service.doneChan)
	}
	close(sagaCoordinator.doneChan)

	wg.Wait()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	fmt.Println("========== Saga 协议演示 ==========")
	fmt.Println("场景1: 所有步骤都成功")
	fmt.Println("==================================")

	runSagaDemo(-1, "TXN-SAGA-001")

	fmt.Println("\n\n========== Saga 协议演示 ==========")
	fmt.Println("场景2: 支付服务失败，触发补偿")
	fmt.Println("==================================")

	runSagaDemo(2, "TXN-SAGA-002")

	fmt.Println("\n\n========== Saga 特点总结 ==========")
	fmt.Println("1. Saga将长事务拆分为多个本地事务步骤")
	fmt.Println("2. 每个步骤是一个独立的本地事务")
	fmt.Println("3. 每个步骤有对应的补偿事务")
	fmt.Println("4. 正向执行: 依次执行每个步骤")
	fmt.Println("5. 反向补偿: 如果某个步骤失败，按逆序执行补偿")
	fmt.Println("6. 最终一致性: 通过补偿机制保证")
	fmt.Println("7. 无锁阻塞: 每个步骤独立提交，不阻塞其他事务")
	fmt.Println("8. 复杂的协调逻辑: 需要处理各种失败场景")
	fmt.Println("9. 可能出现部分成功的中间状态")
}
