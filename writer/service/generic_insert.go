package service

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	fch "github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/metrico/qryn/v4/logger"
	"github.com/metrico/qryn/v4/writer/chwrapper"
	"github.com/metrico/qryn/v4/writer/model"
	"github.com/metrico/qryn/v4/writer/utils/helpers"
	"github.com/metrico/qryn/v4/writer/utils/promise"
	"github.com/metrico/qryn/v4/writer/utils/stat"
)

const (
	INSERT_MODE_DEFAULT = 1
	INSERT_MODE_SYNC    = 2
	INSERT_MODE_ASYNC   = 3
)

const (
	INSERT_STATE_IDLE      = 0
	INSERT_STATE_INSERTING = 1
	INSERT_STATE_CLOSING   = 2
)

const (
	BANDWITH_LIMIT = 50 * 1024 * 1024
)

type InsertRequest interface {
	Rows() []interface{}
	Response() chan error
}

type TableTimeSeriesReq struct {
	TimeSeries []*model.TableTimeSeries
	Resp       chan error
}

type TableMetrics struct {
	Metrics []*model.TableMetrics
	Resp    chan error
}

type IInsertServiceV2 interface {
	Run()
	Stop()
	Request(req helpers.SizeGetter, insertMode int) *promise.Promise[uint32]
	Ping() (time.Time, error)
	GetState(insertMode int) int
	GetNodeName() string
	Init()
	PlanFlush()
}

type requestPortion struct {
	cols []IColPoolRes
	// res  []*chan error
	res  []*promise.Promise[uint32]
	size int64
}

type InsertServiceV2 struct {
	ServiceData
	ID string

	V3Session      chwrapper.IChClientFactory
	DatabaseNode   *model.DataDatabasesMap
	AsyncInsert    bool
	OnBeforeInsert func()
	pushInterval   time.Duration
	maxQueueSize   int64
	serviceType    string

	insertRequest string

	acquireColumns func() []IColPoolRes
	processRequest func(any, []IColPoolRes) (int, []IColPoolRes, error)

	columns  []IColPoolRes
	size     int64
	lastSend time.Time
	results  []*promise.Promise[uint32]
	running  bool

	watchdog    *time.Ticker
	lastRequest time.Time
	ctx         context.Context
	cancel      context.CancelFunc

	insertCtx    context.Context
	insertCancel context.CancelFunc

	mtx sync.Mutex

	client chwrapper.IChClient

	state int32
}

func (svc *InsertServiceV2) PlanFlush() {
	svc.mtx.Lock()
	defer svc.mtx.Unlock()
	svc.insertCancel()
}

func (svc *InsertServiceV2) Init() {
	if svc.running {
		return
	}
	func() {
		svc.mtx.Lock()
		defer svc.mtx.Unlock()
		svc.watchdog = time.NewTicker(time.Second)
		svc.ctx, svc.cancel = context.WithCancel(context.Background())
		svc.insertCtx, svc.insertCancel = context.WithTimeout(context.Background(), svc.pushInterval)
		svc.columns = svc.acquireColumns()
		svc.lastRequest = time.Now()
		svc.running = true
	}()
}

func (svc *InsertServiceV2) GetNodeName() string {
	return svc.DatabaseNode.Node
}

func (svc *InsertServiceV2) timeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), time.Duration(
		int64(svc.DatabaseNode.WriteTimeout)*int64(time.Second)))
}

func (svc *InsertServiceV2) Ping() (time.Time, error) {
	checkDuration := time.Duration(int64(svc.DatabaseNode.WriteTimeout)*int64(time.Second)*2) + time.Second*5
	if svc.lastRequest.Add(checkDuration).After(time.Now()) {
		return svc.lastRequest, nil
	}
	return svc.lastRequest, fmt.Errorf("[SVC005] insert service `%s` must be in a deadlock", svc.ID)
}

func (svc *InsertServiceV2) GetState(insertMode int) int {
	return int(atomic.LoadInt32(&svc.state))
}

func (svc *InsertServiceV2) Run() {
	for {
		select {
		case <-svc.watchdog.C:
			svc.ping()
		case <-svc.ctx.Done():
			svc.mtx.Lock()
			for _, r := range svc.results {
				r.Done(0, fmt.Errorf("[IS003] insert service is stopped"))
			}
			svc.running = false
			svc.watchdog.Stop()
			svc.mtx.Unlock()
			return
		case <-svc.insertCtx.Done():
			svc.fetchLoopIteration()
		}
	}
}

func (svc *InsertServiceV2) ping() {
	if svc.client == nil {
		return
	}
	if svc.lastRequest.Add(time.Second).After(time.Now()) {
		return
	}
	to, _ := svc.timeoutContext()
	err := svc.client.Ping(to)
	if err != nil {
		svc.client.Close()
		svc.client = nil
		logger.Error(fmt.Sprintf("[IS004]: %v", err))
		return
	}
	svc.lastRequest = time.Now()
}

func (svc *InsertServiceV2) Stop() {
	svc.cancel()
}

type SizeGetter interface {
	GetSize() int64
}

func (svc *InsertServiceV2) Request(req helpers.SizeGetter, insertMode int) *promise.Promise[uint32] {
	// res := req.Response()
	p := promise.New[uint32]()
	if !svc.running {
		logger.Info("service stopped")
		p.Done(0, fmt.Errorf("service stopped"))
		return p
	}
	size := req.GetSize()
	func() {
		var (
			inserted int
			err      error
		)
		svc.mtx.Lock()
		defer svc.mtx.Unlock()

		inserted, svc.columns, err = svc.processRequest(req, svc.columns)

		if err != nil || inserted == 0 {
			p.Done(0, err)
			return
		}
		svc.size += size
		if svc.maxQueueSize > 0 && svc.size > svc.maxQueueSize {
			svc.insertCancel()
		}
		svc.results = append(svc.results, p)
	}()
	return p
}

func (svc *InsertServiceV2) swapBuffers() (*requestPortion, error) {
	svc.mtx.Lock()
	defer svc.mtx.Unlock()
	svc.insertCtx, svc.insertCancel = context.WithTimeout(context.Background(), svc.pushInterval)
	if svc.size == 0 {
		return nil, nil
	}
	columns := svc.columns
	svc.columns = svc.acquireColumns()
	svc.lastSend = time.Now()
	size := svc.size
	svc.size = 0
	results := svc.results
	svc.results = nil
	return &requestPortion{columns, results, size}, nil
}

func (svc *InsertServiceV2) setState(state int) {
	atomic.StoreInt32(&svc.state, int32(state))
}

func (svc *InsertServiceV2) fetchLoopIteration() {
	if svc.client == nil {
		var err error
		svc.client, err = svc.V3Session()
		if err != nil {
			logger.Error("DB Connect error. Reconnect in 1s: ", err)
			time.Sleep(time.Second)
			return
		}
	}

	portion, _ := svc.swapBuffers()
	if portion == nil {
		return
	}

	if svc.OnBeforeInsert != nil {
		svc.OnBeforeInsert()
	}

	waiting := append([]*promise.Promise[uint32]{}, portion.res...)
	releaseWaiting := func(err error) {
		for _, w := range waiting {
			w.Done(0, err)
		}
	}

	input := make(proto.Input, len(portion.cols))
	size := int64(0)
	for i, c := range portion.cols {
		input[i] = c.Input()
		size += int64(svc.IngestSize(&input[i]))
	}

	svc.setState(INSERT_STATE_INSERTING)
	defer svc.setState(INSERT_STATE_IDLE)

	startSending := time.Now()
	lastFlush := time.Now()
	rows := int64(input[0].Data.Rows())

	to, cancel2 := context.WithTimeout(svc.ctx, time.Duration(int64(svc.DatabaseNode.WriteTimeout)*int64(time.Second)))
	defer cancel2()

	err := svc.client.Do(to, fch.Query{
		Body:  svc.insertRequest + " VALUES ",
		Input: input,
	})

	stat.AddCompoundMetric("tx_close_time_ms", time.Since(lastFlush).Milliseconds())
	stat.AddCompoundMetric("send_time_ms", time.Since(startSending).Milliseconds())
	stat.AddSentMetrics(svc.serviceType+"_sent_rows", rows)
	stat.AddSentMetrics(svc.serviceType+"_sent_bytes", size)

	svc.lastRequest = time.Now()
	releaseWaiting(err)

	if err != nil {
		svc.client.Close()
		svc.client = nil
	}
}

func (svc *InsertServiceV2) IngestSize(input *proto.InputColumn) int {
	switch input.Data.(type) {
	case *proto.ColStr:
		return len(input.Data.(*proto.ColStr).Buf)
	case proto.ColUInt64:
		return 8 * input.Data.Rows()
	case proto.ColInt64:
		return 8 * input.Data.Rows()
	case proto.ColDate:
		return 2 * input.Data.Rows()
	case proto.ColFloat64:
		return 8 * input.Data.Rows()
	case *proto.ColFixedStr:
		return len(input.Data.(*proto.ColFixedStr).Buf)
	case proto.ColInt8:
		return input.Data.Rows()
	case proto.ColBool:
		return input.Data.Rows()
	}
	return 0
}

type InsertServiceV2RoundRobin struct {
	ServiceData
	// V3Session    func() (IChClient, error)

	V3Session      chwrapper.IChClientFactory
	DatabaseNode   *model.DataDatabasesMap
	AsyncInsert    bool
	OnBeforeInsert func()
	maxQueueSize   int64
	pushInterval   time.Duration
	serviceType    string

	insertRequest string

	acquireColumns func() []IColPoolRes
	processRequest func(any, []IColPoolRes) (int, []IColPoolRes, error)

	svcNum  int
	running bool

	services []*InsertServiceV2
	rand     *rand.Rand
	mtx      sync.Mutex
}

func (svc *InsertServiceV2RoundRobin) PlanFlush() {
	for _, svc := range svc.services {
		svc.PlanFlush()
	}
}

func (svc *InsertServiceV2RoundRobin) GetNodeName() string {
	return svc.DatabaseNode.Node
}

func (svc *InsertServiceV2RoundRobin) GetState(insertMode int) int {
	var idle bool
	for _, svc := range svc.services {
		switch svc.GetState(insertMode) {
		case INSERT_STATE_INSERTING:
			return INSERT_STATE_INSERTING
		case INSERT_STATE_IDLE:
			idle = true
		}
	}
	if idle {
		return INSERT_STATE_IDLE
	}
	return INSERT_STATE_CLOSING
}

func (svc *InsertServiceV2RoundRobin) Ping() (time.Time, error) {
	minTime := time.Now()
	for _, svc := range svc.services {
		t, err := svc.Ping()
		if err != nil {
			return t, err
		}
		if minTime.After(t) {
			minTime = t
		}
	}
	return minTime, nil
}

func (svc *InsertServiceV2RoundRobin) init() {
	if svc.services != nil {
		return
	}
	logger.Info(fmt.Sprintf("creating %d services", svc.svcNum))
	svc.services = make([]*InsertServiceV2, svc.svcNum)
	svc.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := range svc.services {
		svc.services[i] = &InsertServiceV2{
			ID:             fmt.Sprintf("%s-%s-%v", svc.DatabaseNode.Node, svc.insertRequest, svc.AsyncInsert),
			ServiceData:    ServiceData{},
			V3Session:      svc.V3Session,
			DatabaseNode:   svc.DatabaseNode,
			AsyncInsert:    svc.AsyncInsert,
			OnBeforeInsert: svc.OnBeforeInsert,
			pushInterval:   svc.pushInterval,
			maxQueueSize:   svc.maxQueueSize,
			insertRequest:  svc.insertRequest,
			acquireColumns: svc.acquireColumns,
			processRequest: svc.processRequest,
			serviceType:    svc.serviceType,
		}
		svc.services[i].Init()
	}
}

func (svc *InsertServiceV2RoundRobin) Init() {
	svc.mtx.Lock()
	defer svc.mtx.Unlock()
	svc.init()
}

func (svc *InsertServiceV2RoundRobin) Run() {
	svc.mtx.Lock()
	logger.Info("Running")
	svc.init()
	if svc.running {
		logger.Info("Already running")
		svc.mtx.Unlock()
		return
	}
	wg := sync.WaitGroup{}

	for i := range svc.services {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc.services[i].Run()
		}(i)
	}
	svc.running = true
	svc.mtx.Unlock()
	wg.Wait()
}

func (svc *InsertServiceV2RoundRobin) Stop() {
	for _, _svc := range svc.services {
		_svc.Stop()
	}
}

func (svc *InsertServiceV2RoundRobin) Request(req helpers.SizeGetter, insertMode int) *promise.Promise[uint32] {
	var insertingSvcs []IInsertServiceV2
	var idleSvcs []IInsertServiceV2
	for _, _svc := range svc.services {
		switch _svc.GetState(insertMode) {
		case INSERT_STATE_INSERTING:
			insertingSvcs = append(insertingSvcs, _svc)
		case INSERT_STATE_IDLE:
			idleSvcs = append(idleSvcs, _svc)
		}
	}
	svc.mtx.Lock()
	randomIdx := svc.rand.Float64()
	svc.mtx.Unlock()
	if len(insertingSvcs) > 0 {
		return insertingSvcs[int(randomIdx*float64(len(insertingSvcs)))].Request(req, insertMode)
	} else if len(idleSvcs) > 0 {
		return idleSvcs[int(randomIdx*float64(len(idleSvcs)))].Request(req, insertMode)
	}
	return svc.services[int(randomIdx*float64(len(svc.services)))].Request(req, insertMode)
}

type InsertServiceV2Multimodal struct {
	ServiceData
	// V3Session    func() (IChClient, error)
	V3Session      chwrapper.IChClientFactory
	DatabaseNode   *model.DataDatabasesMap
	AsyncInsert    bool
	PushInterval   time.Duration
	OnBeforeInsert func()
	MaxQueueSize   int64
	ServiceType    string

	InsertRequest string

	AcquireColumns func() []IColPoolRes
	ProcessRequest func(any, []IColPoolRes) (int, []IColPoolRes, error)

	SvcNum int

	SyncService  *InsertServiceV2RoundRobin
	AsyncService *InsertServiceV2RoundRobin
	running      bool
	mtx          sync.Mutex
}

func (svc *InsertServiceV2Multimodal) PlanFlush() {
	svc.SyncService.PlanFlush()
	svc.AsyncService.PlanFlush()
}

func (svc *InsertServiceV2Multimodal) GetNodeName() string {
	return svc.DatabaseNode.Node
}

func (svc *InsertServiceV2Multimodal) GetState(insertMode int) int {
	switch insertMode {
	case INSERT_MODE_SYNC:
		return svc.SyncService.GetState(insertMode)
	case INSERT_MODE_ASYNC:
		return svc.AsyncService.GetState(insertMode)
	}
	if svc.AsyncInsert {
		return svc.AsyncService.GetState(insertMode)
	}
	return svc.SyncService.GetState(insertMode)
}

func (svc *InsertServiceV2Multimodal) init() {
	if svc.SyncService != nil {
		return
	}
	logger.Info(fmt.Sprintf("creating %d services", svc.SvcNum))
	svc.SyncService = &InsertServiceV2RoundRobin{
		ServiceData:    ServiceData{},
		V3Session:      svc.V3Session,
		DatabaseNode:   svc.DatabaseNode,
		AsyncInsert:    false,
		OnBeforeInsert: svc.OnBeforeInsert,
		pushInterval:   svc.PushInterval,
		maxQueueSize:   svc.MaxQueueSize,
		insertRequest:  svc.InsertRequest,
		acquireColumns: svc.AcquireColumns,
		processRequest: svc.ProcessRequest,
		svcNum:         svc.SvcNum,
		serviceType:    svc.ServiceType,
	}
	svc.SyncService.Init()
	svc.AsyncService = &InsertServiceV2RoundRobin{
		ServiceData:    ServiceData{},
		V3Session:      svc.V3Session,
		DatabaseNode:   svc.DatabaseNode,
		AsyncInsert:    true,
		OnBeforeInsert: svc.OnBeforeInsert,
		pushInterval:   svc.PushInterval,
		maxQueueSize:   svc.MaxQueueSize,
		insertRequest:  svc.InsertRequest,
		acquireColumns: svc.AcquireColumns,
		processRequest: svc.ProcessRequest,
		svcNum:         svc.SvcNum,
		serviceType:    svc.ServiceType,
	}
	svc.AsyncService.Init()
}

func (svc *InsertServiceV2Multimodal) Init() {
	svc.mtx.Lock()
	defer svc.mtx.Unlock()
	svc.init()
}

func (svc *InsertServiceV2Multimodal) Run() {
	svc.mtx.Lock()
	logger.Info("Running")
	svc.init()
	if svc.running {
		svc.mtx.Unlock()
		return
	}
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.SyncService.Run()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		svc.AsyncService.Run()
	}()
	logger.Info("created service")
	svc.running = true
	svc.mtx.Unlock()
	wg.Wait()
}

func (svc *InsertServiceV2Multimodal) Stop() {
	svc.SyncService.Stop()
	svc.AsyncService.Stop()
}

func (svc *InsertServiceV2Multimodal) Request(req helpers.SizeGetter, insertMode int) *promise.Promise[uint32] {
	switch insertMode {
	case INSERT_MODE_SYNC:
		return svc.SyncService.Request(req, insertMode)
	case INSERT_MODE_ASYNC:
		return svc.AsyncService.Request(req, insertMode)
	}
	if svc.AsyncInsert {
		return svc.SyncService.Request(req, insertMode)
	} else {
		return svc.SyncService.Request(req, insertMode)
	}
}

func (svc *InsertServiceV2Multimodal) Ping() (time.Time, error) {
	minTime := time.Now()
	for _, svc := range []IInsertServiceV2{svc.SyncService, svc.AsyncService} {
		t, err := svc.Ping()
		if err != nil {
			return t, err
		}
		if minTime.After(t) {
			minTime = t
		}
	}
	return minTime, nil
}
