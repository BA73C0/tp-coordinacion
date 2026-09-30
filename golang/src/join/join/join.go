package join

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	running           atomic.Bool
	routines          sync.WaitGroup
	topSize           int
	eofByClient       map[uint32]int
	aggregationAmount int
	inputQueue        middleware.Middleware
	outputQueue       middleware.Middleware
	topFruitPerClient map[uint32]*Heap
}

const HeaderPos = 0

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	join := &Join{
		routines:          sync.WaitGroup{},
		topSize:           config.TopSize,
		eofByClient:       map[uint32]int{},
		aggregationAmount: config.AggregationAmount,
		inputQueue:        inputQueue,
		outputQueue:       outputQueue,
		topFruitPerClient: make(map[uint32]*Heap),
	}
	join.running.Store(true)
	return join, nil
}

func (join *Join) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)

	join.routines.Add(1)
	go func() {
		defer join.routines.Add(-1)
		err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			join.handleMessage(msg, ack, nack)
		})
		if err != nil && (join.running.Load() || (!join.running.Load() && !errors.Is(err, middleware.ErrMessageMiddlewareDisconnected))) {
			slog.Error("While starting input queue consumption", "err", err)
		}
	}()

	slog.Info("Join is running")
	<-signals
	join.Close()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	header := fruitRecords[HeaderPos]
	clientId := header.Amount

	if header.Fruit == "EOF" {
		join.eofByClient[clientId]++

		if join.eofByClient[clientId] == join.aggregationAmount {
			if err := join.handleEndOfRecordsMessage(clientId); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
		}

		return
	}

	join.handleDataMessage(clientId, fruitRecords[HeaderPos+1:])
}

func (join *Join) handleDataMessage(clientId uint32, fruitItems []fruititem.FruitItem) {
	slog.Info("Handling data message", "clientId", clientId, "fruitItems", fruitItems)

	if _, exists := join.topFruitPerClient[clientId]; !exists {
		join.topFruitPerClient[clientId] = NewHeap()
	}

	for _, item := range fruitItems {
		join.topFruitPerClient[clientId].Push(item)
	}
}

func (join *Join) handleEndOfRecordsMessage(clientId uint32) error {
	slog.Info("Handling end of records message", "clientId", clientId)

	topFruitItems := []fruititem.FruitItem{}

	for i := 0; i < join.topSize; i++ {
		if join.topFruitPerClient[clientId].IsEmpty() {
			break
		}

		item, err := join.topFruitPerClient[clientId].Pop()
		if err != nil {
			return err
		}
		topFruitItems = append(topFruitItems, item)
	}

	message, err := inner.SerializeMessage(topFruitItems)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (join *Join) Close() {
	slog.Info("Closing join")
	join.running.Store(false)

	err := join.inputQueue.StopConsuming()
	if err != nil && !errors.Is(err, middleware.ErrMessageMiddlewareDisconnected) {
		slog.Error("While stopping input queue consumption", "err", err)
	}

	// Espero a que terminen de procesar los mensajes que estaban en curso
	join.routines.Wait()

	err = join.inputQueue.Close()
	if err != nil && !errors.Is(err, middleware.ErrMessageMiddlewareDisconnected) {
		slog.Error("While closing input queue", "err", err)
	}
	err = join.outputQueue.Close()
	if err != nil && !errors.Is(err, middleware.ErrMessageMiddlewareDisconnected) {
		slog.Error("While closing coordination exchange", "err", err)
	}
}
