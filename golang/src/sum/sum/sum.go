package sum

import (
	"errors"
	"fmt"
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

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	running         atomic.Bool
	routines        sync.WaitGroup
	id              int
	mutex           sync.Mutex
	sumPrefix       string
	sumAmount       int
	coordExchange   middleware.Middleware
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	aggregationKeys []string
	counterAck      map[uint32]uint32
	counterClient   map[uint32]uint32
	lastClientMsg   map[uint32]uint32
	fruitItemMap    map[uint32]map[string]fruititem.FruitItem
}

const coordExchangeName = "sum_coord_exchange"

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	coordExchangeRouteKey := fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)
	coordExchange, err := middleware.CreateExchangeMiddleware(coordExchangeName, []string{coordExchangeRouteKey}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	sum := &Sum{
		id:              config.Id,
		routines:        sync.WaitGroup{},
		mutex:           sync.Mutex{},
		sumPrefix:       config.SumPrefix,
		sumAmount:       config.SumAmount,
		inputQueue:      inputQueue,
		coordExchange:   coordExchange,
		outputExchange:  outputExchange,
		aggregationKeys: outputExchangeRouteKeys,
		counterAck:      map[uint32]uint32{},
		counterClient:   map[uint32]uint32{},
		lastClientMsg:   map[uint32]uint32{},
		fruitItemMap:    map[uint32]map[string]fruititem.FruitItem{},
	}
	sum.running.Store(true)
	return sum, nil
}

func (sum *Sum) Run() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)

	sum.routines.Add(1)
	go func() {
		defer sum.routines.Add(-1)
		err := sum.coordExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleCoordinationMessage(msg, ack, nack)
		})
		if sum.shouldNotifyError(err) {
			slog.Error("While starting coordination exchange consumption", "err", err)
		}
	}()

	sum.routines.Add(1)
	go func() {
		defer sum.routines.Add(-1)
		err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleMessage(msg, ack, nack)
		})
		if sum.shouldNotifyError(err) {
			slog.Error("While starting input queue consumption", "err", err)
		}
	}()

	slog.Info("Sum is running", "id", sum.id)
	<-signals
	sum.Close()
}

func (sum *Sum) Close() {
	slog.Info("Closing sum")
	sum.running.Store(false)

	err := sum.inputQueue.StopConsuming()
	if sum.shouldNotifyError(err) {
		slog.Error("While stopping input queue consumption", "err", err)
	}
	err = sum.coordExchange.StopConsuming()
	if sum.shouldNotifyError(err) {
		slog.Error("While stopping coordination exchange consumption", "err", err)
	}

	// Espero a que terminen de procesar los mensajes que estaban en curso
	sum.routines.Wait()

	err = sum.inputQueue.Close()
	if sum.shouldNotifyError(err) {
		slog.Error("While closing input queue", "err", err)
	}
	err = sum.coordExchange.Close()
	if sum.shouldNotifyError(err) {
		slog.Error("While closing coordination exchange", "err", err)
	}
	err = sum.outputExchange.Close()
	if sum.shouldNotifyError(err) {
		slog.Error("While closing output exchange", "err", err)
	}
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	header := fruitRecords[HeaderPos]
	clientId := header.Amount

	msgId := fruitRecords[ClientIdPos].Amount

	sum.mutex.Lock()
	if header.Fruit != "EOF" {
		sum.counterClient[clientId]++
	}
	sum.lastClientMsg[clientId] = msgId
	sum.mutex.Unlock()

	if header.Fruit == "EOF" {
		if err := sum.coordinateEndOfRecords(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
		ack()
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords[ClientIdPos+1:]); err != nil {
		slog.Error("While handling data message", "err", err)
		nack()
		return
	}

	ack()
}

func (sum *Sum) handleDataMessage(clientId uint32, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	if _, ok := sum.fruitItemMap[clientId]; !ok {
		sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}
	sum.mutex.Unlock()

	for _, fruitRecord := range fruitRecords {
		sum.mutex.Lock()
		_, ok := sum.fruitItemMap[clientId][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[clientId][fruitRecord.Fruit] = sum.fruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
		sum.mutex.Unlock()
	}
	return nil
}

func (sum *Sum) shouldNotifyError(err error) bool {
	return err != nil && (sum.running.Load() || (!sum.running.Load() && !errors.Is(err, middleware.ErrMessageMiddlewareDisconnected)))
}
