package sum

import (
	"fmt"
	"log/slog"
	"sync"

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
	id             int
	mutex          sync.Mutex
	sumPrefix      string
	sumAmount      int
	coordExchange  middleware.Middleware
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	counterAck     map[uint32]uint32
	counterClient  map[uint32]uint32
	lastClientMsg  map[uint32]uint32
	fruitItemMap   map[uint32]map[string]fruititem.FruitItem
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

	return &Sum{
		id:             config.Id,
		mutex:          sync.Mutex{},
		sumPrefix:      config.SumPrefix,
		sumAmount:      config.SumAmount,
		inputQueue:     inputQueue,
		coordExchange:  coordExchange,
		outputExchange: outputExchange,
		counterAck:     map[uint32]uint32{},
		counterClient:  map[uint32]uint32{},
		lastClientMsg:  map[uint32]uint32{},
		fruitItemMap:   map[uint32]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		sum.coordExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleCoordinationMessage(msg, ack, nack)
		})
	}()

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
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
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords[ClientIdPos+1:]); err != nil {
		slog.Error("While handling data message", "err", err)
	}
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
