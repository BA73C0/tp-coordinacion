package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	sumAmount     int
	eofByClient   map[uint32]int
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[uint32]map[string]fruititem.FruitItem
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		eofByClient:   map[uint32]int{},
		sumAmount:     config.SumAmount,
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[uint32]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	header := fruitRecords[0]
	clientId := header.Amount

	if header.Fruit == "EOF" {
		aggregation.eofByClient[clientId]++

		if aggregation.eofByClient[clientId] == aggregation.sumAmount {
			if err := aggregation.handleEndOfRecordsMessage(clientId); err != nil {
				slog.Error("While handling end of record message", "err", err)
			}
		}

		return
	}

	aggregation.handleDataMessage(clientId, fruitRecords[1:])
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientId uint32) error {
	slog.Info("Received End Of Records message", "clientId", clientId)

	fruitTopRecords := aggregation.buildFruitTop(clientId)
	message, err := inner.SerializeMessage(fruitTopRecords)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	eofMessage := []fruititem.FruitItem{}
	message, err = inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientId uint32, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.fruitItemMap[clientId]; !ok {
		aggregation.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}

	slog.Info("Received data message", "clientId", clientId, "fruitRecords", fruitRecords)

	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.fruitItemMap[clientId][fruitRecord.Fruit]; ok {
			aggregation.fruitItemMap[clientId][fruitRecord.Fruit] = aggregation.fruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.fruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientId uint32) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(aggregation.fruitItemMap[clientId]))
	for _, item := range aggregation.fruitItemMap[clientId] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
