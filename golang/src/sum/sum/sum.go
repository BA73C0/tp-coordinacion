package sum

import (
	"fmt"
	"log/slog"

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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[uint32]map[string]fruititem.FruitItem
}

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

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[uint32]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
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

	header := fruitRecords[0]
	clientId := header.Amount

	if header.Fruit == "EOF" {
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords[1:]); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId uint32) error {
	slog.Info("Received End Of Records message")
	for key := range sum.fruitItemMap[clientId] {
		header := fruititem.FruitItem{
			Fruit:  "",
			Amount: clientId,
		}

		fruitRecord := []fruititem.FruitItem{header, sum.fruitItemMap[clientId][key]}
		message, err := inner.SerializeMessage(fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	header := fruititem.FruitItem{
		Fruit:  "EOF",
		Amount: clientId,
	}
	eofMessage := []fruititem.FruitItem{header}
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	// Vacio el mapa del cliente terminado
	sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}

	return nil
}

func (sum *Sum) handleDataMessage(clientId uint32, fruitRecords []fruititem.FruitItem) error {
	if _, ok := sum.fruitItemMap[clientId]; !ok {
		sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMap[clientId][fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[clientId][fruitRecord.Fruit] = sum.fruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
