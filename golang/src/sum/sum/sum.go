package sum

import (
	"fmt"
	"log/slog"
	"maps"
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
	eofExchange    middleware.Middleware
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[uint32]map[string]fruititem.FruitItem
}

const eofExchangeName = "eof_exchange"

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

	eofExchangeRouteKey := fmt.Sprintf("%s_%s", config.SumPrefix, eofExchangeName)

	eofExchange, err := middleware.CreateExchangeMiddleware(config.SumPrefix, []string{eofExchangeRouteKey}, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	return &Sum{
		id:             config.Id,
		mutex:          sync.Mutex{},
		inputQueue:     inputQueue,
		eofExchange:    eofExchange,
		outputExchange: outputExchange,
		fruitItemMap:   map[uint32]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go func() {
		sum.eofExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleMessage(msg, ack, nack, false)
		})
	}()

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack, true)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func(), fromGateway bool) {
	defer ack()

	fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	header := fruitRecords[0]
	clientId := header.Amount

	if header.Fruit == "EOF" {

		// Solo aviso si es un EOF que viene del gateway, si es de otro sum no aviso
		if fromGateway {
			if err := sum.NotifyEndOfRecords(clientId); err != nil {
				slog.Debug("While notifying end of records to other sums", "err", err)
			}
		} else {
			senderId := fruitRecords[1].Amount

			if senderId == uint32(sum.id) {
				slog.Debug("Received EOF message from myself, ignoring", "clientId", clientId)
				return
			}
		}

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
	slog.Info("Received End Of Records message", "clientId", clientId)

	sum.mutex.Lock()
	if _, ok := sum.fruitItemMap[clientId]; !ok {
		// Si no existe el mapa para el cliente, lo creo vacio
		sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}
	mapClone := maps.Clone(sum.fruitItemMap[clientId])
	sum.mutex.Unlock()

	for key := range mapClone {
		header := fruititem.FruitItem{
			Fruit:  "",
			Amount: clientId,
		}

		fruitRecord := []fruititem.FruitItem{header, mapClone[key]}
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

	slog.Info("Sending EOF message to aggregation")

	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	// Vacio el mapa del cliente terminado
	sum.mutex.Lock()
	sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	sum.mutex.Unlock()

	return nil
}

func (sum *Sum) NotifyEndOfRecords(clientId uint32) error {
	slog.Info("Notifying end of records to other sums", "clientId", clientId)

	header := fruititem.FruitItem{
		Fruit:  "EOF",
		Amount: clientId,
	}
	senderId := fruititem.FruitItem{
		Fruit:  "SENDER_ID",
		Amount: uint32(sum.id),
	}

	eofMessage := []fruititem.FruitItem{header, senderId}
	message, err := inner.SerializeMessage(eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message to other sums", "err", err)
		return err
	}
	if err := sum.eofExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message to other sums", "err", err)
		return err
	}
	return nil
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
