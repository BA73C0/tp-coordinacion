package sum

import (
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const SleepTime = 1 // seconds

type CoordinationMsgTypes string
type CoordinationMsgPos int

const (
	ReceivedEOF  CoordinationMsgTypes = "ReceivedEOF"
	Ack          CoordinationMsgTypes = "Ack"
	Confirmed    CoordinationMsgTypes = "Confirmed"
	ClientId     CoordinationMsgTypes = "ClientId"
	ReceivedMsgs CoordinationMsgTypes = "ReceivedMsgs"
	SenderId     CoordinationMsgTypes = "SenderId"
)

const (
	HeaderPos       CoordinationMsgPos = 0
	ClientIdPos     CoordinationMsgPos = 1
	ReceivedMsgsPos CoordinationMsgPos = 2
)

func sendMsg(mom middleware.Middleware, routeKeys []string, msg []fruititem.FruitItem) error {
	message, err := inner.SerializeMessage(msg)
	if err != nil {
		slog.Debug("While serializing message", "err", err)
		return err
	}

	for _, routeKey := range routeKeys {
		if err := mom.SendTo(*message, routeKey); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	return nil
}

func (sum *Sum) coordinateEndOfRecords(clientId uint32) error {

	if sum.sumAmount == 1 {
		slog.Info("Only one sum, no need to coordinate end of records", "clientId", clientId)
		return sum.handleConfirmedCoordMsg(clientId)
	}

	slog.Info("Notifying end of records to other sums", "clientId", clientId)

	header := fruititem.FruitItem{
		Fruit:  string(ReceivedEOF),
		Amount: uint32(sum.id),
	}
	senderId := fruititem.FruitItem{
		Fruit:  string(ClientId),
		Amount: clientId,
	}

	keys := make([]string, sum.sumAmount-1)
	for i := 0; i < sum.sumAmount; i++ {
		if i == sum.id {
			continue
		}
		keys = append(keys, fmt.Sprintf("%s_%d", sum.sumPrefix, i))
	}

	received := []fruititem.FruitItem{header, senderId}
	if err := sendMsg(sum.coordExchange, keys, received); err != nil {
		slog.Debug("While sending EOF message to other sums", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleCoordinationMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	fruitRecords, _, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing coordination message", "err", err)
		return
	}

	header := fruitRecords[HeaderPos]
	msgType := header.Fruit
	coordinatorId := header.Amount

	switch msgType {
	case string(ReceivedEOF):
		sum.handleReceivedCoordMsg(fruitRecords, coordinatorId)
	case string(Ack):
		sum.handleAckCoordMsg(fruitRecords, coordinatorId)
	case string(Confirmed):
		clientId := fruitRecords[ClientIdPos].Amount
		sum.handleConfirmedCoordMsg(clientId)
	default:
		slog.Debug("Unknown coordination message type", "type", msgType)
	}
}

func (sum *Sum) handleReceivedCoordMsg(msgs []fruititem.FruitItem, coordinatorId uint32) error {
	clientId := msgs[ClientIdPos].Amount

	slog.Info("Received end of records notification", "coordinatorId", coordinatorId, "clientId", clientId)

	header := fruititem.FruitItem{
		Fruit:  string(Ack),
		Amount: uint32(coordinatorId),
	}
	clientIdMsg := fruititem.FruitItem{
		Fruit:  string(ClientId),
		Amount: clientId,
	}

	sum.mutex.Lock()
	counter := sum.counterClient[clientId]
	sum.counterClient[clientId] = 0
	sum.mutex.Unlock()

	msgsReceived := fruititem.FruitItem{
		Fruit:  string(ReceivedMsgs),
		Amount: counter,
	}

	keys := []string{fmt.Sprintf("%s_%d", sum.sumPrefix, coordinatorId)}

	ack := []fruititem.FruitItem{header, clientIdMsg, msgsReceived}
	if err := sendMsg(sum.coordExchange, keys, ack); err != nil {
		slog.Debug("While sending acknowledgment message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleAckCoordMsg(msgs []fruititem.FruitItem, coordinatorId uint32) error {
	clientId := msgs[ClientIdPos].Amount

	slog.Info("Received acknowledgment", "coordinatorId", coordinatorId, "clientId", clientId)

	sum.mutex.Lock()
	sum.counterAck[clientId]++
	sum.counterClient[clientId] += msgs[ReceivedMsgsPos].Amount
	counter := sum.counterClient[clientId]
	lastMsgId := sum.lastClientMsg[clientId]
	sum.mutex.Unlock()

	if lastMsgId == counter {
		slog.Info("All messages from client have been acknowledged", "clientId", clientId)

		header := fruititem.FruitItem{
			Fruit:  string(Confirmed),
			Amount: uint32(sum.id),
		}
		clientIdMsg := fruititem.FruitItem{
			Fruit:  string(ClientId),
			Amount: clientId,
		}

		keys := make([]string, sum.sumAmount)
		for i := 0; i < sum.sumAmount; i++ {
			if i == sum.id {
				continue
			}
			keys = append(keys, fmt.Sprintf("%s_%d", sum.sumPrefix, i))
		}

		confirmed := []fruititem.FruitItem{header, clientIdMsg}
		if err := sendMsg(sum.coordExchange, keys, confirmed); err != nil {
			slog.Debug("While sending confirmation message", "err", err)
			return err
		}

		sum.handleConfirmedCoordMsg(clientId)

	} else {
		slog.Info("Not all messages from client have been acknowledged", "clientId", clientId, "acknowledged", counter, "lastMsgId", lastMsgId)

		if sum.counterAck[clientId] != uint32(sum.sumAmount-1) {
			return nil
		}

		go func() {
			time.Sleep(SleepTime * time.Second)
			sum.coordinateEndOfRecords(clientId)
		}()
	}

	return nil
}

func (sum *Sum) handleConfirmedCoordMsg(clientId uint32) error {
	slog.Info("Received confirmation", "clientId", clientId)

	sum.mutex.Lock()
	mapClone := maps.Clone(sum.fruitItemMap[clientId])
	sum.mutex.Unlock()

	keys := []string{"aggregation_0"}

	for key := range mapClone {
		header := fruititem.FruitItem{
			Fruit:  string(ClientId),
			Amount: clientId,
		}

		fruitRecord := []fruititem.FruitItem{header, mapClone[key]}
		if err := sendMsg(sum.outputExchange, keys, fruitRecord); err != nil {
			slog.Debug("While sending fruits to aggregation", "err", err)
			return err
		}
	}

	header := fruititem.FruitItem{
		Fruit:  "EOF",
		Amount: clientId,
	}

	eofMessage := []fruititem.FruitItem{header}
	if err := sendMsg(sum.outputExchange, keys, eofMessage); err != nil {
		slog.Debug("While sending EOF message to aggregation", "err", err)
		return err
	}

	// Vacio el mapa del cliente terminado
	sum.mutex.Lock()
	sum.fruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	sum.mutex.Unlock()

	return nil
}
