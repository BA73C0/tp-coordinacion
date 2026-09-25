package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var ops atomic.Uint32

type MessageHandler struct {
	id      uint32
	counter uint32
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		id:      ops.Add(1),
		counter: 0,
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	client := fruititem.FruitItem{
		Fruit:  "CLIENT_ID",
		Amount: messageHandler.id,
	}
	msgId := fruititem.FruitItem{
		Fruit:  "MSG_ID",
		Amount: messageHandler.counter,
	}
	messageHandler.counter++

	data := []fruititem.FruitItem{client, msgId, fruitRecord}
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	header := fruititem.FruitItem{
		Fruit:  "EOF",
		Amount: messageHandler.id,
	}
	msgId := fruititem.FruitItem{
		Fruit:  "MSG_ID",
		Amount: messageHandler.counter,
	}
	messageHandler.counter++

	data := []fruititem.FruitItem{header, msgId}
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	return fruitRecords, nil
}
