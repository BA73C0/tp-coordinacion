package messagehandler

import (
	"sync/atomic"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

var ops atomic.Uint32

type MessageHandler struct {
	id uint32
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		id: ops.Add(1),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	client := fruititem.FruitItem{
		Fruit:  "CLIENT",
		Amount: messageHandler.id,
	}

	data := []fruititem.FruitItem{client, fruitRecord}
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	header := fruititem.FruitItem{
		Fruit:  "EOF",
		Amount: messageHandler.id,
	}
	data := []fruititem.FruitItem{header}
	return inner.SerializeMessage(data)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	fruitRecords, _, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	return fruitRecords, nil
}
