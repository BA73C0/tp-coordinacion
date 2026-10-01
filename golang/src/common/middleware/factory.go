package middleware

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const AMQPDefaultUser = "guest"
const AMQPDefaultPass = "guest"
const AMQPURI = "amqp://" + AMQPDefaultUser + ":" + AMQPDefaultPass + "@"

type MiddlewareImplementation struct {
	consuming *atomic.Bool
	conn      *amqp.Connection
	ch        *amqp.Channel
	queue     amqp.Queue
	exchange  string
	keys      []string
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial(AMQPURI + connectionSettings.Hostname + ":" + strconv.Itoa(connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // auto-delete
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)

	if err != nil {
		ch.Close()
		return nil, err
	}

	return MiddlewareImplementation{
		consuming: &atomic.Bool{},
		conn:      conn,
		ch:        ch,
		queue:     q,
		exchange:  "",
		keys:      []string{""},
	}, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, err := amqp.Dial(AMQPURI + connectionSettings.Hostname + ":" + strconv.Itoa(connectionSettings.Port))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // kind -- "direct", "fanout", "topic" or "headers"
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		"",    // queue name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)

	if err != nil {
		ch.Close()
		return nil, err
	}

	for i, key := range keys {
		err = ch.QueueBind(
			q.Name,   // queue name
			key,      // routing key
			exchange, // exchange name
			false,    // noWait
			nil,      // arguments
		)

		if err != nil {
			for j := range i {
				_ = UnbindQueueFromExchange(exchange, keys[j], ch, q.Name)
			}

			ch.Close()
			return nil, err
		}
	}

	return MiddlewareImplementation{
		consuming: &atomic.Bool{},
		conn:      conn,
		ch:        ch,
		queue:     q,
		exchange:  exchange,
		keys:      keys,
	}, nil
}

// Comienza a escuchar a la cola/exchange e invoca a callbackFunc tras
// cada mensaje de datos o de control con el cuerpo del mensaje.
// callbackFunc tiene como parámetro:
// msg - El struct tal y como lo recibe el método Send.
// ack - Una función que hace ACK del mensaje recibido.
// nack - Una función que hace NACK del mensaje recibido.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q MiddlewareImplementation) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if !q.consuming.CompareAndSwap(false, true) {
		return ErrMessageMiddlewareMessage
	}

	messages, err := q.ch.Consume(
		q.queue.Name, // queue name
		q.queue.Name, // consumer tag
		false,        // auto-ack
		false,        // exclusive
		false,        // noLocal
		false,        // noWait
		nil,          // arguments
	)

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareMessage
	}

	for msg := range messages {
		callbackFunc(
			Message{
				Body: string(msg.Body),
			},
			func() {
				err = msg.Ack(
					false, // multiple
				)
			},
			func() {
				err = msg.Nack(
					false, // multiple
					false, // requeue
				)
			},
		)

		if errors.Is(err, amqp.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		} else if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	return nil
}

// Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
// no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
func (q MiddlewareImplementation) StopConsuming() error {
	if !q.consuming.CompareAndSwap(true, false) {
		return nil
	}

	err := q.ch.Cancel(
		q.queue.Name, // consumer tag
		false,        // noWait
	)

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return nil
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q MiddlewareImplementation) Send(msg Message) error {
	for _, key := range q.keys {
		// Cuando se publica en el exchange default ("")
		// hay que usar el nombre de la cola como routing key
		key_name := key
		if key_name == "" {
			key_name = q.queue.Name
		}

		err := q.ch.Publish(
			q.exchange, // exchange
			key_name,   // key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Transient,
				Timestamp:    time.Now(),
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
			},
		)

		if errors.Is(err, amqp.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		} else if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	return nil
}

// Envía un mensaje a la cola o a los tópicos con el que se inicializó el exchange.
// Si se pierde la conexión con el middleware devuelve ErrMessageMiddlewareDisconnected.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareMessage.
func (q MiddlewareImplementation) SendTo(msg Message, routeKey string) error {
	err := q.ch.Publish(
		q.exchange, // exchange
		routeKey,   // key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Transient,
			Timestamp:    time.Now(),
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		},
	)
	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return nil
}

// Se desconecta de la cola o exchange al que estaba conectado.
// Si ocurre un error interno que no puede resolverse devuelve ErrMessageMiddlewareClose.
func (q MiddlewareImplementation) Close() error {
	if q.exchange != "" {
		for _, key := range q.keys {
			err := UnbindQueueFromExchange(q.exchange, key, q.ch, q.queue.Name)
			if err != nil {
				return err
			}
		}

		err := q.ch.ExchangeDelete(
			q.exchange, // exchange name
			false,      // ifUnused
			false,      // noWait
		)

		if errors.Is(err, amqp.ErrClosed) {
			return ErrMessageMiddlewareDisconnected
		} else if err != nil {
			return ErrMessageMiddlewareClose
		}
	}

	err := q.ch.Close()

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareClose
	}

	err = q.conn.Close()
	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}

func UnbindQueueFromExchange(exchange string, key string, ch *amqp.Channel, queueName string) error {
	err := ch.QueueUnbind(
		queueName, // queue name
		key,       // routing key
		exchange,  // exchange name
		nil,       // arguments
	)

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareClose
	}

	_, err = ch.QueueDelete(
		queueName, // queue name
		true,      // if unused
		false,     // if empty
		false,     // noWait
	)

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}
