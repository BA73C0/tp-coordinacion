package middleware

import (
	"errors"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const AMQPDefaultUser = "guest"
const AMQPDefaultPass = "guest"
const AMQPURI = "amqp://" + AMQPDefaultUser + ":" + AMQPDefaultPass + "@"

type MiddlewareImplementation struct {
	ch          *amqp.Channel
	queue       amqp.Queue
	consumerTag string
	exchange    string
	keys        []string
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

	q, _ := ch.QueueDeclare(
		queueName, // queue name
		true,      // durable
		false,     // auto-delete
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)

	return MiddlewareImplementation{
		ch:       ch,
		queue:    q,
		exchange: "",
		keys:     []string{""},
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

	ch.ExchangeDeclare(
		exchange, // name
		"direct", // kind -- "direct", "fanout", "topic" or "headers"
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	q, _ := ch.QueueDeclare(
		"",    // queue name
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // noWait
		nil,   // arguments
	)

	for _, key := range keys {
		ch.QueueBind(
			q.Name,   // queue name
			key,      // routing key
			exchange, // exchange name
			false,    // noWait
			nil,      // arguments
		)
	}

	return MiddlewareImplementation{
		ch:       ch,
		queue:    q,
		exchange: exchange,
		keys:     keys,
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
	messages, err := q.ch.Consume(
		q.queue.Name, // queue name
		"",           // consumer tag
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
		if q.consumerTag == "" {
			q.consumerTag = msg.ConsumerTag
		}

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
	err := q.ch.Cancel(
		q.consumerTag, // consumer tag
		false,         // noWait
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
			err := q.ch.QueueUnbind(
				q.queue.Name, // queue name
				key,          // routing key
				q.exchange,   // exchange name
				nil,          // arguments
			)

			if errors.Is(err, amqp.ErrClosed) {
				return ErrMessageMiddlewareDisconnected
			} else if err != nil {
				return ErrMessageMiddlewareClose
			}
		}
	}

	err := q.ch.Close()

	if errors.Is(err, amqp.ErrClosed) {
		return ErrMessageMiddlewareDisconnected
	} else if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return nil
}
