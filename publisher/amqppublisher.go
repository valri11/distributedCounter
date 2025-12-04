package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/gofrs/uuid/v5"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/valri11/distributedcounter/types"
)

type amqpPublisher struct {
	url          string
	exchangeName string

	conn    *amqp.Connection
	msgChan *amqp.Channel
}

func NewAmqpPublisher(baseUrl string, params map[string]string) (*amqpPublisher, error) {
	p := amqpPublisher{}

	vhost := params["vhost"]
	exchangeName := params["exchangename"]
	exchangeType := "headers"
	userName := params["user"]
	userPassword := params["password"]

	// connect
	config := amqp.Config{
		Vhost:      vhost,
		Properties: amqp.NewConnectionProperties(),
	}

	// Parse the base URL
	brokerURL, err := url.Parse(baseUrl)
	if err != nil {
		return nil, fmt.Errorf("Error parsing URL: %v\n", err)
	}

	// Create a UserInfo object with the username and password
	brokerURL.User = url.UserPassword(userName, userPassword)

	p.url = brokerURL.String()

	conn, err := amqp.DialConfig(p.url, config)
	if err != nil {
		return nil, fmt.Errorf("dial AMQP endpoint %s (vhost: %s): %v", p.url, vhost, err)
	}
	p.conn = conn

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open AMQP channel: %v", err)
	}
	// set channel to Confirm mode
	err = ch.Confirm(false)
	if err != nil {
		return nil, fmt.Errorf("set channel to Confirm mode: %v", err)
	}

	p.msgChan = ch

	err = ch.ExchangeDeclare(
		exchangeName, // name
		exchangeType, // type
		true,         // durable
		false,        // auto-delete
		false,        // internal
		false,        // noWait
		nil,          // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("declare exchange: %v", err)
	}
	p.exchangeName = exchangeName

	return &p, nil
}

func (p *amqpPublisher) ReportUsage(ctx context.Context, resUsage []types.AccountUsage) error {
	jsonData, err := json.Marshal(resUsage)
	if err != nil {
		return err
	}

	conf, err := p.msgChan.PublishWithDeferredConfirmWithContext(ctx,
		p.exchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"Type":      "ResourceUsage",
				"Version":   "1.0",
				"Status":    "published",
				"Namespace": "tiles",
			},
			ContentType:     "application/json",
			ContentEncoding: "UTF-8",
			DeliveryMode:    amqp.Persistent,
			Priority:        0,
			MessageId:       uuid.Must(uuid.NewV4()).String(),
			AppId:           "resourceserver",
			Body:            jsonData,
		},
	)
	if err != nil {
		return fmt.Errorf("publish message: %v", err)
	}

	confirmed, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("message confirmation: %v", err)
	}
	if !confirmed {
		return fmt.Errorf("message is not confirmed")
	}

	return nil
}
