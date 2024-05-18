package tranformer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Provider struct {
	Writer   *kafka.Writer
	Topic    string
	Address  string
	user     string
	password string
}

// NewProvider generates a new kafka provider allowing for writes to a topic
func NewProvider(address, topic, user, pw string) (*Provider, error) {
	mechanism, _ := scram.Mechanism(scram.SHA256, user, pw)
	w := kafka.Writer{
		Addr:  kafka.TCP(address),
		Topic: topic,
		Transport: &kafka.Transport{
			SASL: mechanism,
			TLS:  &tls.Config{},
		},
	}

	return &Provider{
		Writer:   &w,
		Topic:    topic,
		Address:  address,
		user:     user,
		password: pw,
	}, nil
}

// WriteMessage allows writing to topic defined in the Provider constructor.
func (p *Provider) WriteMessage(ctx context.Context, msgBody []byte, reportType string) error {
	if msgBody == nil {
		return errors.New("msbBody cannot be nil")
	}

	header := []kafka.Header{{
		Key:   "reportType",
		Value: []byte(reportType),
	}}

	err := p.Writer.WriteMessages(ctx, kafka.Message{Value: msgBody, Headers: header})
	if err != nil {
		return fmt.Errorf("failed to write message to topic %s: %w", p.Topic, err)
	}
	return nil
}
