package tranformer

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Provider struct {
	Writer   *kafka.Writer
	Topic    string
	Address  string
	user     string
	password string
	logger   *slog.Logger
}

// NewProvider generates a new kafka provider allowing for writes to a topic
func NewProvider(address, topic, user, pw string, logger *slog.Logger) (*Provider, error) {
	mechanism, err := scram.Mechanism(scram.SHA256, user, pw)
	if err != nil {
		return nil, fmt.Errorf("failed to create scram.Mechanism for auth: %w", err)
	}
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
		logger:   logger,
	}, nil
}

// WriteMessage allows writing to topic defined in the Provider constructor.
func (p *Provider) WriteMessage(ctx context.Context, msgBody []byte, reportType string) error {
	if msgBody == nil {
		p.logger.Debug("msg body is nil in WriteMessage", reportType)
		return errors.New("msgBody cannot be nil")
	}

	header := []kafka.Header{{
		Key:   "reportType",
		Value: []byte(reportType),
	}}

	p.logger.Debug("writing message", reportType)
	err := p.Writer.WriteMessages(ctx, kafka.Message{Value: msgBody, Headers: header})
	if err != nil {
		p.logger.Debug("WriteMessages failed", reportType)
		return fmt.Errorf("failed to write message to topic %s: %w", p.Topic, err)
	}
	return nil
}
