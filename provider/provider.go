package internal

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Provider interface {
	WriteMessage(ctx context.Context, payload WriterPayload) error
}

type WriterPayload struct {
	Body []byte
	Type string
}

type KProvider struct {
	Writer   *kafka.Writer
	Topic    string
	Address  string
	user     string
	password string
	logger   *slog.Logger
}

// NewProvider generates a new kafka provider allowing for writes to a topic
func NewKProvider(address, topic, user, pw string, logger *slog.Logger) (*KProvider, error) {
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

	return &KProvider{
		Writer:   &w,
		Topic:    topic,
		Address:  address,
		user:     user,
		password: pw,
		logger:   logger,
	}, nil
}

// WriteMessage allows writing to topic defined in the Provider constructor.
func (p *KProvider) WriteMessage(ctx context.Context, wp WriterPayload) error {
	if wp.Type == "" {
		p.logger.Debug("payload type is empty in WriteMessage", "type", wp.Type)
		return errors.New("payload type cannot be empty")
	}
	if wp.Body == nil {
		p.logger.Debug("payload body is nil in WriteMessage", "type", wp.Type)
		return errors.New("payload body cannot be nil")
	}

	header := []kafka.Header{{
		Key:   "reportType",
		Value: []byte(wp.Type),
	}}

	p.logger.Debug("writing message", "type", wp.Type)
	err := p.Writer.WriteMessages(ctx, kafka.Message{Value: wp.Body, Headers: header})
	if err != nil {
		p.logger.Debug("WriteMessages failed", "Type", wp.Type, "bBody", string(wp.Body))
		return fmt.Errorf("failed to write message to topic %s; line: %s;  err: %w", p.Topic, string(wp.Body), err)
	}
	return nil
}
