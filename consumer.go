package tranformer

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Consumer struct {
	Reader   *kafka.Reader
	Topic    string
	Address  string
	user     string
	password string
	logger   *slog.Logger
}

// NewConsumer generates a new kafka provider.
func NewConsumer(address, topic, user, pw, groupID string, logger *slog.Logger) (*Consumer, error) {
	mechanism, err := scram.Mechanism(scram.SHA256, user, pw)
	if err != nil {
		return nil, fmt.Errorf("failed to create scram.Mechanism for auth: %w", err)
	}
	readerConfig := kafka.ReaderConfig{
		GroupID:        groupID,
		CommitInterval: time.Second,
		Brokers:        []string{"organic-ray-9236-us1-kafka.upstash.io:9092"},
		Topic:          topic,
		Dialer: &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		}}
	reader := kafka.NewReader(readerConfig)

	return &Consumer{
		Reader:   reader,
		Topic:    topic,
		Address:  address,
		user:     user,
		password: pw,
		logger:   logger,
	}, nil

}

// ReadMessage allows for the consumer to read a message from a topic
// and commit that message.
func (c *Consumer) ReadMessage(ctx context.Context) (kafka.Message, error) {
	message, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		fmt.Errorf("failed to read message: %w", err)
		return kafka.Message{}, err
	}
	if err := c.Reader.CommitMessages(ctx, message); err != nil {
		fmt.Errorf("failed to commit message: %w", err)
	}

	return message, err
}
