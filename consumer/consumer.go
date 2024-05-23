package consumer

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Consumer interface {
	ReadMessage(ctx context.Context) (ReaderResponse, error)
}

type ReaderResponse struct {
	Topic         string
	Queue         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Headers       []ReaderHeader
	Time          time.Time
}

type ReaderHeader struct {
	Key   string
	Value []byte
}

type KConsumer struct {
	Reader   *kafka.Reader
	Topic    string
	Address  string
	user     string
	password string
	logger   *slog.Logger
}

func NewKConsumer(address, topic, user, pw, groupID string, logger *slog.Logger) (*KConsumer, error) {
	mechanism, err := scram.Mechanism(scram.SHA256, user, pw)
	if err != nil {
		return nil, fmt.Errorf("failed to create scram.Mechanism for auth: %w", err)
	}
	readerConfig := kafka.ReaderConfig{
		GroupID:        groupID,
		CommitInterval: time.Second,
		Brokers:        []string{address},
		Topic:          topic,
		Dialer: &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		}}

	reader := kafka.NewReader(readerConfig)
	if err != nil {
		return nil, err
	}
	return &KConsumer{
		Reader:   reader,
		Topic:    topic,
		Address:  address,
		user:     user,
		password: pw,
		logger:   logger,
	}, nil
}

// ReadMessage allows for the consumer to read a message from a topic,
// commit the message, and return a readerResponse struct.
func (c *KConsumer) ReadMessage(ctx context.Context) (ReaderResponse, error) {
	var readerResponse ReaderResponse
	var err error
	message, err := c.Reader.ReadMessage(ctx)
	if err != nil {
		return readerResponse, fmt.Errorf("failed to read message from topic %s: %w", c.Topic, err)
	}

	return messageToReaderResponse(message), nil
}

func messageToReaderResponse(msg kafka.Message) ReaderResponse {
	var rhs []ReaderHeader
	for _, h := range msg.Headers {
		rhs = append(rhs, ReaderHeader{Key: h.Key,
			Value: h.Value,
		})
	}
	return ReaderResponse{
		Topic:         msg.Topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		HighWaterMark: msg.HighWaterMark,
		Key:           msg.Key,
		Value:         msg.Value,
		Headers:       rhs,
		Time:          msg.Time,
	}
}
