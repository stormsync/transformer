package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/cbrewster/slog-env"

	"github.com/stormsync/transformer"
)

func main() {
	logger := slog.New(slogenv.NewHandler(slog.NewTextHandler(os.Stderr, nil)))
	var groupID = "transform-consume"
	address := os.Getenv("KAFKA_ADDRESS")
	if address == "" {
		log.Fatal("address is required.  Use env var ADDRESS")
	}

	user := os.Getenv("KAFKA_USER")
	if user == "" {
		log.Fatal("kafka user is required.  Use env var KAFKA_USER")
	}

	pw := os.Getenv("KAFKA_PASSWORD")
	if pw == "" {
		log.Fatal("kafka password is required.  Use env var KAFKA_PASSWORD")
	}

	consumerTopic := os.Getenv("CONSUMER_TOPIC")
	if consumerTopic == "" {
		log.Fatal("consume topic is required.  Use env var CONSUMER_TOPIC")
	}
	providerTopic := os.Getenv("PROVIDER_TOPIC")
	if providerTopic == "" {
		log.Fatal("provider topic is required.  Use env var PROVIDER_TOPIC")
	}

	newConsumer, err := tranformer.NewKConsumer(address, consumerTopic, user, pw, groupID, logger)

	if err != nil {
		log.Fatal("unable to create consume: ", err)
	}

	provider, err := tranformer.NewKProvider(address, providerTopic, user, pw, logger)
	if err != nil {
		log.Fatal("unable to create provider: ", err)
	}

	transformer := tranformer.NewTransformer(newConsumer, provider, logger)

	if err != nil {
		log.Fatal("failed to create the collect: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger.Info("Starting transform service")

	for {
		if err := transformer.GetMessage(ctx); err != nil {
			logger.Error("failed to collect message: ", err)
			cancel()
			time.Sleep(10 * time.Second)
			break
		}
	}
}
