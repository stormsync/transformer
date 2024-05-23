package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	jaegerPropagator "go.opentelemetry.io/contrib/propagators/jaeger"

	slogenv "github.com/cbrewster/slog-env"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"

	"github.com/stormsync/transformer/internal"
)

func main() {
	logger := slog.New(slogenv.NewHandler(slog.NewTextHandler(os.Stderr, nil)))
	otel.SetTextMapPropagator(jaegerPropagator.Jaeger{})
	ctx := context.Background()
	traceProvider, err := startTracer()
	if err != nil {
		log.Fatal("Unable to initiate tracer: ", err)
	}
	defer func() {
		if err := traceProvider.Shutdown(context.Background()); err != nil {
			log.Fatalf("traceprovider: %v", err)
		}
	}()

	tracer := traceProvider.Tracer("transform")
	ctx, span := tracer.Start(ctx, "main")
	defer span.End()

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

	newConsumer, err := internal.NewKConsumer(address, consumerTopic, user, pw, groupID, logger)

	if err != nil {
		log.Fatal("unable to create consume: ", err)
	}

	provider, err := internal.NewKProvider(address, providerTopic, user, pw, logger)
	if err != nil {
		log.Fatal("unable to create provider: ", err)
	}

	transformer := internal.NewTransformer(newConsumer, provider, tracer, logger)

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
		// TODO: remove this - using for testing
		time.Sleep(10 * time.Second)
	}
}

func startTracer() (*trace.TracerProvider, error) {
	headers := map[string]string{
		"content-type": "application/json",
	}

	exporter, err := otlptrace.New(
		context.Background(),
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint("localhost:4318"),
			otlptracehttp.WithHeaders(headers),
			otlptracehttp.WithInsecure(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new tracing exporter: %w", err)
	}

	tracerProvider := trace.NewTracerProvider(
		trace.WithBatcher(
			exporter,
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
			trace.WithBatchTimeout(trace.DefaultScheduleDelay*time.Millisecond),
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
		),
		trace.WithResource(
			resource.NewWithAttributes(
				semconv.SchemaURL,
				semconv.ServiceNameKey.String("collector"),
			),
		),
	)

	otel.SetTracerProvider(tracerProvider)

	return tracerProvider, nil
}
