package transformer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/stormsync/collector"
	"go.opentelemetry.io/otel/trace"

	"github.com/stormsync/transformer/consumer"
	"github.com/stormsync/transformer/provider"
	"github.com/stormsync/transformer/report"

	"google.golang.org/protobuf/proto"
)

type Transformer struct {
	consumer consumer.Consumer
	producer provider.Provider
	tracer   trace.Tracer

	consumerTopic string
	producerTopic string // transformed-weather-data
	logger        *slog.Logger
}

// NewTransformer will return a pointer to a Transformer that allowes for pulling report
// messages off the raw topic, converting each line into a marshaled protobuff,
// and sending that off to the transformed topic.
func NewTransformer(consumer consumer.Consumer, provider provider.Provider, tracer trace.Tracer, logger *slog.Logger) *Transformer {
	return &Transformer{
		tracer:   tracer,
		consumer: consumer,
		producer: provider,
		logger:   logger,
	}
}

// GetMessage pulls a message off of the topic, transforms it,
// applies business logic if needed, marshals, and moves onto
// another topic for further processing.
func (t *Transformer) GetMessage(ctx context.Context) error {

	readResponse, err := t.consumer.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to get message: %w", err)
	}

	t.logger.Debug("incoming message", "message value ", string(readResponse.Value))

	reportType, err := getReportTypeFromHeader(readResponse.Headers)
	if err != nil {
		t.logger.Debug("getreporttypefromheader()", "error", err)
	}
	t.logger.Debug("report type", "type", reportType.String())

	msgBytes, err := t.processMessage(reportType, readResponse.Value)
	if err != nil {
		return fmt.Errorf("failed to process message: %w", err)
	}

	wp := provider.WriterPayload{
		Body: msgBytes,
		Type: reportType.String(),
	}
	if err := t.producer.WriteMessage(ctx, wp); err != nil {

		return fmt.Errorf("failed to write message for type %s: %w", reportType.String(), err)
	}
	t.logger.Debug("message written to topic", "topic", t.producerTopic, "report type", reportType.String(), "line", string(readResponse.Value))

	return nil
}

// getReportTypeFromHeader extracts the report type from the message headers
func getReportTypeFromHeader(hdrs []consumer.ReaderHeader) (collector.ReportType, error) {
	var rptType collector.ReportType
	var err error

	if len(hdrs) == 0 {
		return rptType, errors.New("headers do not contain report type")
	}

	reportERR := errors.New("unable to find reportType key, cannot determine report type")
	for _, v := range hdrs {
		if strings.EqualFold(v.Key, "reportType") {
			rptType, err = collector.FromString(string(v.Value))
			if err != nil {
				return rptType, fmt.Errorf("unable to process report due to unknown type: %w", err)
			}
			reportERR = nil
			break
		}
	}
	return rptType, reportERR
}

// processMessage performs the logic to get a generic line from an input message and turn it
// into the appropriate marshaled protob type that gets passed back as []byte.
func (t *Transformer) processMessage(rptType collector.ReportType, line []byte) ([]byte, error) {
	var msg []byte
	var err error
	switch rptType {
	case collector.Hail:
		msg, err = processHailMessage(line)
	case collector.Wind:
		msg, err = processWindMessage(line)
	case collector.Tornado:
		msg, err = processTornadoMessage(line)
	default:
		err = fmt.Errorf("unknown report type %q", rptType.String())
	}
	return msg, err
}

func processHailMessage(line []byte) ([]byte, error) {
	if line == nil {
		return nil, errors.New("line cannot be nil")
	}
	hailMsg, err := report.FromCSVLineToHailMsg(line)
	if err != nil {
		return nil, fmt.Errorf("unable to convert line to hail report %q: %w", string(line), err)
	}

	// implement any business logic before this line
	mBytes, err := proto.Marshal(&hailMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to process hail message: %w", err)
	}
	return mBytes, nil
}

func processWindMessage(line []byte) ([]byte, error) {
	if line == nil {
		return nil, errors.New("line cannot be nil")
	}
	windMsg, err := report.FromCSVLineToWindMsg(line)
	if err != nil {
		return nil, fmt.Errorf("unable to convert line to wind report %q: %w", string(line), err)
	}

	// implement any business logic before this line
	mBytes, err := proto.Marshal(&windMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to process wind message: %w", err)
	}
	return mBytes, nil
}

func processTornadoMessage(line []byte) ([]byte, error) {
	if line == nil {
		return nil, errors.New("line cannot be nil")
	}
	tornadoMsg, err := report.FromCSVLineToTornadoMsg(line)
	if err != nil {
		return nil, fmt.Errorf("unable to convert line to tornado report %q: %w", string(line), err)
	}

	// implement any business logic before this line
	mBytes, err := proto.Marshal(&tornadoMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to process Tornado Message: %w", err)
	}
	return mBytes, nil
}
