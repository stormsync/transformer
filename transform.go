package tranformer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/stormsync/collector"

	"github.com/stormsync/transformer/report"

	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Transformer struct {
	consumer      *Consumer
	producer      *Provider
	consumerTopic string
	producerTopic string // transformed-weather-data
	logger        *slog.Logger
}

// NewTransformer will return a pointer to a Transformer that allowes for pulling report
// messages off the raw topic, converting each line into a marshaled protobuff,
// and sending that off to the transformed topic.
func NewTransformer(consumer *Consumer, provider *Provider, logger *slog.Logger) *Transformer {
	return &Transformer{
		consumer: consumer,
		producer: provider,
		logger:   logger,
	}
}

// GetMessage pulls a message off of the topic, transforms it,
// applies business logic if needed, marshals, and moves onto
// another topic for further processing.
func (t *Transformer) GetMessage(ctx context.Context) error {
	msg, err := t.consumer.ReadMessage(ctx)
	if err != nil {
		return fmt.Errorf("failed to get message: %w", err)
	}
	t.logger.Debug("incoming message", string(msg.Value))

	reportType, err := getReportTypeFromHeader(msg.Headers)
	if err != nil {
		log.Println("error: ", err)
	}
	t.logger.Debug("report type", reportType.String())

	lines := strings.Split(string(msg.Value), "\n")
	for i, line := range lines {
		if i == 0 || line == "" {
			t.logger.Debug("skipping line number", i)
			continue
		}
		msgBytes, err := t.processMessage(reportType, []byte(line))
		if err != nil {
			return fmt.Errorf("failed to process message: %w", err)
		}

		if err := t.producer.WriteMessage(ctx, msgBytes, reportType.String()); err != nil {
			return nil
		}
		t.logger.Debug("message written to topic", t.producerTopic, "report type", reportType.String(), "line", line)

	}

	return nil
}

// getReportTypeFromHeader extracts the report type from the message headers
func getReportTypeFromHeader(hdrs []kafka.Header) (collector.ReportType, error) {
	var rptType collector.ReportType
	var err error
	for _, v := range hdrs {
		if strings.EqualFold(string(v.Value), "reportType") {
			rptType, err = collector.FromString(string(v.Value))
			if err != nil {
				err = errors.Join(err, fmt.Errorf("unable to process report due to unknown type: %w", err))
			}
		}
	}
	return rptType, err
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
	log.Printf("CSV Line turned into a report: \n%#+v\n", windMsg)

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
