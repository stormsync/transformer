package tranformer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"
	"time"

	slogenv "github.com/cbrewster/slog-env"
	"github.com/stormsync/collector"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	report "github.com/stormsync/transformer/proto"
	report2 "github.com/stormsync/transformer/report"
)

func Test_processHailMessage(t *testing.T) {
	type args struct {
		line []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr error
	}{
		{
			name: "should parse a valid hail message line correctly",
			args: args{line: []byte("2132,450,9 SE Granbury,Hood,TX,32.36,-97.66,DELAYED REPORT emergency management reported 4.5 inch hail in Pecan Plantation. (FWD)")},
			want: mustMarshal(&report.HailMsg{
				Time:      report2.StringToUnixTime(time.Now().UTC().Format(time.DateOnly), "2132"),
				Size:      int32(450),
				Distance:  9,
				Direction: "SE",
				Location:  "Granbury",
				County:    "Hood",
				State:     "TX",
				Lat:       "32.36",
				Lon:       "-97.66",
				Remarks:   "DELAYED REPORT emergency management reported 4.5 inch hail in Pecan Plantation. (FWD)",
			}),
			wantErr: nil,
		},
		{
			name:    "should error when time is missing from hail line",
			args:    args{line: []byte("450,9 SE Granbury,Hood,TX,32.36,-97.66,DELAYED REPORT emergency management reported 4.5 inch hail in Pecan Plantation. (FWD)")},
			want:    nil,
			wantErr: errors.New("line did not contain at least 8 columns"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processHailMessage(tt.args.line)

			if err != nil {
				err = errors.Unwrap(err)
			}
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func mustMarshal(m proto.Message) []byte {
	b, err := proto.Marshal(m)
	if err != nil {
		log.Fatal("failed to setup proto marshal for test: ", err)
	}
	return b
}

func Test_processWindMessage(t *testing.T) {
	type args struct {
		line []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr error
	}{
		{
			name: "should parse a valid wind message line correctly",
			args: args{line: []byte("1835,UNK,2 N Holt,Irwin,GA,31.63,-83.15,Trees down on McLeod Road. (TAE)")},
			want: mustMarshal(&report.WindMsg{
				Time:      report2.StringToUnixTime(time.Now().UTC().Format(time.DateOnly), "1835"),
				Speed:     int32(0),
				Distance:  2,
				Direction: "N",
				Location:  "Holt",
				County:    "Irwin",
				State:     "GA",
				Lat:       "31.63",
				Lon:       "-83.15",
				Remarks:   "Trees down on McLeod Road. (TAE)",
			}),
			wantErr: nil,
		},
		{
			name:    "should error when time is missing",
			args:    args{line: []byte("UNK,2 N Holt,Irwin,GA,31.63,-83.15,Trees down on McLeod Road. (TAE)")},
			want:    nil,
			wantErr: fmt.Errorf("line did not contain at least 8 columns"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processWindMessage(tt.args.line)
			if err != nil {
				err = errors.Unwrap(err)
			}
			assert.Equalf(t, tt.want, got, "processWindMessage(%v)", tt.args.line)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func Test_processTornadoMessage(t *testing.T) {
	type args struct {
		line []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr error
	}{
		{
			name: "should parse a valid tornado message correctly",
			args: args{line: []byte("1131,UNK,2 SSW Lamont,Jefferson,FL,30.35,-83.83,A tornado touched down in far eastern Jefferson county and moved through most of southern Madison county. EF0 tree damage was confirmed in Jefferson county with EF1 dam (TAE)")},
			want: mustMarshal(&report.TornadoMsg{
				Time:      report2.StringToUnixTime(time.Now().UTC().Format(time.DateOnly), "1131"),
				F_Scale:   int32(0),
				Distance:  2,
				Direction: "SSW",
				Location:  "Lamont",
				County:    "Jefferson",
				State:     "FL",
				Lat:       "30.35",
				Lon:       "-83.83",
				Remarks:   "A tornado touched down in far eastern Jefferson county and moved through most of southern Madison county. EF0 tree damage was confirmed in Jefferson county with EF1 dam (TAE)",
			}),
			wantErr: nil,
		},
		{
			name:    "should error when time is missing for tornado",
			args:    args{line: []byte("UNK,2 SSW Lamont,Jefferson,FL,30.35,-83.83,A tornado touched down in far eastern Jefferson county and moved through most of southern Madison county. EF0 tree damage was confirmed in Jefferson county with EF1 dam (TAE)")},
			want:    nil,
			wantErr: fmt.Errorf("line did not contain at least 8 columns"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processTornadoMessage(tt.args.line)
			if err != nil {
				err = errors.Unwrap(err)
			}
			assert.Equalf(t, tt.want, got, "processTornadoMessage(%v)", tt.args.line)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

func TestTransformer_processMessage(t *testing.T) {
	type args struct {
		rptType collector.ReportType
		line    []byte
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr error
	}{
		{
			name: "should return correct byte slice for hail data",
			args: args{
				rptType: collector.Hail,
				line:    []byte("1835,UNK,2 N Holt,Irwin,GA,31.63,-83.15,Trees down on McLeod Road. (TAE)"),
			},
			want: mustMarshal(&report.WindMsg{
				Time:      report2.StringToUnixTime(time.Now().UTC().Format(time.DateOnly), "1835"),
				Speed:     int32(0),
				Distance:  2,
				Direction: "N",
				Location:  "Holt",
				County:    "Irwin",
				State:     "GA",
				Lat:       "31.63",
				Lon:       "-83.15",
				Remarks:   "Trees down on McLeod Road. (TAE)",
			}),
			wantErr: nil,
		},
		{
			name: "should return correct byte slice for wind data",
			args: args{
				rptType: collector.Hail,
				line:    []byte("1835,UNK,2 N Holt,Irwin,GA,31.63,-83.15,Trees down on McLeod Road. (TAE)"),
			},
			want: mustMarshal(&report.WindMsg{
				Time:      report2.StringToUnixTime(time.Now().UTC().Format(time.DateOnly), "1835"),
				Speed:     int32(0),
				Distance:  2,
				Direction: "N",
				Location:  "Holt",
				County:    "Irwin",
				State:     "GA",
				Lat:       "31.63",
				Lon:       "-83.15",
				Remarks:   "Trees down on McLeod Road. (TAE)",
			}),
			wantErr: nil,
		},
		{
			name: "should return correct byte slice for tornado data",
			args: args{
				rptType: collector.Tornado,
				line:    []byte("1131,UNK,2 SSW Lamont,Jefferson,FL,30.35,-83.83,A tornado touched down in far eastern Jefferson county and moved through most of southern Madison county. EF0 tree damage was confirmed in Jefferson county with EF1 dam (TAE)"),
			},
			want: mustMarshal(&report.TornadoMsg{
				Time:      report2.StringToUnixTime(time.Now().UTC().Format(time.DateOnly), "1131"),
				F_Scale:   int32(0),
				Distance:  2,
				Direction: "SSW",
				Location:  "Lamont",
				County:    "Jefferson",
				State:     "FL",
				Lat:       "30.35",
				Lon:       "-83.83",
				Remarks:   "A tornado touched down in far eastern Jefferson county and moved through most of southern Madison county. EF0 tree damage was confirmed in Jefferson county with EF1 dam (TAE)",
			}),
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Transformer{}
			got, err := tr.processMessage(tt.args.rptType, tt.args.line)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}

type mockConsumer struct {
	expectedData  ReaderResponse
	expectedError error
}

func (mc *mockConsumer) ReadMessage(ctx context.Context) (ReaderResponse, error) {
	return mc.expectedData, mc.expectedError
}

type mockProducer struct {
	expectedError error
}

func (mp *mockProducer) WriteMessage(ctx context.Context, wp WriterPayload) error {
	return mp.expectedError
}
func TestTransformer_GetMessage(t1 *testing.T) {
	type fields struct {
		consumer      Consumer
		producer      Provider
		consumerTopic string
		producerTopic string
		logger        *slog.Logger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr error
	}{
		{
			name: "should properly get and process messages",
			fields: fields{
				consumer: &mockConsumer{
					expectedData: ReaderResponse{
						Topic: "raw-weather-report",
						Key:   nil,
						Value: []byte("1835,UNK,2 N Holt,Irwin,GA,31.63,-83.15,Trees down on McLeod Road. (TAE)"),
						Headers: []ReaderHeader{{
							Key:   "reportType",
							Value: []byte(collector.Tornado.String()),
						}},
						Time: time.Time{},
					},
					expectedError: nil,
				},
				producer:      &mockProducer{expectedError: nil},
				consumerTopic: "raw-weather-report",
				logger:        slog.New(slogenv.NewHandler(slog.NewTextHandler(os.Stderr, nil))),
			},
			args:    args{ctx: context.Background()},
			wantErr: nil,
		},
		{
			name: "should properly get and process messages",
			fields: fields{
				consumer: &mockConsumer{
					expectedData: ReaderResponse{
						Topic: "raw-weather-report",
						Key:   nil,
						Value: []byte("1835,UNK,2 N Holt,Irwin,GA,31.63,-83.15,Trees down on McLeod Road. (TAE)"),
						Headers: []ReaderHeader{{
							Key:   "reportType",
							Value: []byte(collector.Tornado.String()),
						}},
						Time: time.Time{},
					},
					expectedError: nil,
				},
				producer:      &mockProducer{expectedError: fmt.Errorf("failed to write message for type tornado: %w", errors.New("some producer error"))},
				consumerTopic: "raw-weather-report",
				logger:        slog.New(slogenv.NewHandler(slog.NewTextHandler(os.Stderr, nil))),
			},
			args:    args{ctx: context.Background()},
			wantErr: fmt.Errorf("failed to write message for type tornado: %w", errors.New("some producer error")),
		},
	}
	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {

			t := &Transformer{
				consumer:      tt.fields.consumer,
				consumerTopic: tt.fields.consumerTopic,
				producerTopic: tt.fields.producerTopic,
				producer:      tt.fields.producer,
				logger:        tt.fields.logger,
			}

			err := t.GetMessage(tt.args.ctx)
			if err != nil {
				err = errors.Unwrap(err)
			}
			assert.Equal(t1, tt.wantErr, err)
		})
	}
}

func Test_getReportTypeFromHeader(t *testing.T) {
	type args struct {
		hdrs []ReaderHeader
	}
	tests := []struct {
		name    string
		args    args
		want    collector.ReportType
		wantErr error
	}{
		{
			name:    "should return hail report type for hail header",
			args:    args{hdrs: []ReaderHeader{{"reportType", []byte(collector.Hail.String())}}},
			want:    collector.Hail,
			wantErr: nil,
		},
		{
			name:    "should return tornado report type for tornado header",
			args:    args{hdrs: []ReaderHeader{{"reportType", []byte(collector.Tornado.String())}}},
			want:    collector.Tornado,
			wantErr: nil,
		},
		{
			name:    "should return wind report type for wind header",
			args:    args{hdrs: []ReaderHeader{{"reportType", []byte(collector.Wind.String())}}},
			want:    collector.Wind,
			wantErr: nil,
		},
		{
			name:    "should return error for no key found.",
			args:    args{hdrs: []ReaderHeader{{"", []byte(collector.Wind.String())}}},
			want:    collector.Hail,
			wantErr: errors.New("unable to find reportType key, cannot determine report type"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getReportTypeFromHeader(tt.args.hdrs)

			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantErr, err)
		})
	}
}
