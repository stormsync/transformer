package report

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestToUnixTime(t *testing.T) {
	type args struct {
		dateOnly string
		word     string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "should return correct unix time",
			args: args{
				dateOnly: "2024-05-17",
				word:     "1300",
			},
			want: 1715950800,
		},
		{
			name: "should return 0 due to incorrect hour string",
			args: args{
				dateOnly: "2024-05-17",
				word:     "300",
			},
			want: 0,
		},
		{
			name: "should return 0 due to incorrect dateOnly string",
			args: args{
				dateOnly: "202-05-17",
				word:     "300",
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StringToUnixTime(tt.args.dateOnly, tt.args.word)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetDistanceFromLocation(t *testing.T) {
	type args struct {
		loc string
	}
	tests := []struct {
		name          string
		args          args
		wantDistance  int32
		wantDirection string
		wantLocation  string
	}{
		{
			name:          "should return correct distance, direction, and location",
			args:          args{loc: "2 N Luverne"},
			wantDistance:  2,
			wantDirection: "N",
			wantLocation:  "Luverne",
		},
		{
			name:          "should return correct distance, direction, and location",
			args:          args{loc: "4 WNW Tanglewood Forest"},
			wantDistance:  4,
			wantDirection: "WNW",
			wantLocation:  "Tanglewood Forest",
		},
		{
			name:          "should return correct direction, and location",
			args:          args{loc: "WNW Tanglewood Forest"},
			wantDistance:  0,
			wantDirection: "WNW",
			wantLocation:  "Tanglewood Forest",
		},
		{
			name:          "should return correct location",
			args:          args{loc: "Tanglewood Forest"},
			wantDistance:  0,
			wantDirection: "",
			wantLocation:  "Tanglewood Forest",
		},
		{
			name:          "should return correct distance and location",
			args:          args{loc: "4 Tanglewood Forest"},
			wantDistance:  4,
			wantDirection: "",
			wantLocation:  "Tanglewood Forest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := GetDistanceFromLocation(tt.args.loc)
			assert.Equalf(t, tt.wantDistance, got, "GetDistanceFromLocation(%v)", tt.args.loc)
			assert.Equalf(t, tt.wantDirection, got1, "GetDistanceFromLocation(%v)", tt.args.loc)
			assert.Equalf(t, tt.wantLocation, got2, "GetDistanceFromLocation(%v)", tt.args.loc)
		})
	}
}
