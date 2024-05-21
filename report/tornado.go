package report

import (
	"errors"
	"strings"
	"time"

	"github.com/stormsync/collector"

	report "github.com/stormsync/transformer/proto"
)

// FromCSVLineToTornado}Msg is the function that will do the actual work to get
// a line transformed into a tornado message.
func FromCSVLineToTornadoMsg(line []byte) (report.TornadoMsg, error) {
	words := strings.Split(string(line), ",")
	if len(words) < 8 {
		return report.TornadoMsg{}, errors.New("line did not contain at least 8 columns")
	}
	distance, direction, location := GetDistanceFromLocation(words[2])

	return report.TornadoMsg{
		Type:      collector.Tornado.String(),
		Time:      StringToUnixTime(time.Now().UTC().Format(time.DateOnly), words[0]),
		F_Scale:   StringToInt32(words[1]),
		Distance:  distance,
		Direction: direction,
		Location:  location,
		County:    words[3],
		State:     words[4],
		Lat:       words[5],
		Lon:       words[6],
		Remarks:   words[7],
	}, nil
	// xx
}
