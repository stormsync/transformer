package report

import (
	"errors"
	"strings"
	"time"

	report "github.com/stormsync/transformer/proto"
)

// FromCSVLineToWindMsg is the function that will do the actual work to get
// a line transformed into a wind message.
func FromCSVLineToWindMsg(line []byte) (report.WindMsg, error) {
	words := strings.Split(string(line), ",")
	if len(words) < 8 {
		return report.WindMsg{}, errors.New("line did not contain at least 8 columns")
	}
	distance, direction, location := GetDistanceFromLocation(words[2])
	
	return report.WindMsg{
		Time:      StringToUnixTime(time.Now().UTC().Format(time.DateOnly), words[0]),
		Speed:     StringToInt32(words[1]),
		Distance:  distance,
		Direction: direction,
		Location:  location,
		County:    words[3],
		State:     words[4],
		Lat:       words[5],
		Lon:       words[6],
		Remarks:   words[7],
	}, nil
}
