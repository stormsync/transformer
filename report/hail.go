// Package report is used to transform the lines coming from the NWS storm report
// into a protobuff message.
package report

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/stormsync/collector"

	report "github.com/stormsync/transformer/proto"
)

var (
	// directions is used to decode direction in the Location line while it is being parsed
	directions = map[string]struct{}{
		"N":   {},
		"NNE": {},
		"NE":  {},
		"ENE": {},
		"E":   {},
		"ESE": {},
		"SE":  {},
		"SSE": {},
		"S":   {},
		"SSW": {},
		"SW":  {},
		"WSW": {},
		"W":   {},
		"WNW": {},
		"NW":  {},
		"NNW": {},
	}
)

func FromCSVLineToHailMsg(line []byte) (report.HailMsg, error) {
	words := strings.Split(string(line), ",")
	if len(words) < 8 {
		return report.HailMsg{}, errors.New("line did not contain at least 8 columns")
	}
	distance, direction, location := GetDistanceFromLocation(words[2])
	return report.HailMsg{
		Type:      collector.Hail.String(),
		Time:      StringToUnixTime(time.Now().UTC().Format(time.DateOnly), words[0]),
		Size:      StringToInt32(words[1]),
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

// GetDistanceFromLocation takes in a location field and provides the distance, direction, and landmark as output.
// input of "1 SSW Bassville Park" would generate 1, "SSW", "Bassville Park"
// input of "SSW Bassville Park" would generate 0, "SSW", "Bassville Park"
// input of "1 Bassville Park" would generate 1, "", "Bassville Park"
// input of "Bassville Park" would generate 0, "", "Bassville Park"
func GetDistanceFromLocation(loc string) (int32, string, string) {
	var distance int32
	var direction, location string
	words := strings.Split(loc, " ")
	for i, word := range words {
		if n, err := strconv.Atoi(word); err == nil {
			distance = int32(n)
			continue
		}

		if _, ok := directions[strings.ToUpper(word)]; ok {
			direction = strings.ToUpper(word)
			continue
		}
		location = strings.Join(words[i:], " ")
		break
	}
	return distance, direction, location
}

// StringToInt32 will take in a string and attempt to convert it into an int.
// if there is an error on conversion a zero is returned.
// if no error the integer is cast to an int32.
func StringToInt32(size string) int32 {
	i, err := strconv.Atoi(size)
	if err != nil {
		i = 0
	}
	return int32(i)
}

// StringToUnixTime will take in the hhmm field from a NWS report line as well as a
// date string, such as 2024-01-30, and build a UTC timestamp that matches
// the time of the line entry according to NWS as well as the date the overall report
// is being recorded on.
func StringToUnixTime(dateOnly string, hhmm string) int64 {
	if len(hhmm) < 4 {
		return 0
	}

	if _, err := time.Parse(time.DateOnly, dateOnly); err != nil {
		return 0
	}

	t := fmt.Sprintf("%s %s:%s:00", dateOnly, hhmm[0:2], hhmm[2:4])
	newTime, err := time.Parse(time.DateTime, t)
	if err != nil {
		return 0
	}
	return newTime.UTC().Unix()

}
