package aivision

import (
	"encoding/json"
	"testing"
)

func TestAsInt64JSONNumber(t *testing.T) {
	for _, test := range []struct {
		number string
		want   int64
	}{
		{"1791076323540501433", 1791076323540501433},
		{"1791076323540501433e0", 1791076323540501433},
		{"1791076323540501433.9", 1791076323540501433},
		{"-1791076323540501433.9", -1791076323540501433},
		{"9223372036854775807", 9223372036854775807},
		{"-9223372036854775808", -9223372036854775808},
		{"9.223372036854775807e18", 9223372036854775807},
		{"-9.223372036854775808e18", -9223372036854775808},
		{"1e3", 1000},
		{"1.9", 1},
		{"-1.9", -1},
		{"1e-400", 0},
	} {
		t.Run(test.number, func(t *testing.T) {
			got, err := asInt64(json.Number(test.number))
			if err != nil || got != test.want {
				t.Fatalf("asInt64(%s) = %d, %v; want %d", test.number, got, err, test.want)
			}
		})
	}
}

func TestAsInt64RejectsJSONNumberOverflow(t *testing.T) {
	for _, number := range []string{"9223372036854775808", "-9223372036854775809", "1e100", "invalid"} {
		t.Run(number, func(t *testing.T) {
			if got, err := asInt64(json.Number(number)); err == nil {
				t.Fatalf("asInt64(%s) = %d; want an error", number, got)
			}
		})
	}
}
