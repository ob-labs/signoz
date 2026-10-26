package oceanbasetelemetrystore

import (
	"github.com/SigNoz/signoz/pkg/ingest"
	"testing"
)

func TestStatisticsVersionSupportsOceanBaseAndExcludesSeekdb(t *testing.T) {
	for _, test := range []struct {
		version string
		want    bool
	}{
		{"5.7.25-OceanBase_CE-v4.4.2.1", true},
		{"5.7.25-OceanBase-v4.4.2.1", true},
		{"5.7.25-OceanBase-v4.3.5.3", false},
		{"5.7.25-OceanBase-v4.2.5.0", false},
		{"5.7.25-OceanBase seekdb-v1.1.0", false},
		{"5.7.25-seekdb-v1.1.0", false},
		{"8.0.36-MySQL", false},
		{"OceanBase-vUnknown", false},
	} {
		if got := ingest.TraceStatisticsVersionSupported(test.version); got != test.want {
			t.Errorf("%q: got %v want %v", test.version, got, test.want)
		}
	}
}
