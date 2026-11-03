package aivision

import (
	"errors"
	"fmt"

	"github.com/SigNoz/signoz/pkg/http/render"
	jsoniter "github.com/json-iterator/go"
)

const traceWindowEnvelopeReserve = 16 * 1024
const traceWindowEntryReserve = 512

var errTraceWindowPayloadBudget = errors.New("complete recovery window exceeds response byte budget; narrow the time range")

// Use the renderer's serializer for exact escaping and number formatting.
// Marshal only individual entries, never the complete response a second time.
var traceWindowJSON = jsoniter.ConfigCompatibleWithStandardLibrary

type traceWindowWireBudget struct{ used, limit int }

func newTraceWindowWireBudget(limit int) *traceWindowWireBudget {
	return &traceWindowWireBudget{used: traceWindowEnvelopeReserve, limit: limit}
}

func (b *traceWindowWireBudget) addBytes(size int) error {
	if size < 0 || b.used > b.limit || size > b.limit-b.used {
		return errTraceWindowPayloadBudget
	}
	b.used += size
	return nil
}

func (b *traceWindowWireBudget) addRow(row map[string]any) error {
	encoded, err := traceWindowJSON.Marshal(row)
	if err != nil {
		return err
	}
	// Every entry pays for its exact encoded object plus more than its array
	// comma and framing. This also covers empty/one-entry boundary cases.
	return b.addBytes(len(encoded) + traceWindowEntryReserve)
}

func (b *traceWindowWireBudget) addRawGroup(traceID string) error {
	encoded, err := traceWindowJSON.Marshal(traceID)
	if err != nil {
		return err
	}
	// Count the escaped ID explicitly, including IDs outside the usual hex
	// shape. Sixty-four bytes cover {"traceId":...,"spans":[]} and its comma.
	return b.addBytes(len(encoded) + 64)
}

func traceWindowResponse(meta map[string]any, rows, details []map[string]any) map[string]any {
	contract := traceWindowContract
	if meta["representation"] == "metrics-v1" {
		contract = traceMetricsWindowContract
	}
	return map[string]any{"supported": true, "contract": contract, "complete": true, "meta": meta, "rows": rows, "raw": details}
}

func (b *traceWindowWireBudget) validateEnvelope(meta map[string]any) error {
	// The fixed reserve covers the actual HTTP success wrapper, metadata, and
	// empty arrays. Replacing [] with entries adds only the independently paid
	// object bytes, group framing and commas. Thus the encoded HTTP body cannot
	// exceed used <= limit, without allocating a full-window encoded copy here.
	empty := traceWindowResponse(meta, []map[string]any{}, []map[string]any{})
	encoded, err := traceWindowJSON.Marshal(&render.SuccessResponse{Status: "success", Data: empty})
	if err != nil {
		return err
	}
	if len(encoded) > traceWindowEnvelopeReserve {
		return fmt.Errorf("complete recovery window metadata exceeds the bounded envelope")
	}
	return nil
}
