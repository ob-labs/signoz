package ingest

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestExtractTraceMediaTypedAndBounded(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\nexample")
	b64 := base64.StdEncoding.EncodeToString(png)
	for _, tc := range []struct {
		input string
		count int
	}{
		{fmt.Sprintf(`[{"type":"image_url","image_url":{"url":"data:image/png;base64,%s"}}]`, b64), 1},
		{fmt.Sprintf(`{"type":"image","source":{"type":"base64","media_type":"image/png","data":"%s"}}`, b64), 1},
		{fmt.Sprintf(`{"arbitrary":"%s"}`, b64), 0},
		{fmt.Sprintf(`{"type":"image","mime_type":"image/png","data":"%s","text":"[REDACTED]"}`, b64), 0},
		{fmt.Sprintf(`{"type":"image","mime_type":"image/png","data":"%s","text":"[redacted]"}`, b64), 0},
		{`{"type":"image_url","image_url":{"url":"https://example.invalid/image.png"}}`, 0},
		{`{"type":"file","mime_type":"application/pdf","data":"YmFk"}`, 0},
	} {
		items := extractTraceMedia(tc.input, 16<<20)
		if len(items) != tc.count {
			t.Fatalf("%s: got %d want %d", tc.input, len(items), tc.count)
		}
		if len(items) > 0 && string(items[0].Data) != string(png) {
			t.Fatal("media bytes changed")
		}
	}
	if got := extractTraceMedia(fmt.Sprintf(`{"type":"image","mime_type":"image/png","data":"%s"}`, b64), len(png)-1); len(got) != 0 {
		t.Fatal("accepted oversized media")
	}
}

func TestTraceMediaTwentyMiBBoundary(t *testing.T) {
	const limit = 16 << 20
	content := append([]byte("%PDF-"), bytes.Repeat([]byte{'x'}, limit-5)...)
	makeMessage := func(value []byte) string {
		return fmt.Sprintf(`{"type":"file","mime_type":"application/pdf","data":"%s"}`, base64.StdEncoding.EncodeToString(value))
	}
	items := extractTraceMedia(makeMessage(content), limit)
	if len(items) != 1 || !bytes.Equal(items[0].Data, content) {
		t.Fatal("exactly 16 MiB must be preserved")
	}
	content = append(content, 'x')
	if items := extractTraceMedia(makeMessage(content), limit); len(items) != 0 {
		t.Fatal("a file exceeding 16 MiB must not be accepted")
	}
}

func TestProjectTraceMediaPreservesOrderNumbersAndErrors(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString([]byte("%PDF-example"))
	raw := fmt.Sprintf(`{"n":9007199254740993,"content":[{"type":"text","text":"hello"},{"type":"file","mime_type":"application/pdf","data":"%s"}]}`, b64)
	projected, err := projectTraceMedia(raw, 16<<20, func(m traceMedia) (string, error) { return "attachment", nil })
	if !strings.Contains(projected, `"sha256":`) {
		t.Fatal("missing stable content identity")
	}
	if err != nil || !strings.Contains(projected, `9007199254740993`) || !strings.Contains(projected, `"id":"attachment"`) || strings.Contains(projected, b64) {
		t.Fatalf("projection: %s %v", projected, err)
	}
	expected := errors.New("storage failed")
	if _, err := projectTraceMedia(raw, 16<<20, func(m traceMedia) (string, error) { return "", expected }); !errors.Is(err, expected) {
		t.Fatal("storage error was lost")
	}
}

func TestMediaFailureProjectionRetainsRawOutsideDisplay(t *testing.T) {
	for _, raw := range []string{
		`{"type":"audio","mime_type":"audio/mpeg","data":"invalid"}`,
		`{"type":"file","mime_type":"application/pdf","data":"` + base64.StdEncoding.EncodeToString([]byte("%PDF-too-large")) + `"}`,
	} {
		projected, err := projectTraceMedia(raw, 5, func(traceMedia) (string, error) { t.Fatal("invalid bytes stored"); return "", nil })
		if err != nil || !strings.Contains(projected, "aivision_media_error") {
			t.Fatalf("unsafe display fallback: %s %v", projected, err)
		}
	}
	if !validTraceMedia("image/svg+xml", []byte(`<svg xmlns="http://www.w3.org/2000/svg"><rect width="2" height="2"/></svg>`)) {
		t.Fatal("safe svg rejected")
	}
	for _, svg := range []string{`<svg><script>alert(1)</script></svg>`, `<svg onload="alert(1)"/>`, `<!DOCTYPE svg><svg/>`, `<svg><image href="https://example.org/a"/></svg>`} {
		if validTraceMedia("image/svg+xml", []byte(svg)) {
			t.Fatal("active svg accepted")
		}
	}
}

func TestSerializedToolMediaUsesSameReferencePipeline(t *testing.T) {
	raw := `{"lc":1,"type":"constructor","kwargs":{"content":"{\"imageUrl\":\"data:image/png;base64,iVBORw0KGgo=\",\"mimeType\":\"image/png\"}"}}`
	got, err := projectTraceMedia(raw, 16<<20, func(traceMedia) (string, error) { return "stored", nil })
	if err != nil || !strings.Contains(got, "aivision_attachment") || strings.Contains(got, "base64") {
		t.Fatalf("serialized tool output remained inline: %s %v", got, err)
	}
}

func TestDataURLFailuresNeverRemainInline(t *testing.T) {
	for _, envelope := range []string{
		`{"type":"audio_url","audio_url":{"url":"%s"}}`,
		`{"type":"video_url","video_url":"%s"}`,
		`{"mimeType":"application/pdf","fileUrl":"%s"}`,
		`{"type":"document","source":{"media_type":"application/pdf","url":"%s"}}`,
		`{"type":"file","mime_type":"application/pdf","data":"%s"}`,
	} {
		for _, tc := range []struct{ body, reason string }{
			{"invalid", "invalid"},
			{base64.StdEncoding.EncodeToString([]byte("%PDF-too-large")), "budget"},
		} {
			raw := fmt.Sprintf(envelope, "data:application/pdf;base64,"+tc.body)
			got, err := projectTraceMedia(raw, 5, func(traceMedia) (string, error) {
				t.Fatal("invalid media reached storage")
				return "", nil
			})
			if err != nil || !strings.Contains(got, `"reason":"`+tc.reason+`"`) || strings.Contains(got, "base64") {
				t.Fatalf("unsafe fallback for %s: %s %v", raw, got, err)
			}
		}
		raw := fmt.Sprintf(envelope, "https://example.invalid/media")
		got, err := projectTraceMedia(raw, 5, nil)
		if err != nil || got != raw {
			t.Fatalf("external URL changed: %s %v", got, err)
		}
	}
}

func TestStandardDocumentEnvelopesUseDataPlaneProjection(t *testing.T) {
	b64 := base64.StdEncoding.EncodeToString([]byte("%PDF-example"))
	for _, envelope := range []string{
		`{"type":"input_file","file_data":"data:application/pdf;base64,%s","filename":"example.pdf"}`,
		`{"type":"file","file":{"file_data":"data:application/pdf;base64,%s","filename":"example.pdf"}}`,
		`{"type":"resource","resource":{"mimeType":"application/pdf","blob":"%s","uri":"file:///ignored.pdf"}}`,
	} {
		raw := fmt.Sprintf(envelope, b64)
		projected, err := projectTraceMedia(raw, 16<<20, func(m traceMedia) (string, error) {
			if m.MIME != "application/pdf" || string(m.Data) != "%PDF-example" {
				t.Fatal("document bytes changed")
			}
			return "stored", nil
		})
		if err != nil || !strings.Contains(projected, `"id":"stored"`) || strings.Contains(projected, "base64") || strings.Contains(projected, b64) {
			t.Fatalf("document remained inline: %s %v", projected, err)
		}
		projected, err = projectTraceMedia(raw, 5, func(traceMedia) (string, error) { t.Fatal("oversized document stored"); return "", nil })
		if err != nil || !strings.Contains(projected, `"reason":"budget"`) {
			t.Fatalf("document budget bypass: %s %v", projected, err)
		}
	}
}

func TestMalformedDataURLsNeverReachDisplay(t *testing.T) {
	for _, raw := range []string{
		`{"type":"input_file","file_data":"data:application/pdf;base64,"}`,
		`{"type":"file","mime_type":"application/pdf","data":"data:application/pdf;broken,` + strings.Repeat("x", 1024) + `"}`,
		`"data:application/pdf;broken,AAAA"`,
		`{"type":"image_url","image_url":{"url":"data:image/png,AAAA"}}`,
	} {
		projected, err := projectTraceMedia(raw, 16<<20, func(traceMedia) (string, error) { t.Fatal("invalid data stored"); return "", nil })
		if err != nil || !strings.Contains(projected, `"reason":"invalid"`) || strings.Contains(projected, "data:") {
			t.Fatalf("malformed URL remained inline: %s %v", projected, err)
		}
	}
}
