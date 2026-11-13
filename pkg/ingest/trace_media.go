package ingest

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"io"
	"strings"
	"unicode/utf8"
)

// traceMedia describes validated bytes discovered in a typed message block.
// It contains no tenant identity: that comes exclusively from the gateway.
type traceMedia struct {
	MIME   string
	SHA256 string
	Data   []byte
}

// traceMediaSource resolves only documented media envelopes. File names and
// MCP URIs are metadata: they never cause file reads or network requests.
func traceMediaSource(block map[string]any) map[string]any {
	for _, key := range []string{"source", "input_audio", "file", "resource"} {
		if source, ok := block[key].(map[string]any); ok {
			return source
		}
	}
	return block
}

// extractTraceMedia recognizes only explicit model media blocks. It does not
// fetch URLs, guess untyped base64 or interpret redaction placeholders.
func extractTraceMedia(raw string, maxBytes int) []traceMedia {
	if maxBytes <= 0 {
		return nil
	}
	upper := strings.ToUpper(raw)
	if strings.Contains(upper, "[REDACTED") || strings.Contains(upper, "[FILTERED_") {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var value any
	if dec.Decode(&value) != nil || dec.Decode(new(any)) != io.EOF {
		return nil
	}
	var result []traceMedia
	var visit func(any, int)
	visit = func(value any, depth int) {
		if depth > 16 || len(result) >= 32 {
			return
		}
		switch v := value.(type) {
		case string:
			if header, encoded, ok := strings.Cut(strings.TrimPrefix(v, "data:"), ";base64,"); ok && strings.HasPrefix(v, "data:") {
				visit(map[string]any{"type": "media", "mime_type": header, "data": encoded}, depth+1)
			}
		case []any:
			for _, item := range v {
				visit(item, depth+1)
			}
		case map[string]any:
			kind, _ := v["type"].(string)
			mime, encoded := "", ""
			if kind == "image_url" || kind == "input_image" || kind == "audio_url" || kind == "video_url" {
				source := v["image_url"]
				if kind == "audio_url" {
					source = v["audio_url"]
				}
				if kind == "video_url" {
					source = v["video_url"]
				}
				if obj, ok := source.(map[string]any); ok {
					source = obj["url"]
				}
				if u, ok := source.(string); ok && strings.HasPrefix(u, "data:") {
					if header, body, ok := strings.Cut(u[5:], ";base64,"); ok {
						mime, encoded = header, body
					}
				}
			} else if kind == "input_audio" {
				if source, ok := v["input_audio"].(map[string]any); ok {
					format, _ := source["format"].(string)
					mime = map[string]string{"wav": "audio/wav", "mp3": "audio/mpeg", "ogg": "audio/ogg", "flac": "audio/flac"}[format]
					encoded, _ = source["data"].(string)
				}
			} else if kind == "" || kind == "base64" || kind == "image" || kind == "audio" || kind == "video" || kind == "document" || kind == "file" || kind == "input_file" || kind == "resource" || kind == "media" || kind == "text" {
				source := traceMediaSource(v)
				if nested, ok := v["source"].(map[string]any); ok {
					if typ, ok := nested["type"].(string); ok && typ != "base64" {
						return
					}
				}
				for _, key := range []string{"media_type", "mime_type", "mimeType"} {
					if mime, _ = source[key].(string); mime != "" {
						break
					}
				}
				for _, key := range []string{"data", "blob", "file_data", "url", "file_url", "imageUrl", "audioUrl", "videoUrl", "fileUrl"} {
					if encoded, _ = source[key].(string); encoded != "" {
						break
					}
				}
				if strings.HasPrefix(encoded, "data:") {
					header, body, ok := strings.Cut(encoded[5:], ";base64,")
					if ok && (mime == "" || strings.EqualFold(header, mime)) {
						mime = header
						encoded = body
					} else {
						encoded = ""
					}
				}
			}
			if mime != "" && encoded != "" {
				mime = strings.ToLower(mime)
				if len(encoded) > base64.StdEncoding.EncodedLen(maxBytes) {
					return
				}
				data, err := base64.StdEncoding.Strict().DecodeString(encoded)
				if err != nil || len(data) == 0 || len(data) > maxBytes || !validTraceMedia(mime, data) {
					return
				}
				hash := sha256.Sum256(data)
				result = append(result, traceMedia{mime, hex.EncodeToString(hash[:]), data})
				return
			}
			for _, key := range []string{"messages", "content", "parts", "input", "output", "data", "kwargs"} {
				visit(v[key], depth+1)
			}
		}
	}
	visit(value, 0)
	return result
}

// projectTraceMedia replaces only validated media blocks with references from
// the data-plane store. The original attribute remains available to its caller
// for exact raw reconstruction and transactional persistence.
func projectTraceMedia(raw string, maxBytes int, resolve func(traceMedia) (string, error), scopes ...mediaScope) (string, error) {
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.UseNumber()
	var root any
	if dec.Decode(&root) != nil || dec.Decode(new(any)) != io.EOF {
		if strings.HasPrefix(raw, "data:") {
			root = raw
		} else {
			return raw, nil
		}
	}
	changed := false
	var visit func(any, int) (any, error)
	visit = func(value any, depth int) (any, error) {
		if depth > 16 {
			return value, nil
		}
		leaf := false
		switch v := value.(type) {
		case string:
			// Serialized SDK message content is a known envelope, not a scan
			// of arbitrary business fields. Keep its string representation.
			if strings.HasPrefix(strings.TrimSpace(v), "{") || strings.HasPrefix(strings.TrimSpace(v), "[") {
				dec := json.NewDecoder(strings.NewReader(v))
				dec.UseNumber()
				var nested any
				if dec.Decode(&nested) == nil && dec.Decode(new(any)) == io.EOF {
					next, err := visit(nested, depth+1)
					if err != nil {
						return nil, err
					}
					if encoded, err := json.Marshal(next); err == nil && string(encoded) != v {
						return string(encoded), nil
					}
				}
			}
			leaf = strings.HasPrefix(v, "data:")
		case map[string]any:
			kind, _ := v["type"].(string)
			if kind == "" && (v["data"] != nil || v["url"] != nil || v["imageUrl"] != nil || v["audioUrl"] != nil || v["videoUrl"] != nil || v["fileUrl"] != nil) && (v["mime_type"] != nil || v["mimeType"] != nil || v["media_type"] != nil) {
				leaf = true
			}
			switch kind {
			case "image_url", "input_image", "audio_url", "video_url", "input_audio", "base64", "image", "audio", "video", "document", "file", "input_file", "resource", "media", "text":
				leaf = true
			}
		}
		if leaf {
			encoded, err := json.Marshal(value)
			upper := strings.ToUpper(string(encoded))
			if strings.Contains(upper, "[REDACTED") || strings.Contains(upper, "[FILTERED_") {
				changed = true
				return map[string]any{"type": "aivision_media_error", "reason": "filtered"}, nil
			}
			if err != nil {
				return nil, err
			}
			// Persist exact originals even when a typed media value cannot be
			// previewed. Large or malformed Base64 must not reach display reads.
			var typed map[string]any
			switch v := value.(type) {
			case string:
				if header, body, ok := strings.Cut(strings.TrimPrefix(v, "data:"), ";base64,"); ok {
					typed = map[string]any{"mime_type": header, "data": body}
				}
			case map[string]any:
				typed = v
			}
			source := traceMediaSource(typed)
			body := ""
			dataURL := false
			for _, key := range []string{"data", "blob", "file_data"} {
				if candidate, ok := source[key].(string); ok && candidate != "" {
					body = candidate
					break
				}
			}
			// Classify failures for every supported URL envelope, not only
			// image_url. Otherwise invalid/oversized data URLs remain inline.
			for _, key := range []string{"image_url", "audio_url", "video_url", "url", "file_url", "imageUrl", "audioUrl", "videoUrl", "fileUrl"} {
				candidate := source[key]
				if nested, ok := candidate.(map[string]any); ok {
					candidate = nested["url"]
				}
				if rawURL, ok := candidate.(string); ok && strings.HasPrefix(rawURL, "data:") {
					body = rawURL
					dataURL = true
					break
				}
			}
			if strings.HasPrefix(body, "data:") {
				dataURL = true
				_, body, _ = strings.Cut(body, ";base64,")
			}
			if raw, ok := value.(string); ok && strings.HasPrefix(raw, "data:") {
				dataURL = true
			}
			if strings.HasPrefix(body, "https://") || strings.HasPrefix(body, "http://") {
				body = ""
			}
			reason := ""
			decodedSize := base64.StdEncoding.DecodedLen(len(body))
			if strings.HasSuffix(body, "==") {
				decodedSize -= 2
			} else if strings.HasSuffix(body, "=") {
				decodedSize--
			}
			if len(body) > base64.StdEncoding.EncodedLen(maxBytes) || decodedSize > maxBytes {
				reason = "budget"
			}
			items := extractTraceMedia(string(encoded), maxBytes)
			if (body != "" || dataURL) && len(items) == 0 && reason == "" {
				reason = "invalid"
			}
			if reason != "" {
				changed = true
				return map[string]any{"type": "aivision_media_error", "reason": reason}, nil
			}
			if len(items) == 1 {
				id, err := resolve(items[0])
				if err != nil {
					return nil, err
				}
				if id != "" {
					changed = true
					ref := map[string]any{"type": "aivision_attachment", "id": id, "mimeType": items[0].MIME, "sha256": items[0].SHA256}
					if len(scopes) > 0 {
						ref["spaceId"] = scopes[0].space
						ref["agentId"] = scopes[0].agent
					}
					return ref, nil
				}
			}
			return value, nil
		}
		switch v := value.(type) {
		case []any:
			for i, child := range v {
				next, err := visit(child, depth+1)
				if err != nil {
					return nil, err
				}
				v[i] = next
			}
		case map[string]any:
			for _, key := range []string{"messages", "content", "parts", "input", "output", "data", "kwargs"} {
				if child, ok := v[key]; ok {
					next, err := visit(child, depth+1)
					if err != nil {
						return nil, err
					}
					v[key] = next
				}
			}
		}
		return value, nil
	}
	projected, err := visit(root, 0)
	if err != nil {
		return "", err
	}
	if !changed {
		return raw, nil
	}
	encoded, err := json.Marshal(projected)
	return string(encoded), err
}

func validTraceMedia(mime string, data []byte) bool {
	switch mime {
	case "image/svg+xml":
		return safeTraceSVG(data)
	case "image/bmp", "image/x-ms-bmp":
		return bytes.HasPrefix(data, []byte("BM"))
	case "audio/flac", "audio/x-flac":
		return bytes.HasPrefix(data, []byte("fLaC"))
	case "audio/mp4", "audio/x-m4a", "video/quicktime":
		return len(data) >= 12 && string(data[4:8]) == "ftyp"
	case "audio/webm":
		return bytes.HasPrefix(data, []byte("\x1a\x45\xdf\xa3")) && bytes.Contains(data[:min(len(data), 64)], []byte("webm"))
	case "image/png":
		return bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n"))
	case "image/jpeg":
		return bytes.HasPrefix(data, []byte("\xff\xd8\xff"))
	case "image/gif":
		return bytes.HasPrefix(data, []byte("GIF87a")) || bytes.HasPrefix(data, []byte("GIF89a"))
	case "image/webp":
		return len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WEBP"
	case "image/avif":
		return len(data) >= 16 && string(data[4:8]) == "ftyp" && (string(data[8:12]) == "avif" || string(data[8:12]) == "avis" || bytes.Contains(data[16:min(len(data), 64)], []byte("avif")) || bytes.Contains(data[16:min(len(data), 64)], []byte("avis")))
	case "audio/wav", "audio/x-wav", "audio/vnd.wave":
		return len(data) >= 12 && string(data[:4]) == "RIFF" && string(data[8:12]) == "WAVE"
	case "audio/ogg", "audio/vorbis", "video/ogg":
		return bytes.HasPrefix(data, []byte("OggS"))
	case "audio/mpeg":
		return bytes.HasPrefix(data, []byte("ID3")) || len(data) > 1 && data[0] == 255 && data[1]&224 == 224
	case "application/pdf":
		return bytes.HasPrefix(data, []byte("%PDF-"))
	case "video/mp4":
		return len(data) >= 12 && string(data[4:8]) == "ftyp"
	case "video/webm":
		return bytes.HasPrefix(data, []byte("\x1a\x45\xdf\xa3")) && bytes.Contains(data[:min(len(data), 64)], []byte("webm"))
	case "text/plain", "text/markdown", "application/json":
		return utf8.Valid(data)
	}
	return false
}

// Only inert, self-contained SVG is stored for image rendering.
func safeTraceSVG(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	root, depth := false, 0
	for {
		token, err := dec.Token()
		if err == io.EOF {
			return root && depth == 0
		}
		if err != nil {
			return false
		}
		switch t := token.(type) {
		case xml.Directive:
			return false
		case xml.ProcInst:
			if t.Target != "xml" {
				return false
			}
		case xml.StartElement:
			if depth == 0 {
				if root || t.Name.Local != "svg" {
					return false
				}
				root = true
			}
			depth++
			switch strings.ToLower(t.Name.Local) {
			case "svg", "g", "path", "rect", "circle", "ellipse", "line", "polyline", "polygon", "text", "tspan", "defs", "lineargradient", "radialgradient", "stop", "clippath", "mask", "title", "desc":
			default:
				return false
			}
			for _, a := range t.Attr {
				key, val := strings.ToLower(a.Name.Local), strings.ToLower(a.Value)
				if strings.HasPrefix(key, "on") || key == "href" || key == "style" || strings.Contains(val, "url(") || strings.Contains(val, "javascript:") {
					return false
				}
			}
		case xml.EndElement:
			depth--
		}
	}
}
