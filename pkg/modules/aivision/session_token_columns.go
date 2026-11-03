package aivision

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/SigNoz/signoz/pkg/ingest"
	"regexp"
	"strings"
)

// Versioned STORED expressions deliberately preserve the existing JSON query
// CASE, including JSON type checks and DECIMAL(65,0). They are derived by the
// database on every row_json write, including writes from an older publisher.
// Source revision/context/semantics are still checked by the ordinary coverage
// guard in the same read transaction. No application-maintained helper is used.
const (
	sessionTokenGeneratedValueColumn    = ingest.SessionTokenGeneratedValueColumn
	sessionTokenGeneratedBillableColumn = ingest.SessionTokenGeneratedBillableColumn
	sessionTokenGeneratedValueExpr      = ingest.SessionTokenGeneratedValueExpr
	sessionTokenGeneratedBillableExpr   = ingest.SessionTokenGeneratedBillableExpr
	// These are the full metadata forms observed on seekdb 1.4.0 and OceanBase
	// CE 4.4.2.1. Unknown rewrites fail closed; parentheses are not discarded.
	sessionTokenGeneratedStoredValueExpr    = "(case when (JSON_EXTRACT(`row_json`,'$.coverage.billableCalls') = 0) then 0 when ((JSON_EXTRACT(`row_json`,'$.coverage.billableCalls') > 0) and (JSON_TYPE(JSON_EXTRACT(`row_json`,'$.usage.total')) in ('INTEGER','UNSIGNED INTEGER','DOUBLE','DECIMAL'))) then cast(JSON_UNQUOTE(JSON_EXTRACT(`row_json`,'$.usage.total')) as number(65,0)) else NULL end)"
	sessionTokenGeneratedStoredBillableExpr = "(case when (JSON_EXTRACT(`row_json`,'$.coverage.billableCalls') > 0) then 1 else 0 end)"
)

var sessionTokenGeneratedPrefix = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,31}$`)

// sessionTokenGeneratedDDL is also retained for the explicit maintenance SQL
// compatibility tests. Standard startup owns the versioned migration; query
// requests only inspect capability and never run DDL.
func sessionTokenGeneratedDDL(prefix string) (string, error) {
	if !sessionTokenGeneratedPrefix.MatchString(prefix) {
		return "", fmt.Errorf("invalid session token generated-column table prefix")
	}
	return "ALTER TABLE `" + prefix + "_trace_projections` ADD COLUMN " + sessionTokenGeneratedValueColumn + " DECIMAL(65,0) GENERATED ALWAYS AS (" + sessionTokenGeneratedValueExpr + ") STORED,ADD COLUMN " + sessionTokenGeneratedBillableColumn + " TINYINT GENERATED ALWAYS AS (" + sessionTokenGeneratedBillableExpr + ") STORED", nil
}

// generatedExpressionTokens normalizes whitespace, SQL keyword/function case,
// row_json identifier quoting, and the engine's NUMBER synonym for DECIMAL.
// Quoted string contents, operators, arguments and parentheses stay exact.
// It is deliberately a narrow metadata comparer, never a general SQL parser.
func generatedExpressionTokens(expression string) ([]string, bool) {
	tokens := []string{}
	for i := 0; i < len(expression); {
		c := expression[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			i++
			continue
		}
		if c == '\'' {
			start := i
			i++
			closed := false
			for i < len(expression) {
				if expression[i] == '\'' {
					if i+1 < len(expression) && expression[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					closed = true
					break
				}
				if expression[i] == '\\' {
					return nil, false // Expected paths/type literals contain no escapes.
				}
				i++
			}
			if !closed {
				return nil, false
			}
			tokens = append(tokens, expression[start:i])
			continue
		}
		if c == '`' {
			end := strings.IndexByte(expression[i+1:], '`')
			if end < 0 || expression[i+1:i+1+end] != "row_json" {
				return nil, false
			}
			tokens = append(tokens, "row_json")
			i += end + 2
			continue
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' {
			start := i
			for i < len(expression) && (expression[i] >= 'a' && expression[i] <= 'z' || expression[i] >= 'A' && expression[i] <= 'Z' || expression[i] >= '0' && expression[i] <= '9' || expression[i] == '_') {
				i++
			}
			word := strings.ToLower(expression[start:i])
			if word == "number" && len(tokens) > 0 && tokens[len(tokens)-1] == "as" {
				word = "decimal"
			}
			tokens = append(tokens, word)
			continue
		}
		if c >= '0' && c <= '9' {
			start := i
			for i < len(expression) && expression[i] >= '0' && expression[i] <= '9' {
				i++
			}
			tokens = append(tokens, expression[start:i])
			continue
		}
		if strings.ContainsRune("(),=>", rune(c)) {
			tokens = append(tokens, string(c))
			i++
			continue
		}
		return nil, false
	}
	return tokens, len(tokens) > 0
}

func generatedExpressionMatches(actual, source, stored string) bool {
	actualTokens, ok := generatedExpressionTokens(actual)
	if !ok {
		return false
	}
	for _, candidate := range []string{source, "(" + source + ")", stored} {
		expectedTokens, valid := generatedExpressionTokens(candidate)
		if valid && strings.Join(actualTokens, "\x00") == strings.Join(expectedTokens, "\x00") {
			return true
		}
	}
	return false
}

// sessionTokenGeneratedCapability is a read-only per-request gate. Missing or
// mismatched optional columns select the existing JSON strategy. Inspection,
// permission, transport and scan errors remain errors, never empty success.
func (s *oceanBaseStore) sessionTokenGeneratedCapability(ctx context.Context, tx *sql.Tx) (bool, string, error) {
	if !sessionTokenGeneratedPrefix.MatchString(s.cfg.TablePrefix) {
		return false, "", fmt.Errorf("invalid session token generated-column table prefix")
	}
	rows, err := tx.QueryContext(ctx, "SELECT COLUMN_NAME,COLUMN_TYPE,EXTRA,GENERATION_EXPRESSION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME IN (?,?)", s.cfg.TablePrefix+"_trace_projections", sessionTokenGeneratedValueColumn, sessionTokenGeneratedBillableColumn)
	if err != nil {
		return false, "", fmt.Errorf("inspect session token generated columns: %w", err)
	}
	defer rows.Close()
	seen := map[string]bool{}
	for rows.Next() {
		var name, columnType, extra string
		var expression sql.NullString
		if err := rows.Scan(&name, &columnType, &extra, &expression); err != nil {
			return false, "", fmt.Errorf("scan session token generated columns: %w", err)
		}
		if seen[name] {
			return false, "session_token_columns_mismatch", nil
		}
		seen[name] = true
		if !expression.Valid || strings.ToUpper(strings.Join(strings.Fields(extra), " ")) != "STORED GENERATED" {
			return false, "session_token_columns_mismatch", nil
		}
		columnType = strings.ToLower(strings.ReplaceAll(columnType, " ", ""))
		switch name {
		case sessionTokenGeneratedValueColumn:
			if columnType != "decimal(65,0)" || !generatedExpressionMatches(expression.String, sessionTokenGeneratedValueExpr, sessionTokenGeneratedStoredValueExpr) {
				return false, "session_token_columns_mismatch", nil
			}
		case sessionTokenGeneratedBillableColumn:
			if (columnType != "tinyint" && columnType != "tinyint(4)") || !generatedExpressionMatches(expression.String, sessionTokenGeneratedBillableExpr, sessionTokenGeneratedStoredBillableExpr) {
				return false, "session_token_columns_mismatch", nil
			}
		default:
			return false, "session_token_columns_mismatch", nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, "", fmt.Errorf("read session token generated columns: %w", err)
	}
	if !seen[sessionTokenGeneratedValueColumn] || !seen[sessionTokenGeneratedBillableColumn] {
		return false, "session_token_columns_missing", nil
	}
	return true, "ready", nil
}
