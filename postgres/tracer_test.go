package postgres

import (
	"testing"
)

func TestExtractSQLOperation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "simple select",
			sql:  "SELECT * FROM users",
			want: "SELECT",
		},
		{
			name: "lowercase insert",
			sql:  "insert into users (id) values ($1)",
			want: "INSERT",
		},
		{
			name: "leading whitespace",
			sql:  "   \n\t  UPDATE users SET name = $1",
			want: "UPDATE",
		},
		{
			name: "block comment",
			sql:  "/* query_name: get_user */ SELECT id FROM users",
			want: "SELECT",
		},
		{
			name: "line comment",
			sql:  "-- explain analyze\nDELETE FROM sessions WHERE expired = true",
			want: "DELETE",
		},
		{
			name: "multiple comments",
			sql:  "/* c1 */ -- c2\n/* c3 */ WITH cte AS (SELECT 1) SELECT * FROM cte",
			want: "WITH",
		},
		{
			name: "empty string",
			sql:  "",
			want: "QUERY",
		},
		{
			name: "whitespace only",
			sql:  "   \t\n  ",
			want: "QUERY",
		},
		{
			name: "unclosed comment",
			sql:  "/* unfinished comment",
			want: "QUERY",
		},
		{
			name: "unclosed line comment",
			sql:  "-- line comment without newline",
			want: "QUERY",
		},
		{
			name: "semicolon immediately after keyword",
			sql:  "BEGIN;",
			want: "BEGIN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractSQLOperation(tt.sql)
			if got != tt.want {
				t.Errorf("extractSQLOperation(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

func BenchmarkExtractSQLOperation(b *testing.B) {
	query := "/* trace_id: 12345 */ SELECT id, name, email, created_at, updated_at FROM users WHERE status = 'active' ORDER BY id DESC LIMIT 50"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = extractSQLOperation(query)
	}
}
