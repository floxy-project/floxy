package floxy

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"
)

//go:embed migrations_sqlite/*.sql
var sqliteMigrationFiles embed.FS

// RunSQLiteMigrations executes embedded SQLite migrations in lexical order within a single transaction.
func RunSQLiteMigrations(ctx context.Context, db *sql.DB) error {
	entries, err := fs.ReadDir(sqliteMigrationFiles, "migrations_sqlite")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Sort by filename to ensure deterministic order (e.g., 0001_..., 0002_...)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	tx, err := db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := sqliteMigrationFiles.ReadFile("migrations_sqlite/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		content := string(b)
		// Very simple split by semicolon; adequate for our DDL files
		stmts := splitSQLStatements(content)
		for _, stmt := range stmts {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				if isSQLiteDuplicateColumnError(err) {
					continue
				}
				return fmt.Errorf("exec migration %s: %w", e.Name(), err)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	tx = nil
	return nil
}

func splitSQLStatements(sqlText string) []string {
	parts := strings.Split(sqlText, ";")
	res := make([]string, 0, len(parts))
	for _, p := range parts {
		stmt := strings.TrimSpace(p)
		if stmt == "" {
			continue
		}
		// Skip pure comment-only lines
		lines := strings.Split(stmt, "\n")
		allComment := true
		for _, ln := range lines {
			l := strings.TrimSpace(ln)
			if l == "" {
				continue
			}
			if !strings.HasPrefix(l, "--") {
				allComment = false
				break
			}
		}
		if allComment {
			continue
		}
		res = append(res, stmt)
	}
	return res
}

func isSQLiteDuplicateColumnError(err error) bool {
	return strings.Contains(err.Error(), "duplicate column name")
}
