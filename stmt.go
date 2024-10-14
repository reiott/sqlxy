package sqlxy

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

var _ Stmt = (*stmt)(nil)

type stmt struct {
	db    *DB
	stmts []*sqlx.Stmt
}

func (s *stmt) Close() error {
	return scan(len(s.stmts), func(i int) error {
		return s.stmts[i].Close()
	})
}

func (s *stmt) Exec(ctx context.Context, args ...any) sql.Result {
	return s.stmts[0].MustExecContext(ctx, args...)
}

func (s *stmt) Query(ctx context.Context, args ...any) (*sqlx.Rows, error) {
	return s.stmts[s.indexForSalve()].QueryxContext(ctx, args...)
}

func (s *stmt) QueryRow(ctx context.Context, args ...any) *sqlx.Row {
	return s.stmts[s.indexForSalve()].QueryRowxContext(ctx, args...)
}

func (s *stmt) Select(ctx context.Context, dest any, args ...any) error {
	return s.stmts[s.indexForSalve()].SelectContext(ctx, dest, args...)
}

func (s *stmt) Get(ctx context.Context, dest any, args ...any) error {
	return s.stmts[s.indexForSalve()].GetContext(ctx, dest, args...)
}

func (s *stmt) indexForSalve() int {
	return s.db.slave(len(s.db.conns))
}
