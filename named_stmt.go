package sqlxy

import (
	"context"
	"database/sql"

	"github.com/jmoiron/sqlx"
)

var _ NamedStmt = (*namedStmt)(nil)

type namedStmt struct {
	db    *DB
	stmts []*sqlx.NamedStmt
}

func (s *namedStmt) Close() error {
	return scan(len(s.stmts), func(i int) error {
		return s.stmts[i].Close()
	})
}

func (s *namedStmt) Exec(ctx context.Context, arg any) sql.Result {
	return s.stmts[0].MustExecContext(ctx, arg)
}

func (s *namedStmt) Query(ctx context.Context, arg any) (*sqlx.Rows, error) {
	return s.stmts[s.indexForSalve()].QueryxContext(ctx, arg)
}

func (s *namedStmt) QueryRow(ctx context.Context, arg any) *sqlx.Row {
	return s.stmts[s.indexForSalve()].QueryRowxContext(ctx, arg)
}

func (s *namedStmt) Select(ctx context.Context, dest any, arg any) error {
	return s.stmts[s.indexForSalve()].SelectContext(ctx, dest, arg)
}

func (s *namedStmt) Get(ctx context.Context, dest any, arg any) error {
	return s.stmts[s.indexForSalve()].GetContext(ctx, dest, arg)
}

func (s *namedStmt) indexForSalve() int {
	return s.db.slave(len(s.db.conns))
}
