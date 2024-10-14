package sqlxy

import (
	"context"
	"database/sql"
	"strings"

	"github.com/jmoiron/sqlx"
)

// Open concurrently opens each underlying physical db.
// source must be a semi-comma separated list of DSNs with the first
// one being used as the master and the rest as slaves.
func Open(ctx context.Context, driver string, source string) (*DB, error) {
	conns := strings.Split(source, ";")
	db := &DB{conns: make([]*sqlx.DB, len(conns))}

	err := scan(len(db.conns), func(i int) (err error) {
		db.conns[i], err = sqlx.ConnectContext(ctx, driver, conns[i])
		return err
	})

	if err != nil {
		return nil, err
	}

	return db, nil
}

type Stmt interface {
	Close() error
	Exec(context.Context, ...any) sql.Result
	Query(context.Context, ...any) (*sqlx.Rows, error)
	QueryRow(context.Context, ...any) *sqlx.Row
	Select(context.Context, any, ...any) error
	Get(context.Context, any, ...any) error
}

type NamedStmt interface {
	Close() error
	Exec(context.Context, any) sql.Result
	Query(context.Context, any) (*sqlx.Rows, error)
	QueryRow(context.Context, any) *sqlx.Row
	Select(context.Context, any, any) error
	Get(context.Context, any, any) error
}

func scan(n int, fn func(i int) error) error {
	errors := make(chan error, n)

	var i int
	for i = 0; i < n; i++ {
		go func(i int) { errors <- fn(i) }(i)
	}

	var err, innerErr error
	for i = 0; i < cap(errors); i++ {
		if innerErr = <-errors; innerErr != nil {
			err = innerErr
		}
	}

	return err
}
