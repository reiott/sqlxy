package sqlxy

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync/atomic"
	"time"

	"github.com/jmoiron/sqlx"
)

// DB is a logical database with multiple underlying physical databases
// forming a single master multiple slaves topology.
// Reads and writes are automatically directed to the correct physical db.
type DB struct {
	conns []*sqlx.DB // Physical databases
	count uint64     // Monotonically incrementing counter on each query
}

// Close closes all physical databases concurrently, releasing any open resources.
func (db *DB) Close() error {
	return scan(len(db.conns), func(i int) error {
		return db.conns[i].Close()
	})
}

// SetMaxIdleConns sets the maximum number of connections in the idle
// connection pool for each underlying physical db.
// If MaxOpenConns is greater than 0 but less than the new MaxIdleConns then the
// new MaxIdleConns will be reduced to match the MaxOpenConns limit
// If n <= 0, no idle connections are retained.
func (db *DB) SetMaxIdleConns(n int) {
	for i := range db.conns {
		db.conns[i].SetMaxIdleConns(n)
	}
}

// SetMaxOpenConns sets the maximum number of open connections
// to each physical database.
// If MaxIdleConns is greater than 0 and the new MaxOpenConns
// is less than MaxIdleConns, then MaxIdleConns will be reduced to match
// the new MaxOpenConns limit. If n <= 0, then there is no limit on the number
// of open connections. The default is 0 (unlimited).
func (db *DB) SetMaxOpenConns(n int) {
	for i := range db.conns {
		db.conns[i].SetMaxOpenConns(n)
	}
}

// SetConnMaxLifetime sets the maximum amount of time a connection may be reused.
// Expired connections may be closed lazily before reuse.
// If d <= 0, connections are reused forever.
func (db *DB) SetConnMaxLifetime(d time.Duration) {
	for i := range db.conns {
		db.conns[i].SetConnMaxLifetime(d)
	}
}

// Ping verifies if a connection to each physical database is still alive,
// establishing a connection if necessary.
func (db *DB) Ping(ctx context.Context) error {
	return scan(len(db.conns), func(i int) error {
		return db.conns[i].PingContext(ctx)
	})
}

// Driver returns the physical database's underlying driver.
func (db *DB) Driver() driver.Driver {
	return db.Master().Driver()
}

// Master returns the master physical database
func (db *DB) Master() *sqlx.DB {
	return db.conns[0]
}

// Slave returns one of the physical databases which is a slave
func (db *DB) Slave() *sqlx.DB {
	return db.conns[db.slave(len(db.conns))]
}

// Beginx begins a transaction and returns an *sqlx.Tx instead of an *sql.Tx.
func (db *DB) Begin() (*sqlx.Tx, error) {
	return db.Master().Beginx()
}

// BeginTx begins a transaction and returns an *sqlx.Tx instead of an
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	return db.Master().BeginTxx(ctx, opts)
}

// Exec (panic) runs Exec using this database.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Exec(ctx context.Context, query string, args ...any) sql.Result {
	return db.Master().MustExecContext(ctx, query, args...)
}

// Prepare returns an Stmt instead of a sqlx.Stmt.
func (db *DB) Prepare(ctx context.Context, query string) (Stmt, error) {
	stmts := make([]*sqlx.Stmt, len(db.conns))

	err := scan(len(db.conns), func(i int) (err error) {
		stmts[i], err = db.conns[i].PreparexContext(ctx, query)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &stmt{db: db, stmts: stmts}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
// Query uses a slave as the physical db.
func (db *DB) Query(ctx context.Context, query string, args ...any) (*sqlx.Rows, error) {
	return db.Slave().QueryxContext(ctx, query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
// QueryRow always return a non-nil value.
// Errors are deferred until Row's Scan method is called.
// QueryRow uses a slave as the physical db.
func (db *DB) QueryRow(ctx context.Context, query string, args ...any) *sqlx.Row {
	return db.Slave().QueryRowxContext(ctx, query, args...)
}

// Select using this DB.
// Any placeholder parameters are replaced with supplied args.
func (db *DB) Select(ctx context.Context, dest any, query string, args ...any) error {
	return db.Slave().SelectContext(ctx, dest, query, args...)
}

// Get using this DB.
// Any placeholder parameters are replaced with supplied args.
// An error is returned if the result set is empty.
func (db *DB) Get(ctx context.Context, dest any, query string, args ...any) error {
	return db.Slave().GetContext(ctx, dest, query, args...)
}

// NamedExec using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedExec(ctx context.Context, query string, arg any) (sql.Result, error) {
	return db.Master().NamedExecContext(ctx, query, arg)
}

// NamedQuery using this DB.
// Any named placeholder parameters are replaced with fields from arg.
func (db *DB) NamedQuery(ctx context.Context, query string, arg any) (*sqlx.Rows, error) {
	return db.Slave().NamedQueryContext(ctx, query, arg)
}

// NamedPrepare returns an NamedStmt
func (db *DB) NamedPrepare(ctx context.Context, query string) (NamedStmt, error) {
	stmts := make([]*sqlx.NamedStmt, len(db.conns))

	err := scan(len(db.conns), func(i int) (err error) {
		stmts[i], err = db.conns[i].PrepareNamedContext(ctx, query)
		return err
	})

	if err != nil {
		return nil, err
	}

	return &namedStmt{db: db, stmts: stmts}, nil
}

func (db *DB) slave(n int) int {
	if n <= 1 {
		return 0
	}
	return int(1 + (atomic.AddUint64(&db.count, 1) % uint64(n-1)))
}
