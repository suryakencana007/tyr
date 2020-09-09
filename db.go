package tyr

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/suryakencana007/mimir"
)

const (
	POSTGRES string = "postgres"
	MYSQL    string = "mysql"
)

type TxArgs struct {
	Query string
	Args  []interface{}
}

type Factory interface {
	Close() error
	Ping() error
	BeginCtx(ctx context.Context) (context.Context, context.CancelFunc)
	BeginTx(ctx context.Context) (*sql.Tx, context.CancelFunc)
	QueryCtx(ctx context.Context, fn func(rs *sql.Rows) error, query string, args ...interface{}) error
	QueryRowCtx(ctx context.Context, fn func(rs *sql.Row) error, query string, args ...interface{}) error
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	TxExecContextWithID(ctx context.Context, tx *sql.Tx, query string, args ...interface{}) (ids interface{}, err error)
	TxExecContext(ctx context.Context, tx *sql.Tx, query string, args ...interface{}) (affected int64, err error)
	TxCommit(ctx context.Context, tx *sql.Tx) error
	WithTransaction(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error) error
	PrepareContext(ctx context.Context, query string) (stmt *sql.Stmt, err error)
}

type DB struct {
	Master     *sql.DB
	Slave      *sql.DB
	RetryCount int
	Timeout    int
	Concurrent int
}

func (r *DB) Ping() error {
	if err := r.Master.Ping(); err != nil {
		mimir.Errorf("event Ping Master got error: %v", err.Error())
		return err
	}
	if err := r.Slave.Ping(); err != nil {
		mimir.Errorf("event Ping Slave got error: %v", err.Error())
		return err
	}
	return nil
}

func (r *DB) Close() error {
	if e := r.Master.Close(); e != nil {
		return e
	}
	if e := r.Slave.Close(); e != nil {
		return e
	}

	return nil
}

func (r *DB) BeginCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, time.Duration(r.Timeout)*time.Second)
}

func (r *DB) BeginTx(ctx context.Context) (*sql.Tx, context.CancelFunc) {
	logger := mimir.For(ctx)

	logger.Info("BeginTx Running...")

	c, cancel := context.WithTimeout(ctx, time.Duration(r.Timeout)*time.Second)
	tx, err := r.Master.BeginTx(c, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		cancel()
		logger.Errorf("event BeginTx got error: %v", err.Error())
		return nil, nil
	}

	return tx, cancel
}

func (r *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	logger := mimir.For(ctx)
	logger.Info("ExecContext Running...", mimir.Field("query", query))
	return r.Master.ExecContext(ctx, query, args...)
}

func (r *DB) PrepareContext(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	logger := mimir.For(ctx)
	logger.Info("PrepareContext Running...", mimir.Field("query", query))
	return r.Master.PrepareContext(ctx, query)
}

func (r *DB) QueryRowCtx(ctx context.Context, fn func(rs *sql.Row) error, query string, args ...interface{}) error {
	logger := mimir.For(ctx)
	logger.Info("QueryRowCtx Running...",
		mimir.Field("query", query),
		mimir.Field("args", args),
	)

	if r.Slave == nil {
		logger.With(
			mimir.Field("query", query),
			mimir.Field("args", args),
		).Errorf("event QueryRowCtx: the database connection is nil")

		return fmt.Errorf("event QueryRowCtx: cannot access your db connection")
	}

	rs := r.Slave.QueryRowContext(ctx, query, args...)
	if err := fn(rs); err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("event QueryRowCtx:  result not found",
				mimir.Field("query", query),
				mimir.Field("args", args))

			return nil
		}

		logger.With(
			mimir.Field("query", query),
			mimir.Field("args", args),
		).Error("event QueryRowCtx: query row failed")

		return err
	}

	return nil
}

func (r *DB) QueryCtx(ctx context.Context, fn func(rs *sql.Rows) error, query string, args ...interface{}) error {
	logger := mimir.For(ctx)
	logger.Info("QueryCtx Running...",
		mimir.Field("query", query),
		mimir.Field("args", args),
	)
	if r.Slave == nil {
		logger.With(
			mimir.Field("query", query),
			mimir.Field("args", args),
		).Error("event QueryCtx: the database connection is nil")

		return fmt.Errorf("event QueryCtx: cannot access your db connection")
	}

	rs, err := r.Slave.QueryContext(ctx, query, args...)
	if err != nil {
		logger.Warn("event QueryContext: query failed",
			mimir.Field("query", query),
			mimir.Field("args", args),
		)

		return err
	}

	defer func() {
		err = rs.Close()
	}()

	if err := fn(rs); err != nil {
		if err == sql.ErrNoRows {
			logger.Warn("event QueryCtx: result not found",
				mimir.Field("query", query),
				mimir.Field("args", args),
			)

			return nil
		}

		logger.With(
			mimir.Field("query", query),
			mimir.Field("args", args),
		).Error("event QueryCtx: query row failed")

		return err
	}

	return nil
}

func (r *DB) TxExecContextWithID(ctx context.Context, tx *sql.Tx, query string, args ...interface{}) (ids interface{}, err error) {
	logger := mimir.For(ctx)
	logger.Info("TxExecContextWithID Running...",
		mimir.Field("query", query),
		mimir.Field("args", args),
	)

	if strings.Contains(query, "RETURNING") {
		stmt, er := tx.PrepareContext(ctx, query)
		if er != nil {
			logger.With(
				mimir.Field("error", er.Error()),
				mimir.Field("query", query),
				mimir.Field("args", args),
			).Error("TxExecContextWithID: PrepareContext")

			return nil, er
		}

		if er := stmt.QueryRowContext(ctx, args...).Scan(&ids); er != nil {
			logger.With(
				mimir.Field("error", er.Error()),
				mimir.Field("query", query),
				mimir.Field("args", args),
			).Error("TxExecContextWithID: QueryRowContext")

			return nil, er
		}

		if errStmt := stmt.Close(); errStmt != nil {
			logger.With(
				mimir.Field("error", errStmt.Error()),
				mimir.Field("query", query),
				mimir.Field("args", args),
			).Error("TxExecContextWithID: Statement Close")

			return nil, errStmt
		}

		return ids, nil
	}

	err = fmt.Errorf("query has no RETUNING id syntax")

	logger.With(
		mimir.Field("error", err.Error()),
		mimir.Field("query", query),
		mimir.Field("args", args),
	).Error("TxExecContextWithID:")

	return nil, err
}

func (r *DB) TxExecContext(ctx context.Context, tx *sql.Tx, query string, args ...interface{}) (affected int64, err error) {
	logger := mimir.For(ctx)
	logger.Info("TxExecContext Running...",
		mimir.Field("query", query),
		mimir.Field("args", args),
	)

	stmt, e := tx.PrepareContext(ctx, query)
	if e != nil {
		logger.With(
			mimir.Field("error", e.Error()),
			mimir.Field("query", query),
			mimir.Field("args", args),
		).Error("event PrepareContext")

		return affected, e
	}

	result, erResult := stmt.ExecContext(ctx, args...)
	if erResult != nil {
		//mimir.With(
		//	mimir.Field("error", erResult.Error()),
		//	mimir.Field("query", query),
		//	mimir.Field("args", args),
		//).Error("ExecContextWithID: result")
		return affected, erResult
	}

	af, erAffected := result.RowsAffected()
	if erAffected != nil {
		//mimir.With(
		//	mimir.Field("error", erAffected.Error()),
		//).Error("TxExecContext: RowsAffected")

		return af, err
	}

	erStmt := stmt.Close()
	if erStmt != nil {
		//mimir.With(
		//	mimir.Field("error", erStmt.Error()),
		//	mimir.Field("query", query),
		//	mimir.Field("args", args),
		//).Error("TxExecContext:")

		return affected, err
	}

	return affected, nil
}

func (r *DB) TxCommit(ctx context.Context, tx *sql.Tx) error {
	logger := mimir.For(ctx)

	// commit db transaction
	if er := tx.Commit(); er != nil {
		if err := tx.Rollback(); err != nil {
			logger.With(
				mimir.Field("error", err.Error()),
			).Error("event Rollback")

			return err
		} // rollback if fail query statement

		logger.With(
			mimir.Field("error", er.Error()),
		).Error("event TxCommit")

		return er
	}

	return nil
}

func (r *DB) WithTransaction(ctx context.Context, fn func(context.Context, *sql.Tx) error) error {
	tx, cancel := r.BeginTx(ctx)
	defer cancel()

	return fn(ctx, tx)
}

type SqlConn struct {
	Driver, ConnStr                 string
	RetryCount, Timeout, Concurrent int
}

func New(master, slave SqlConn) (*DB, error) {
	// Master Connection
	m, err := sql.Open(master.Driver, master.ConnStr)
	if err != nil {
		//mimir.Error(err.Error())
		panic(fmt.Errorf("cannot access your db master connection").Error())
	}

	// Slave Connection
	s, err := sql.Open(slave.Driver, slave.ConnStr)
	if err != nil {
		//mimir.Error(err.Error())
		panic(fmt.Errorf("cannot access your db slave connection").Error())
	}

	return &DB{
		m,
		s,
		master.RetryCount,
		master.Timeout,
		master.Concurrent,
	}, nil
}

func NewNoSlave(conn SqlConn) (*DB, error) {
	return New(conn, conn)
}
