package tyr

import (
	"context"
	"database/sql"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type dbTracer struct {
	*DB
}

func (d *dbTracer) BeginCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return d.DB.BeginCtx(ctx)
}

func (d *dbTracer) BeginTx(ctx context.Context) (*sql.Tx, context.CancelFunc) {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.BeginTx")
	tx, err := d.DB.BeginTx(ctxSpan)
	span.Finish()
	return tx, err
}

func (d *dbTracer) QueryCtx(ctx context.Context, fn func(rs *sql.Rows) error, query string, args ...interface{}) error {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.QueryCtx")
	ext.DBStatement.Set(span, query)
	ext.DBInstance.Set(span, "Slave")
	ext.DBType.Set(span, "sql")
	span.SetTag("db.values", args)

	err := d.DB.QueryCtx(ctxSpan, fn, query, args...)
	span.Finish()
	return err
}

func (d *dbTracer) QueryRowCtx(ctx context.Context, fn func(rs *sql.Row) error, query string, args ...interface{}) error {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.QueryRowCtx")
	ext.DBStatement.Set(span, query)
	ext.DBInstance.Set(span, "Slave")
	ext.DBType.Set(span, "sql")
	span.SetTag("db.values", args)

	err := d.DB.QueryRowCtx(ctxSpan, fn, query, args...)
	span.Finish()
	return err
}

func (d *dbTracer) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.ExecContext")
	ext.DBStatement.Set(span, query)
	ext.DBInstance.Set(span, "Master")
	ext.DBType.Set(span, "sql")
	span.SetTag("db.values", args)

	result, err := d.DB.ExecContext(ctxSpan, query, args...)
	span.Finish()
	return result, err
}

func (d *dbTracer) TxExecContextWithID(ctx context.Context, tx *sql.Tx, query string, args ...interface{}) (ids interface{}, err error) {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.TxExecContextWithID")
	ext.DBStatement.Set(span, query)
	ext.DBInstance.Set(span, "Master")
	ext.DBType.Set(span, "sql")
	span.SetTag("db.values", args)

	ids, err = d.DB.TxExecContextWithID(ctxSpan, tx, query, args...)
	span.Finish()
	return ids, err
}

func (d *dbTracer) TxExecContext(ctx context.Context, tx *sql.Tx, query string, args ...interface{}) (affected int64, err error) {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.TxExecContext")
	ext.DBStatement.Set(span, query)
	ext.DBInstance.Set(span, "Master")
	ext.DBType.Set(span, "sql")
	span.SetTag("db.values", args)

	affected, err = d.DB.TxExecContext(ctxSpan, tx, query, args...)
	span.Finish()
	return affected, err
}

func (d *dbTracer) TxCommit(ctx context.Context, tx *sql.Tx) error {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.TxCommit")
	ext.DBInstance.Set(span, "Master")
	ext.DBType.Set(span, "sql")

	err := d.DB.TxCommit(ctxSpan, tx)
	span.Finish()
	return err
}

func (d *dbTracer) WithTransaction(ctx context.Context, fn func(ctx context.Context, tx *sql.Tx) error) error {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.WithTransaction")
	ext.DBInstance.Set(span, "Master")
	ext.DBType.Set(span, "sql")

	err := d.DB.WithTransaction(ctxSpan, fn)
	span.Finish()
	return err
}

func (d *dbTracer) PrepareContext(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	span, ctxSpan := opentracing.StartSpanFromContext(ctx, "tracer.PrepareContext")
	ext.DBStatement.Set(span, query)
	ext.DBInstance.Set(span, "Master")
	ext.DBType.Set(span, "sql")

	stmt, err = d.DB.PrepareContext(ctxSpan, query)
	span.Finish()
	return stmt, err
}

func NewTracerConn(conn *DB) Factory {
	return &dbTracer{conn}
}
