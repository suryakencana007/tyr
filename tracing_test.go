package tyr

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type TracerConnPGSuite struct {
	suite.Suite
	Context     context.Context
	CloseTracer func()
	DB          Factory
	pool        *dockertest.Pool
	Resource    *dockertest.Resource
}

func (t *TracerConnPGSuite) GetResource() *dockertest.Resource {
	return t.Resource
}

func (t *TracerConnPGSuite) SetResource(resource *dockertest.Resource) {
	t.Resource = resource
}

func (t *TracerConnPGSuite) GetPool() *dockertest.Pool {
	return t.pool
}

func (t *TracerConnPGSuite) SetPool(pool *dockertest.Pool) {
	t.pool = pool
}

func (t *TracerConnPGSuite) GetDB() Factory {
	return t.DB
}

func (t *TracerConnPGSuite) SetDB(db Factory) {
	t.DB = db
}

func (t *TracerConnPGSuite) GetContext() context.Context {
	return t.Context
}

func (t *TracerConnPGSuite) SetContext(ctx context.Context) {
	t.Context = ctx
}

func (t *TracerConnPGSuite) GetCloseTracer() {
	t.CloseTracer()
}

func (t *TracerConnPGSuite) SetCloseTracer(closer func()) {
	t.CloseTracer = closer
}

func (t *TracerConnPGSuite) SetupTest() {
	var err error
	var docker = os.Getenv("ENV_DOCKER_URL")
	println(docker)

	t.pool, err = dockertest.NewPool(docker)
	if err != nil {
		panic(fmt.Sprintf("could not connect to docker: %t\n", err))
	}
	err = NewTracerPoolPG(t)
	if err != nil {
		panic(fmt.Sprintf("prepare pg with docker: %v\n", err))
	}
}

func (t *TracerConnPGSuite) TearDownTest() {
	if err := t.DB.Close(); err != nil {
		panic(fmt.Sprintf("could not db close: %v\n", err))
	}

	//t.GetCloseTracer()

	if err := t.pool.RemoveContainerByName("pg_test"); err != nil {
		panic(fmt.Sprintf("could not remove postgres container: %v\n", err))
	}
}

func (t *TracerConnPGSuite) TestMainCommitInFailedTransaction() {
	ts := t.T()
	txn, cancel := t.DB.BeginTx(t.GetContext())
	defer cancel()
	rows, err := txn.Query("SELECT error")
	assert.Error(ts, err)
	if err == nil {
		rows.Close()
		ts.Fatal("expected failure")
	}
	err = txn.Commit()
	assert.Error(ts, err)
	if err != pq.ErrInFailedTransaction {
		ts.Fatalf("expected ErrInFailedTransaction; got %#v", err)
	}
}

func (t *TracerConnPGSuite) TestExecContext() {
	ts := t.T()
	ctx, cancel := t.DB.BeginCtx(t.GetContext())
	defer cancel()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES ($1, $2)`
	result, err := t.DB.ExecContext(ctx, query, args...)
	assert.NoError(ts, err)
	ids, e := result.RowsAffected()
	assert.NoError(ts, e)
	assert.Equal(ts, 1, int(ids))
}

func (t *TracerConnPGSuite) TestPrepareContext() {
	ts := t.T()
	ctx, cancel := t.DB.BeginCtx(t.GetContext())
	defer cancel()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES ($1, $2)`
	stmt, err := t.DB.PrepareContext(ctx, query)
	assert.NoError(ts, err)
	result, e := stmt.ExecContext(ctx, args...)
	assert.NoError(ts, e)
	ids, er := result.RowsAffected()
	assert.NoError(ts, er)
	assert.Equal(ts, 1, int(ids))
}

func (t *TracerConnPGSuite) TestQueryCtxFailed() {
	ts := t.T()
	ctx, cancel := t.DB.BeginCtx(t.GetContext())
	defer cancel()
	err := t.DB.QueryCtx(ctx, func(rows *sql.Rows) error {
		return nil
	}, "SELECT error")
	assert.Error(ts, err)
}

func (t *TracerConnPGSuite) TestGetUserID() {
	ts := t.T()
	names := make([]string, 0)
	ctx, cancel := t.DB.BeginCtx(t.GetContext())
	defer cancel()
	err := t.DB.QueryCtx(ctx, func(rows *sql.Rows) error {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				ts.Fatal(err)
				return err
			}
			names = append(names, name)
		}
		assert.NoError(ts, rows.Err())
		assert.IsType(ts, []string{}, names)
		return nil
	}, "SELECT id FROM users")
	assert.NoError(ts, err)
}

func (t *TracerConnPGSuite) TestWithTransaction() {
	ts := t.T()
	ctx := t.GetContext()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id`
	err := t.DB.WithTransaction(ctx, func(ctxSpan context.Context, tx *sql.Tx) error {
		_, err := t.DB.TxExecContext(ctxSpan, tx, query, args...)
		//suki.Info("WithTransaction",
		//	suki.Field("Affected", affected),
		//)
		if err != nil {
			return err
		}
		_, err = t.DB.TxExecContextWithID(ctxSpan, tx, query, args...)
		//suki.Info("WithTransaction",
		//	suki.Field("LastInsertID", ids),
		//)
		if err != nil {
			return err
		}
		return t.DB.TxCommit(ctxSpan, tx)
	})
	assert.NoError(ts, err)
}

func (t *TracerConnPGSuite) TestWithTransactionFail() {
	ts := t.T()
	ctx := t.GetContext()
	args := []interface{}{
		1001,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES (?, $2)`
	err := t.DB.WithTransaction(ctx, func(ctxSpan context.Context, tx *sql.Tx) error {
		_, err := t.DB.TxExecContext(ctxSpan, tx, query, args...)
		//suki.Info("WithTransaction",
		//	suki.Field("Affected", affected),
		//)
		if err != nil {
			return err
		}
		return t.DB.TxCommit(ctxSpan, tx)
	})
	assert.Error(ts, err)
}

func (t *TracerConnPGSuite) TestContextTimeOutFail() {
	ts := t.T()
	ctx := t.GetContext()
	args := make([]interface{}, 0)
	query := `SELECT pg_sleep(5)`
	err := t.DB.WithTransaction(ctx, func(ctxSpan context.Context, tx *sql.Tx) error {
		_, err := t.DB.TxExecContext(ctxSpan, tx, query, args...)
		//suki.Info("WithTransaction",
		//	suki.Field("Affected", affected),
		//)
		return err
	})
	assert.Error(ts, err)
}

func (t *TracerConnPGSuite) TestMainDB() {
	assert.IsType(t.T(), &dbTracer{}, t.GetDB())
	assert.Contains(t.T(), fmt.Sprintf("%v", getServerVersion(t.T(), t.GetDB())), fmt.Sprintf("%v", 1000))
}

func TestMainTracerPGSuite(t *testing.T) {
	suite.Run(t, new(TracerConnPGSuite))
}

func NewTracerPoolPG(c ConnectionSuite) (err error) {
	t := c.T()
	resource, err := c.GetPool().RunWithOptions(
		&dockertest.RunOptions{
			Name:       "pg_test",
			Repository: "postgres",
			Tag:        "10-alpine",
			Env: []string{
				"POSTGRES_PASSWORD=root",
				"POSTGRES_USER=root",
				"POSTGRES_DB=dev",
			},
		})
	c.SetResource(resource)
	if err != nil {
		return fmt.Errorf("%v", err.Error())
	}
	err = c.GetResource().Expire(5)
	assert.NoError(t, err)
	purge := func() error {
		return c.GetPool().Purge(c.GetResource())
	}

	//tracer, closeTracer, err := suki.Tracer("tracer_store_test", "1.3.0", suki.With())
	//if err != nil {
	//	suki.Errorf("tracing is disconnected: %s", err)
	//}
	//
	//c.SetCloseTracer(closeTracer)
	//
	//opentracing.SetGlobalTracer(tracer)

	if errPool := c.GetPool().Retry(func() error {
		connInfo := fmt.Sprintf(`postgresql://%s:%s@%s:%s/%s?sslmode=disable`,
			"root",
			"root",
			"docker",
			c.GetResource().GetPort("5432/tcp"),
			"dev",
		)

		conn := SqlConn{
			POSTGRES,
			connInfo,
			3,
			5,
			500,
		}

		db, e := NewNoSlave(conn)
		if e != nil {
			panic(err.Error())
		}

		c.SetDB(NewTracerConn(db))
		return c.GetDB().Ping()
	}); errPool != nil {
		_ = purge()
		return fmt.Errorf("check connection %v", errPool.Error())
	}

	c.SetContext(context.Background())

	//serverSpan := opentracing.StartSpan("Pool Testing PG")
	//defer serverSpan.Finish()
	//
	//c.SetContext(opentracing.ContextWithSpan(c.GetContext(), serverSpan))

	ctx, cancel := c.GetDB().BeginCtx(c.GetContext())
	defer cancel()

	if _, err := c.GetDB().ExecContext(ctx, pgSchema); err != nil {
		_ = purge()
		return fmt.Errorf("failed to create schema %v", err.Error())
	}

	return nil
}
