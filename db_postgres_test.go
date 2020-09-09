package tyr

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/opentracing/opentracing-go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/suryakencana007/mimir"
)

const (
	pgSchema = `CREATE TABLE IF NOT EXISTS users (
id integer NOT NULL, 
name varchar(255) NOT NULL
);`
)

type ConnPGSuite struct {
	suite.Suite
	Context     context.Context
	CloseTracer func()
	DB          Factory
	pool        *dockertest.Pool
	Resource    *dockertest.Resource
}

func (s *ConnPGSuite) GetResource() *dockertest.Resource {
	return s.Resource
}

func (s *ConnPGSuite) SetResource(resource *dockertest.Resource) {
	s.Resource = resource
}

func (s *ConnPGSuite) GetPool() *dockertest.Pool {
	return s.pool
}

func (s *ConnPGSuite) SetPool(pool *dockertest.Pool) {
	s.pool = pool
}

func (s *ConnPGSuite) GetDB() Factory {
	return s.DB
}

func (s *ConnPGSuite) SetDB(db Factory) {
	s.DB = db
}

func (s *ConnPGSuite) GetContext() context.Context {
	return s.Context
}

func (s *ConnPGSuite) SetContext(ctx context.Context) {
	s.Context = ctx
}

func (s *ConnPGSuite) GetCloseTracer() {
	s.CloseTracer()
}

func (s *ConnPGSuite) SetCloseTracer(closer func()) {
	s.CloseTracer = closer
}

func (s *ConnPGSuite) SetupTest() {
	var err error
	var docker = os.Getenv("ENV_DOCKER_URL")
	println(docker)

	s.pool, err = dockertest.NewPool(docker)
	if err != nil {
		panic(fmt.Sprintf("could not connect to docker: %s\n", err))
	}
	err = NewPoolPG(s)
	if err != nil {
		panic(fmt.Sprintf("prepare pg with docker: %v\n", err))
	}
}

func (s *ConnPGSuite) TearDownTest() {
	if err := s.DB.Close(); err != nil {
		panic(fmt.Sprintf("could not db close: %v\n", err))
	}

	//s.GetCloseTracer()

	if err := s.pool.RemoveContainerByName("pg_test"); err != nil {
		panic(fmt.Sprintf("could not remove postgres container: %v\n", err))
	}
}

func (s *ConnPGSuite) TestMainCommitInFailedTransaction() {
	t := s.T()
	txn, cancel := s.DB.BeginTx(s.GetContext())
	defer cancel()
	rows, err := txn.Query("SELECT error")
	assert.Error(t, err)
	if err == nil {
		rows.Close()
		t.Fatal("expected failure")
	}
	err = txn.Commit()
	assert.Error(t, err)
	if err != pq.ErrInFailedTransaction {
		t.Fatalf("expected ErrInFailedTransaction; got %#v", err)
	}
}

func (s *ConnPGSuite) TestExecContext() {
	t := s.T()
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES ($1, $2)`
	result, err := s.DB.ExecContext(ctx, query, args...)
	assert.NoError(t, err)
	ids, e := result.RowsAffected()
	assert.NoError(t, e)
	assert.Equal(t, 1, int(ids))
}

func (s *ConnPGSuite) TestPrepareContext() {
	t := s.T()
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES ($1, $2)`
	stmt, err := s.DB.PrepareContext(ctx, query)
	assert.NoError(t, err)
	result, e := stmt.ExecContext(ctx, args...)
	assert.NoError(t, e)
	ids, er := result.RowsAffected()
	assert.NoError(t, er)
	assert.Equal(t, 1, int(ids))
}

func (s *ConnPGSuite) TestQueryCtxFailed() {
	t := s.T()
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	err := s.DB.QueryCtx(ctx, func(rows *sql.Rows) error {
		return nil
	}, "SELECT error")
	assert.Error(t, err)
}

func (s *ConnPGSuite) TestGetUserID() {
	t := s.T()
	names := make([]string, 0)
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	err := s.DB.QueryCtx(ctx, func(rows *sql.Rows) error {
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				t.Fatal(err)
				return err
			}
			names = append(names, name)
		}
		assert.NoError(t, rows.Err())
		assert.IsType(t, []string{}, names)
		return nil
	}, "SELECT id FROM users")
	assert.NoError(t, err)
}

func (s *ConnPGSuite) TestWithTransaction() {
	t := s.T()
	ctx := s.GetContext()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id`
	err := s.DB.WithTransaction(ctx, func(ctxSpan context.Context, tx *sql.Tx) error {
		affected, err := s.DB.TxExecContext(ctxSpan, tx, query, args...)
		mimir.Info("WithTransaction",
			mimir.Field("Affected", affected),
		)
		if err != nil {
			return err
		}
		ids, err := s.DB.TxExecContextWithID(ctxSpan, tx, query, args...)
		mimir.Info("WithTransaction",
			mimir.Field("LastInsertID", ids),
		)
		if err != nil {
			return err
		}
		return s.DB.TxCommit(ctxSpan, tx)
	})
	assert.NoError(t, err)
}

func (s *ConnPGSuite) TestWithTransactionFail() {
	t := s.T()
	ctx := s.GetContext()
	args := []interface{}{
		1001,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES (?, $2)`
	err := s.DB.WithTransaction(ctx, func(ctxSpan context.Context, tx *sql.Tx) error {
		affected, err := s.DB.TxExecContext(ctxSpan, tx, query, args...)
		mimir.Info("WithTransaction",
			mimir.Field("Affected", affected),
		)
		if err != nil {
			return err
		}
		return s.DB.TxCommit(ctxSpan, tx)
	})
	assert.Error(t, err)
}

func (s *ConnPGSuite) TestContextTimeOutFail() {
	t := s.T()
	ctx := s.GetContext()
	args := make([]interface{}, 0)
	query := `SELECT pg_sleep(5)`
	err := s.DB.WithTransaction(ctx, func(ctxSpan context.Context, tx *sql.Tx) error {
		affected, err := s.DB.TxExecContext(ctxSpan, tx, query, args...)
		mimir.Info("WithTransaction",
			mimir.Field("Affected", affected),
		)
		return err
	})
	assert.Error(t, err)
}

func (s *ConnPGSuite) TestMainDB() {
	assert.IsType(s.T(), &DB{}, s.GetDB())
	assert.Contains(s.T(), fmt.Sprintf("%v", getServerVersion(s.T(), s.GetDB())), fmt.Sprintf("%v", 1000))
}

func TestMainPGSuite(t *testing.T) {
	suite.Run(t, new(ConnPGSuite))
}

type ConnectionSuite interface {
	T() *testing.T
	GetResource() *dockertest.Resource
	SetResource(resource *dockertest.Resource)
	GetPool() *dockertest.Pool
	SetPool(pool *dockertest.Pool)
	GetDB() Factory
	SetDB(factory Factory)
	SetContext(ctx context.Context)
	GetContext() context.Context
	GetCloseTracer()
	SetCloseTracer(closer func())
}

func NewPoolPG(c ConnectionSuite) (err error) {
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
		c.SetDB(db)
		return c.GetDB().Ping()
	}); errPool != nil {
		_ = purge()
		return fmt.Errorf("check connection %v", errPool.Error())
	}

	tracer, closeTracer, err := mimir.Tracer("store_test", "1.3.0", mimir.With())
	if err != nil {
		mimir.Errorf("tracing is disconnected: %s", err)
	}

	c.SetCloseTracer(closeTracer)

	opentracing.SetGlobalTracer(tracer)

	c.SetContext(context.Background())

	serverSpan := opentracing.StartSpan("Pool Testing PG")
	defer serverSpan.Finish()

	c.SetContext(opentracing.ContextWithSpan(c.GetContext(), serverSpan))

	ctx, cancel := c.GetDB().BeginCtx(c.GetContext())
	defer cancel()

	if _, err := c.GetDB().ExecContext(ctx, pgSchema); err != nil {
		_ = purge()
		return fmt.Errorf("failed to create schema %v", err.Error())
	}

	return nil
}

func getServerVersion(t *testing.T, db Factory) int {
	var (
		version int
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.QueryRowCtx(ctx, func(rs *sql.Row) error {
		return rs.Scan(&version)
	}, `SHOW server_version_num;`)
	if err != nil {
		t.Log(err)
	}
	return version
}
