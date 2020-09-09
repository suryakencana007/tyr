package tyr

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/suryakencana007/mimir"
)

const (
	mysqlSchema = `CREATE TABLE IF NOT EXISTS users (
id integer NOT NULL, 
name varchar(255) NOT NULL
);`
)

type ConnMYSuite struct {
	suite.Suite
	Context     context.Context
	CloseTracer func()
	DB          Factory
	pool        *dockertest.Pool
	Resource    *dockertest.Resource
}

func (s *ConnMYSuite) GetResource() *dockertest.Resource {
	return s.Resource
}

func (s *ConnMYSuite) SetResource(resource *dockertest.Resource) {
	s.Resource = resource
}

func (s *ConnMYSuite) GetPool() *dockertest.Pool {
	return s.pool
}

func (s *ConnMYSuite) SetPool(pool *dockertest.Pool) {
	s.pool = pool
}

func (s *ConnMYSuite) GetDB() Factory {
	return s.DB
}

func (s *ConnMYSuite) SetDB(db Factory) {
	s.DB = db
}

func (s *ConnMYSuite) GetContext() context.Context {
	return s.Context
}

func (s *ConnMYSuite) SetContext(ctx context.Context) {
	s.Context = ctx
}

func (s *ConnMYSuite) GetCloseTracer() {
	s.CloseTracer()
}

func (s *ConnMYSuite) SetCloseTracer(closer func()) {
	s.CloseTracer = closer
}

func (s *ConnMYSuite) SetupTest() {
	var err error
	var docker = os.Getenv("ENV_DOCKER_URL")
	println(docker)

	s.pool, err = dockertest.NewPool(docker)
	s.pool.MaxWait = time.Minute * 2
	if err != nil {
		panic(fmt.Sprintf("could not connect to docker: %s\n", err))
	}
	err = NewPoolMY(s)
	if err != nil {
		panic(fmt.Sprintf("prepare pg with docker: %v\n", err))
	}
}

func (s *ConnMYSuite) TearDownTest() {
	if err := s.DB.Close(); err != nil {
		panic(fmt.Sprintf("could not db close: %v\n", err))
	}

	//s.GetCloseTracer()

	if err := s.pool.RemoveContainerByName("mysql_test"); err != nil {
		panic(fmt.Sprintf("could not remove mysql container: %v\n", err))
	}
}

func (s *ConnMYSuite) TestMainCommitInFailedTransaction() {
	t := s.T()
	txn, cancel := s.DB.BeginTx(s.GetContext())
	defer cancel()
	rows, err := txn.Query("SELECT f()")
	assert.Error(t, err)
	if err == nil {
		rows.Close()
		t.Fatal("expected failure")
	}
}

func (s *ConnMYSuite) TestExecContext() {
	t := s.T()
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES (?, ?)`
	result, err := s.DB.ExecContext(ctx, query, args...)
	assert.NoError(t, err)
	ids, e := result.RowsAffected()
	assert.NoError(t, e)
	assert.Equal(t, 1, int(ids))
}

func (s *ConnMYSuite) TestPrepareContext() {
	t := s.T()
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES (?, ?)`
	stmt, err := s.DB.PrepareContext(ctx, query)
	assert.NoError(t, err)
	result, e := stmt.ExecContext(ctx, args...)
	assert.NoError(t, e)
	ids, er := result.RowsAffected()
	assert.NoError(t, er)
	assert.Equal(t, 1, int(ids))
}

func (s *ConnMYSuite) TestQueryCtxFailed() {
	t := s.T()
	ctx, cancel := s.DB.BeginCtx(s.GetContext())
	defer cancel()
	err := s.DB.QueryCtx(ctx, func(rows *sql.Rows) error {
		return nil
	}, "SELECT error")
	assert.Error(t, err)
}

func (s *ConnMYSuite) TestGetUserID() {
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

func (s *ConnMYSuite) TestWithTransaction() {
	t := s.T()
	ctx := s.GetContext()
	args := []interface{}{
		1003,
		"TEST WithTransaction Func",
	}
	query := `INSERT INTO users (id, name) VALUES (?, ?) RETURNING id`
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

func (s *ConnMYSuite) TestWithTransactionFail() {
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

func (s *ConnMYSuite) TestMainDB() {
	assert.IsType(s.T(), &DB{}, s.GetDB())
	assert.Contains(s.T(), fmt.Sprintf("%v", getMYServerVersion(s.T(), s.GetDB())), "10.5.5")
}

func TestMainMYSuite(t *testing.T) {
	suite.Run(t, new(ConnMYSuite))
}

func NewPoolMY(c ConnectionSuite) (err error) {
	//t := c.T()
	resource, err := c.GetPool().RunWithOptions(
		&dockertest.RunOptions{
			Name:       "mysql_test",
			Repository: "mariadb",
			Tag:        "10.5",
			Env: []string{
				"MYSQL_ROOT_PASSWORD=root",
				"MYSQL_ROOT_USER=root",
				"MYSQL_DATABASE=dev",
			},
		})
	c.SetResource(resource)
	if err != nil {
		return fmt.Errorf("%v", err.Error())
	}

	purge := func() error {
		return c.GetPool().Purge(c.GetResource())
	}

	if errPool := c.GetPool().Retry(func() error {
		connInfo := fmt.Sprintf(`%s:%s@(%s:%s)/%s`,
			"root",
			"root",
			"docker",
			c.GetResource().GetPort("3306/tcp"),
			"dev",
		)

		conn := SqlConn{
			MYSQL,
			connInfo,
			3,
			5,
			500,
		}

		db, e := NewNoSlave(conn)
		if e != nil {
			panic(err.Error())
		}
		db.Master.SetConnMaxLifetime(time.Minute * 30)
		db.Master.SetMaxOpenConns(10)
		db.Master.SetMaxIdleConns(0)
		db.Master.SetConnMaxLifetime(time.Minute * 30)
		db.Slave.SetMaxOpenConns(10)
		db.Slave.SetMaxIdleConns(0)
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

	serverSpan := opentracing.StartSpan("Pool Testing Mysql")
	defer serverSpan.Finish()

	c.SetContext(opentracing.ContextWithSpan(c.GetContext(), serverSpan))

	ctx, cancel := c.GetDB().BeginCtx(c.GetContext())
	defer cancel()

	if _, err := c.GetDB().ExecContext(ctx, mysqlSchema); err != nil {
		_ = purge()
		return fmt.Errorf("failed to create schema %v", err.Error())
	}

	return nil
}

func getMYServerVersion(t *testing.T, db Factory) string {
	var (
		version string
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := db.QueryRowCtx(ctx, func(rs *sql.Row) error {
		return rs.Scan(&version)
	}, `SELECT VERSION()`)
	if err != nil {
		t.Log(err)
	}
	return version
}
