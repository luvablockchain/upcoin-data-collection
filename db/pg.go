package db

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"time"
)

type PgConfig struct {
	Host string
	Port string
	Usr  string
	Pwd  string
	Db   string
}

func (c *PgConfig) toConnStr() string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", c.Usr, c.Pwd, c.Host, c.Port, c.Db)
}

type Postgres struct {
	pool *pgxpool.Pool

	Kline *KlineService

	close chan chan error
}

func NewPostgres(ctx context.Context, config *PgConfig) (*Postgres, error) {
	if config == nil {
		return nil, errors.New("postgresql's config is nil")
	}

	zap.S().Infow(
		"try connecting to postgresql",
		"host", config.Host,
		"port", config.Port,
		"db", config.Db,
	)

	pool, err := pgxpool.Connect(ctx, config.toConnStr())
	if err != nil {
		return nil, errors.Wrap(err, "pgxpool.Connect()")
	}

	postgres := &Postgres{
		pool:  pool,
		Kline: &KlineService{pool: pool},
		close: make(chan chan error),
	}

	if err := postgres.init(); err != nil {
		return nil, err
	}
	zap.S().Info("initialized postgresql's schema successfully")

	go postgres.run()

	zap.S().Infow(
		"connected to postgresql",
		"host", config.Host,
		"port", config.Port,
		"db", config.Db)

	return postgres, nil
}

func (pg *Postgres) run() {
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-ticker.C:
			//
		case result := <-pg.close:
			// Release all resources
			pg.pool.Close()
			// Send result
			result <- nil
			return
		}
	}
}

func (pg *Postgres) Shutdown() error {
	result := make(chan error)
	go func() {
		pg.close <- result
	}()
	return <-result
}
