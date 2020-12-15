package upcoin

import (
	"collector/db"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"time"
)

type task interface {
	do() error
}

type getKlinesTask struct {
	collector *Collector

	symbol    string
	interval  string
	startTime time.Time
	limit     int
}

func (t *getKlinesTask) String() string {
	return fmt.Sprintf("%+v", *t)
}

func (t *getKlinesTask) do() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	collection, exists := t.collector.collections.Get(intervalSymbol{
		symbol:   t.symbol,
		interval: t.interval,
	})
	if !exists {
		return errors.New("symbol with interval does not exist")
	}

	klines, err := t.collector.binanceApi.GetKlines(ctx, t.symbol, t.interval, collection.lastKline.OpenTime, t.limit)
	if err != nil {
		return err
	}

	err = t.collector.pg.Kline.BulkInsert(ctx, klines...)
	if err != nil {
		return err
	}
	return nil
}

type Collector struct {
	binanceApi *binanceApi
	pg         *db.Postgres

	collections *collections
	tasks       chan task

	stop chan chan error
}

func NewCollector(pg *db.Postgres) (*Collector, error) {
	binanceApi, err := newBinanceApi()
	if err != nil {
		return nil, errors.Wrap(err, "NewCollector()")
	}

	collect := &Collector{
		binanceApi:  binanceApi,
		pg:          pg,
		collections: newCollections(),
		tasks:       make(chan task, 512),
		stop:        make(chan chan error),
	}

	intervalSymbol := intervalSymbol{
		symbol:   "BTCUSDT",
		interval: "15m",
	}
	collection := &collection{}
	collect.collections.Put(intervalSymbol, collection)

	go collect.run()

	return collect, nil
}

func (c *Collector) run() {

	c.tasks <- &getKlinesTask{
		collector: c,
		symbol:    "BTCUSDT",
		interval:  "15m",
		startTime: time.Time{},
		limit:     10,
	}

	for {
		select {
		case <-c.stop:
			return
		case task, ok := <-c.tasks:
			if !ok {
				return
			}
			select {
			case <-c.stop:
				zap.S().Warnw(
					"stop working tasks",
					"abortedTasks", cap(c.tasks),
				)
				return
			default:
				err := task.do()
				if err != nil {
					zap.S().Error(err)
				}
			}
		}
	}
}

func (c *Collector) Shutdown() error {
	close(c.stop)
	return nil
}
