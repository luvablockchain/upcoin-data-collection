package main

import (
	"collector/db"
	"collector/timex"
	"collector/upcoin"
	"collector/util"
	"context"
	"go.uber.org/zap"
	"time"
)

// Start program
func main() {
	// Init logger
	if err := util.InitGlobalLogger(); err != nil {
		zap.S().Error(err)
		return
	}

	t := time.Unix(2678400, 0).Add(-time.Nanosecond)
	zap.S().Debug(t.UTC(), t.Weekday())
	zap.S().Debug(timex.WeeksSinceUnix(t))

	zap.S().Debug(timex.MonthsSinceUnix(t))

	return

	// Try connecting to postgres
	pgConfig := &db.PgConfig{
		Host: util.GetEnv("POSTGRES_HOST", "localhost"),
		Port: util.GetEnv("POSTGRES_PORT", "5000"),
		Usr:  util.GetEnv("POSTGRES_USER", "upcoin"),
		Pwd:  util.GetEnv("POSTGRES_PASSWORD", "P@1234uc@"),
		Db:   util.GetEnv("POSTGRES_DB", "upcoin_data_collection"),
	}
	pg, err := db.NewPostgres(context.Background(), pgConfig)
	if err != nil {
		zap.S().Error(err)
		return
	}

	// Upcoin collector
	collector, err := upcoin.NewCollector(pg)
	if err != nil {
		zap.S().Error(err)
		return
	}

	// Shutdown
	shutdown := util.ShutdownListen()
	for {
		select {
		case <-shutdown:
			// Release resources
			if err := pg.Shutdown(); err != nil {
				zap.S().Error(err)
			}
			if err := collector.Shutdown(); err != nil {
				zap.S().Error(err)
			}
			// Exit
			return
		}
	}
}
