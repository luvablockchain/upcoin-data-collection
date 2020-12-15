package db

import (
	"context"
	"github.com/pkg/errors"
	"time"
)

type initFunc func() error

func (pg *Postgres) init() error {
	initFuncs := []initFunc{
		pg.initSchemas,
		pg.initEnums,
		pg.initKlineTable,
	}
	for _, initFunc := range initFuncs {
		if err := initFunc(); err != nil {
			return err
		}
	}
	return nil
}

func (pg *Postgres) initSchemas() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stmt :=
		`
		CREATE SCHEMA IF NOT EXISTS binance;
		`

	_, err := pg.pool.Exec(ctx, stmt)
	if err != nil {
		return errors.Wrap(err, `initSchemas()`)
	}

	return nil
}

func (pg *Postgres) initEnums() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stmt :=
		`
		DO $$ BEGIN
			CREATE TYPE binance.kline_interval AS ENUM (
				'1m', '3m', '5m', '15m', '30m',
				'1h', '2h', '4h', '6h', '8h', '12h',
				'1d', '3d', '1w', '1M'
			);
		EXCEPTION
			WHEN duplicate_object THEN NULL;
		END $$;
		`

	_, err := pg.pool.Exec(ctx, stmt)
	if err != nil {
		return errors.Wrap(err, `initKlineTable()`)
	}

	return nil
}

func (pg *Postgres) initKlineTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stmt :=
		`
		CREATE TABLE IF NOT EXISTS binance.kline (
		    id								BIGINT NOT NULL ,
		    symbol 							text NOT NULL ,
		    time_interval    				binance.kline_interval NOT NULL ,
		    available						bool NOT NULL DEFAULT FALSE,
		    open_time 						timestamptz NOT NULL ,
		    close_time						timestamptz NOT NULL ,
		    open_price						numeric(20,8) NOT NULL DEFAULT 0,
		    close_price 					numeric(20,8) NOT NULL DEFAULT 0,
		    high_price 						numeric(20,8) NOT NULL DEFAULT 0,
		    low_price 						numeric(20,8) NOT NULL DEFAULT 0,
		    trades  						bigint NOT NULL DEFAULT 0,
		    volume 							numeric(20,8) NOT NULL DEFAULT 0,
		    quote_asset_volume 				numeric(20,8) NOT NULL DEFAULT 0,
		    taker_buy_base_asset_volume 	numeric(20,8) NOT NULL DEFAULT 0,
		    taker_buy_quote_asset_volume	numeric(20,8) NOT NULL DEFAULT 0,
		    PRIMARY KEY ( id, symbol, time_interval ) ,
		    CHECK ( open_time < kline.close_time )
		) PARTITION BY LIST (symbol);

		CREATE OR REPLACE FUNCTION binance.valid_kline() RETURNS TRIGGER AS 
		    $$
		    BEGIN 
				IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
				    NEW.symbol = upper(NEW.symbol);
				END IF;
				RETURN NEW;
			END;
		    $$
		LANGUAGE plpgsql;

		DROP TRIGGER IF EXISTS before_ins_or_upd
			ON binance.kline;

		CREATE TRIGGER before_ins_or_upd
		    BEFORE INSERT OR UPDATE 
		    ON binance.kline
		    FOR EACH ROW 
		    WHEN ( pg_trigger_depth() = 0 )
		    EXECUTE PROCEDURE binance.valid_kline();
		
		CREATE TABLE IF NOT EXISTS binance.btcusdt PARTITION OF binance.kline
			FOR VALUES IN ('BTCUSDT') PARTITION BY LIST (time_interval);

		CREATE TABLE IF NOT EXISTS binance.btcusdt_minute_15 PARTITION OF binance.btcusdt
			FOR VALUES IN ('15m')
		
		`

	_, err := pg.pool.Exec(ctx, stmt)
	if err != nil {
		return errors.Wrap(err, `initKlineTable()`)
	}
	return nil
}

