package db

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

type KlineService struct {
	pool *pgxpool.Pool
}

type Kline struct {
	Symbol                   string
	Interval                 string
	Available                bool
	OpenTime                 time.Time
	CloseTime                time.Time
	OpenPrice                decimal.Decimal
	ClosePrice               decimal.Decimal
	HighPrice                decimal.Decimal
	LowPrice                 decimal.Decimal
	Trades                   int64
	Volume                   decimal.Decimal
	QuoteAssetVolume         decimal.Decimal
	TakerBuyBaseAssetVolume  decimal.Decimal
	TakerBuyQuoteAssetVolume decimal.Decimal
}

func (k KlineService) GetLastKlineEach(ctx context.Context, symbol, interval string) ([]*Kline, error) {
	stmt :=
		`
		SELECT
			symbol,
		    time_interval,
		    available,
		    open_time,
		    close_time,
		    open_price,
		   	close_price,
		    high_price,
		    low_price,
		    trades,
			volume,
			quote_asset_volume,
			taker_buy_base_asset_volume,
			taker_buy_quote_asset_volume
		FROM
			binance.kline
		WHERE 
	  		symbol = $1 
		  	AND time_interval = $2
		`

	rows, err := k.pool.Query(ctx, stmt, symbol, interval)
	if err != nil {
		return nil, errors.Wrap(err, "GetLastKlineEach() Query()")
	}

	klines := make([]*Kline, 0 , 1000)
	for rows.Next() {
		kline := &Kline{}
		err := rows.Scan(
			&kline.Symbol,
			&kline.Interval,
			&kline.Available,
			&kline.OpenTime,
			&kline.CloseTime,
			&kline.OpenPrice,
			&kline.ClosePrice,
			&kline.HighPrice,
			&kline.LowPrice,
			&kline.Trades,
			&kline.Volume,
			&kline.QuoteAssetVolume,
			&kline.TakerBuyBaseAssetVolume,
			&kline.TakerBuyQuoteAssetVolume,
		)
		if err != nil {
			return nil, errors.Wrap(err, "GetLastKlineEach() Scan()")
		}
	}
	return klines, nil
}

// Times are inclusive
func (k KlineService) GetFromTo(ctx context.Context, start time.Time, end time.Time) ([]*Kline, error) {
	stmt :=
		`
		SELECT (
		    symbol
		)
		FROM
			binance.kline
		`

	rows, err := k.pool.Query(ctx,stmt)
	if err != nil {
		return nil, errors.Wrap(err, "GetFromTo()")
	}

	klines := make([]*Kline, 0, 100)
	for rows.Next() {
		kline := &Kline{}
		err := rows.Scan(
			kline.Symbol,
		)
		if err != nil {
			return nil, errors.Wrap(err, "GetFromTo()")
		}
		klines = append(klines, kline)
	}

	return klines, nil
}

// BulkInsert inserts klines into binance.kline table
func (k KlineService) BulkInsert(ctx context.Context, klines ...*Kline) error {
	var stmtBuff strings.Builder
	stmtBuff.Grow(1024 + len(klines)*256)
	stmtBuff.WriteString(
		`
		INSERT INTO binance.kline (
			symbol,
			time_interval,
			available,
			open_time,
			close_time,
			open_price,
			close_price,
			high_price,
			low_price,
			trades,
			volume,
			taker_buy_base_asset_volume,
			taker_buy_quote_asset_volume
		) VALUES 
		`,
	)

	stmtArgs := make([]interface{}, 0, len(klines)*13)	// number of klines * number of kline's params

	for i, kline := range klines {
		args := []interface{}{
			kline.Symbol, 						// $1	+1
			kline.Interval,						// $2	+2
			kline.Available,					// $3	+3
			kline.OpenTime,						// $4	+4
			kline.CloseTime,					// $5	+5
			kline.OpenPrice,					// $6	+6
			kline.ClosePrice,					// $7	+7
			kline.HighPrice,					// $8	+8
			kline.LowPrice,						// $9	+9
			kline.Trades,						// $10	+10
			kline.Volume,						// $11	+11
			kline.TakerBuyBaseAssetVolume,		// $12	+12
			kline.TakerBuyQuoteAssetVolume,		// $13	+13
		}
		stmtArgs = append(stmtArgs, args...)
		// Placeholders
		stmtBuff.WriteString(placeholdersForValues(i, len(args)))
		if i < len(klines) - 1 {
			stmtBuff.WriteByte(',')
		}
	}
	_, err := k.pool.Exec(ctx, stmtBuff.String(), stmtArgs...)
	if err != nil {
		return errors.Wrap(err, "BulkInsert()")
	}
	return nil
}

