package upcoin

import (
	"collector/db"
	"context"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"github.com/sonh/go-binance/binance"
	"strings"
	"time"
)

const (
	BinanceApiWeight = `X-Mbx-Used-Weight-1m`
)

type binanceApi struct {
	http *binance.HttpClient
	ws   *binance.WsClient
}

func newBinanceApi() (*binanceApi, error) {
	httpClient, err := binance.NewHttpClient(``, ``)
	if err != nil {
		return nil, errors.Wrap(err, `NewCollector()`)
	}

	binanceApi := &binanceApi{
		http: httpClient,
	}
	return binanceApi, nil
}

func (b *binanceApi) GetKlines(ctx context.Context, symbol string, interval string, start time.Time, limit int) ([]*db.Kline, error) {
	symbol = strings.ToUpper(symbol)

	// request api
	binanceKlines, err := b.http.Market.GetKlines().
		WithSymbol(symbol).
		WithInterval(interval).
		WithStartTime(start).
		WithLimit(limit).
		Do(ctx)
	if err != nil {
		return nil, err
	}

	startId, err := getKlineId(start, interval)
	if err != nil {
		return nil, errors.Wrap(err, "getKlineId()")
	}

	klines := make(map[int64]*db.Kline, limit)
	for i := 0; i < limit; i++ {
		id := startId + int64(i)
		klines[id] = &db.Kline{
			Symbol:                   strings.ToUpper(symbol),
			Interval:                 interval,
			Available:                false,
			OpenTime:                 time.Time{},
			CloseTime:                time.Time{},
			OpenPrice:                decimal.Decimal{},
			ClosePrice:               decimal.Decimal{},
			HighPrice:                decimal.Decimal{},
			LowPrice:                 decimal.Decimal{},
			Trades:                   0,
			Volume:                   decimal.Decimal{},
			QuoteAssetVolume:         decimal.Decimal{},
			TakerBuyBaseAssetVolume:  decimal.Decimal{},
			TakerBuyQuoteAssetVolume: decimal.Decimal{},
		}
	}

	// copy slice
	dbKlines := make([]*db.Kline, 0, len(binanceKlines))
	for _, binanceKline := range binanceKlines {
		dbKline := &db.Kline{
			Symbol:                   symbol,
			Interval:                 interval,
			Available:                true,
			OpenTime:                 binanceKline.OpenTime.Time,
			CloseTime:                binanceKline.CloseTime.Time,
			OpenPrice:                decimal.NewFromFloat(binanceKline.OpenPrice.Value),
			ClosePrice:               decimal.NewFromFloat(binanceKline.ClosePrice.Value),
			HighPrice:                decimal.NewFromFloat(binanceKline.HighPrice.Value),
			LowPrice:                 decimal.NewFromFloat(binanceKline.LowPrice.Value),
			Trades:                   binanceKline.Trades,
			Volume:                   decimal.NewFromFloat(binanceKline.Volume.Value),
			QuoteAssetVolume:         decimal.NewFromFloat(binanceKline.QuoteAssetVolume.Value),
			TakerBuyBaseAssetVolume:  decimal.NewFromFloat(binanceKline.TakerBuyBaseAssetVolume.Value),
			TakerBuyQuoteAssetVolume: decimal.NewFromFloat(binanceKline.TakerBuyQuoteAssetVolume.Value),
		}
		dbKlines = append(dbKlines, dbKline)
	}
	return dbKlines, err
}
