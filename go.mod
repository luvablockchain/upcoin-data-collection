module collector

go 1.15

require (
	github.com/jackc/pgx/v4 v4.10.0
	github.com/pkg/errors v0.9.1
	github.com/shopspring/decimal v0.0.0-20200227202807-02e2044944cc
	github.com/stretchr/testify v1.6.1
	go.uber.org/zap v1.16.0
	github.com/sonh/go-binance v0.0.0
)

replace (
	github.com/sonh/go-binance => ../../gomod/go-binance
)
