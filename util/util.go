package util

import (
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"os/signal"
)

func InitGlobalLogger() error {
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := loggerConfig.Build()
	if err == nil {
		zap.ReplaceGlobals(logger)
		return nil
	} else {
		return errors.Wrap(err, "initGlobalLogger()")
	}
}

func ShutdownListen() chan struct{} {
	shutdown := make(chan struct{})
	go func() {
		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt)
		select {
		case <-interrupt:
			close(shutdown)
		}
	}()
	return shutdown
}

func GetEnv(key string, defaultVal string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	} else {
		return defaultVal
	}
}
