package log

import "context"

type KisLogger interface {
	InfoFX(ctx context.Context, str string, v ...interface{})
	ErrorFX(ctx context.Context, str string, v ...interface{})
	DebugFX(ctx context.Context, str string, v ...interface{})

	InfoF(str string, v ...interface{})
	ErrorF(str string, v ...interface{})
	DebugF(str string, v ...interface{})
}

var kisLog KisLogger

func SetLogger(newlog KisLogger) {
	kisLog = newlog
}

func Logger() KisLogger {
	return kisLog
}
