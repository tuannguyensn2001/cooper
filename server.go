package cooper

import (
	"context"
	"cooper/base"
)

type ILogger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Fatal(args ...interface{})
}

type Handler interface {
	ProcessTask(ctx context.Context, task *base.Task) error
}
