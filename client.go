package cooper

import (
	"context"
	"cooper/base"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"strings"
	"time"
)

var (
	ErrTaskNil      = errors.New("task cannot be nil")
	ErrChannelEmpty = errors.New("channel cannot be empty")
)

type IBroker interface {
	Enqueue(ctx context.Context, task *base.Task) error
}

type Client struct {
	broker IBroker
}

func NewClient(broker IBroker) *Client {
	return &Client{broker: broker}
}

func (c *Client) Enqueue(ctx context.Context, task *base.Task) error {
	if task == nil {
		return ErrTaskNil
	}
	if strings.TrimSpace(task.Channel) == "" {
		return ErrChannelEmpty
	}
	now := time.Now()
	task.EnqueuedAt = now.Unix()
	if task.Queue == "" {
		task.Queue = base.DEFAULT_QUEUE
	}
	if task.Code == "" {
		task.Code = uuid.New().String()
	}

	err := c.broker.Enqueue(ctx, task)
	if err != nil {
		return errors.Wrap(err, "failed to enqueue task")
	}

	return nil
}
