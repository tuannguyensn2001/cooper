package cooper

import (
	"context"
	errors2 "cooper/errors"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"sync"
	"time"
)

type processor struct {
	logger            ILogger
	broker            IBroker
	handler           Handler
	taskCheckInterval time.Duration
	sema              chan struct{}
	done              chan struct{}
	once              sync.Once
	quit              chan struct{}
	abort             chan struct{}
	queueConfig       map[string]int
}

type processorParams struct {
	logger            ILogger
	broker            IBroker
	handler           Handler
	taskCheckInterval time.Duration
	concurrency       int
	queueConfig       map[string]int
}

func newProcessor(params processorParams) *processor {
	return &processor{
		logger:            params.logger,
		broker:            params.broker,
		handler:           params.handler,
		taskCheckInterval: params.taskCheckInterval,
		sema:              make(chan struct{}, params.concurrency),
		done:              make(chan struct{}),
		quit:              make(chan struct{}),
		abort:             make(chan struct{}),
	}
}

func (p *processor) stop() {
	p.once.Do(func() {
		p.logger.Debug("Processor shutting down...")
		close(p.quit)
		p.done <- struct{}{}
	})
}

func (p *processor) shutdown() {
	p.stop()

	p.logger.Info("Waiting for all workers to finish...")

	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	p.logger.Info("All workers have finished.")
}

func (p *processor) exec() {
	select {
	case <-p.quit:
		return
	case p.sema <- struct{}{}:
		nextQueue := p.GetNextQueue()
		msg, err := p.broker.Dequeue(context.TODO(), nextQueue)
		switch {
		case errors.Is(err, errors2.ErrNoTaskFound):
			p.logger.Debug("All queues are empty")
			time.Sleep(p.taskCheckInterval)
			<-p.sema
		case err != nil:
			p.logger.Error(errors.Wrap(err, "failed to dequeue task"))
			<-p.sema
			return
		}
	}

}

func (p *processor) GetNextQueue() string {
	queues := make([]string, 0)

	for queue, _ := range p.queueConfig {
		queues = append(queues, queue)
	}

	shuffle := lo.Shuffle(queues)

	return shuffle[0]
}
