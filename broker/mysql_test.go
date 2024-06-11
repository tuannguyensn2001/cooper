package broker

import (
	"context"
	"cooper/base"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestEnqueue(t *testing.T) {
	b, _, cancel, err := GetContainerMySqlBroker(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancel)

	tests := []struct {
		name      string
		task      *base.Task
		expectErr error
		init      func()
		clean     func()
	}{
		{
			name: "task is not exist",
			task: &base.Task{
				Channel: "test",
				Payload: []byte("test"),
				Code:    "test",
				Queue:   "test",
			},
		},
		{
			name: "task is exist",
			task: &base.Task{
				Channel: "test",
				Payload: []byte("test"),
				Code:    "test",
				Queue:   "test",
			},
			expectErr: ErrTaskAlreadyExists,
			init: func() {
				b.db.Create(&Task{
					Channel: "test",
					Payload: []byte("test"),
					Code:    "test",
					Queue:   "test",
				})
			},
			clean: func() {
				b.db.Where("'key' = ?", "test").Delete(&Task{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.init != nil {
				tt.init()
			}
			if tt.clean != nil {
				t.Cleanup(tt.clean)
			}
			err := b.Enqueue(context.TODO(), tt.task)
			require.Equal(t, tt.expectErr, err)
		})
	}

	t.Run("it should works well in concurrent", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				b.Enqueue(context.TODO(), &base.Task{
					Channel: "test",
					Payload: []byte("test"),
					Code:    "test",
				})
			}()
		}

		wg.Wait()

		var count int64
		b.db.Model(&Task{}).Where("code = ?", "test").Count(&count)
		require.Equal(t, int64(1), count)

		b.db.Delete(&Task{})
	})

}
