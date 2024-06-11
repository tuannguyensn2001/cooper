package cooper

import (
	"context"
	"cooper/base"
	"cooper/broker"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestEnqueue(t *testing.T) {
	b, db, clean, err := broker.GetContainerMySqlBroker(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clean)

	client := NewClient(b)

	tests := []struct {
		name      string
		task      *base.Task
		expectErr error
		expectFn  func(t *testing.T)
	}{
		{
			name:      "task is nil",
			expectErr: ErrTaskNil,
		},
		{
			name:      "channel is empty",
			task:      &base.Task{},
			expectErr: ErrChannelEmpty,
		},
		{
			name: "enqueue successfully",
			task: base.NewTask("test", []byte("test")),
			expectFn: func(t *testing.T) {
				var task base.Task
				db.First(&task)
				require.Equal(t, "test", task.Channel)
				require.Equal(t, []byte("test"), task.Payload)
				require.Equal(t, base.TaskStatePending.String(), task.State)
				require.Equal(t, "default", task.Queue)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.Enqueue(context.TODO(), tt.task)
			require.Equal(t, tt.expectErr, err)
			if tt.expectFn != nil {
				tt.expectFn(t)
			}
		})
	}

}
