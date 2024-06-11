package base

type TaskState string

const (
	TaskStatePending TaskState = "pending"
	TaskStateRunning TaskState = "running"
)

func (state TaskState) String() string {
	return string(state)
}
