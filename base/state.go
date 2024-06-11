package base

type TaskState string

const (
	TaskStatePending TaskState = "pending"
)

func (state TaskState) String() string {
	return string(state)
}
