package base

type Task struct {
	Channel      string
	Payload      []byte
	Code         string
	Queue        string
	State        string
	ErrorMsg     string
	LastFailedAt int64
	CompletedAt  int64
	EnqueuedAt   int64
}

func NewTask(channel string, payload []byte) *Task {
	return &Task{
		Channel: channel,
		Payload: payload,
	}
}
