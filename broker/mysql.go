package broker

import (
	"context"
	"cooper/base"
	"database/sql"
	"github.com/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type MysqlBroker struct {
	db *gorm.DB
}

var ErrTaskAlreadyExists = errors.New("task already exists")
var ErrNoTaskFound = errors.New("no task found")

type Task struct {
	Id           int    `gorm:"column:id;primaryKey"`
	Channel      string `gorm:"column:channel"`
	Payload      []byte `gorm:"column:payload"`
	Code         string `gorm:"column:code"`
	Queue        string `gorm:"column:queue"`
	State        string `gorm:"column:state"`
	ErrorMsg     string `gorm:"column:error_msg"`
	LastFailedAt int64  `gorm:"column:last_failed_at"`
	CompletedAt  int64  `gorm:"column:completed_at"`
	EnqueuedAt   int64  `gorm:"column:enqueued_at"`
}

type MysqlBrokerConfig struct {
	Url        string
	GormConfig gorm.Config
}

func NewMysqlBroker(config MysqlBrokerConfig) (*MysqlBroker, error) {
	db, err := gorm.Open(mysql.Open(config.Url), &config.GormConfig)
	if err != nil {
		return nil, err
	}
	broker := &MysqlBroker{db: db}
	broker.Init()
	return broker, nil
}

func (b *MysqlBroker) Init() {
	b.db.AutoMigrate(&Task{})
}

func (b *MysqlBroker) Enqueue(ctx context.Context, task *base.Task) error {
	err := b.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		err := tx.Model(&Task{}).Where("code = ?", task.Code).Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			return ErrTaskAlreadyExists
		}
		err = tx.Create(&Task{
			Channel:    task.Channel,
			Payload:    task.Payload,
			Code:       task.Code,
			Queue:      task.Queue,
			State:      base.TaskStatePending.String(),
			EnqueuedAt: task.EnqueuedAt,
		}).Error

		return nil
	}, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	if err != nil {
		return err
	}
	return nil
}

func (b *MysqlBroker) Dequeue(ctx context.Context, queue string) (*base.Task, error) {
	var task Task
	err := b.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Where("queue = ? and state = ?", queue, base.TaskStatePending.String()).First(&task).Error
		if err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return ErrNoTaskFound
			}
			return err
		}

		task.State = base.TaskStateRunning.String()
		err = tx.Save(&task).Error
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return &base.Task{
		Channel:    task.Channel,
		Payload:    task.Payload,
		Code:       task.Code,
		Queue:      task.Queue,
		EnqueuedAt: task.EnqueuedAt,
	}, nil

}
