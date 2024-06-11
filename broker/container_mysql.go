package broker

import (
	"context"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/wait"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
)

func GetContainerMySqlBroker(ctx context.Context) (*MysqlBroker, *gorm.DB, func(), error) {
	mysqlContainer, err := mysql.RunContainer(ctx, testcontainers.WithImage("mysql"), mysql.WithDatabase("cooper"), testcontainers.WithWaitStrategy(
		wait.ForLog("port: 3306  MySQL Community Server - GPL"),
	))
	if err != nil {
		return nil, nil, nil, err
	}
	connStr, err := mysqlContainer.ConnectionString(ctx, "")
	if err != nil {
		return nil, nil, nil, err
	}
	b, err := NewMysqlBroker(MysqlBrokerConfig{
		Url: connStr,
		GormConfig: gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		},
	})
	if err != nil {
		return nil, nil, nil, err
	}

	cancel := func() {
		log.Println("Terminating container")
		mysqlContainer.Terminate(ctx)
	}

	b.db.Delete(&Task{})

	return b, b.db, cancel, nil

}
