package conn

import (
	"context"
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/id"
	"kis-flow/kis"
	"sync"
)

type KisConnector struct {
	// Connector ID
	CId string
	// Connector Name
	CName string
	// Connector Config
	Conf *config.KisConnConfig

	// Connector Init
	oneceInit sync.Once
}

// NewKisConnector 根据配置策略创建一个KisConnector
func NewKisConnector(config *config.KisConnConfig) *KisConnector {
	conn := new(KisConnector)
	conn.CId = id.KisID(common.KisIdTypeConnnector)
	conn.CName = config.CName
	conn.Conf = config

	return conn
}

func (conn *KisConnector) Init() error {
	var err error

	conn.oneceInit.Do(func() {
		err = kis.Pool().CallConnInit(conn)
	})

	return err
}

func (conn *KisConnector) Call(ctx context.Context, flow kis.Flow, args interface{}) error {
	return kis.Pool().CallConnector(ctx, flow, conn, args)
}

func (conn *KisConnector) GetName() string {
	return conn.CName
}

func (conn *KisConnector) GetId() string {
	return conn.CId
}

func (conn *KisConnector) GetConfig() *config.KisConnConfig {
	return conn.Conf
}
