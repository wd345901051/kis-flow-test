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

	// KisConnector的自定义临时数据
	metaData map[string]interface{}
	// 管理metaData的读写锁
	mLock sync.RWMutex
}

// NewKisConnector 根据配置策略创建一个KisConnector
func NewKisConnector(config *config.KisConnConfig) *KisConnector {
	conn := new(KisConnector)
	conn.CId = id.KisID(common.KisIdTypeConnnector)
	conn.CName = config.CName
	conn.Conf = config

	conn.metaData = make(map[string]interface{})

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

// GetMetaData 得到当前Connector的临时数据
func (conn *KisConnector) GetMetaData(key string) interface{} {
	conn.mLock.RLock()
	defer conn.mLock.RUnlock()

	data, ok := conn.metaData[key]
	if !ok {
		return nil
	}

	return data
}

// SetMetaData 设置当前Connector的临时数据
func (conn *KisConnector) SetMetaData(key string, value interface{}) {
	conn.mLock.Lock()
	defer conn.mLock.Unlock()

	conn.metaData[key] = value
}
