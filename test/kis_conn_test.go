package test

import (
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/log"
	"testing"
)

func TestNewConnConfig(t *testing.T) {
	source := config.KisSource{
		Name: "订单数据",
		Must: []string{"order_id", "user_id"},
	}
	option := config.KisFuncOption{
		CName:         "connectiorName1",
		RetryTimes:    3,
		RetryDurition: 300,
		Params:        config.FParam{"p1": "v1", "p2": "v2"},
	}
	myFunc1 := config.NewFuncConfig("funcName1", common.S, &source, &option)
	connParams := config.FParam{
		"p1": "v1",
		"p2": "v2",
	}
	myConnector1 := config.NewConnConfig("connectorName1", "127.0.0.1:8080", common.REDIS, "key", connParams)
	if err := myConnector1.WithFunc(myFunc1); err != nil {
		log.Logger().ErrorF("WithFunc Error: %s\n", err.Error())
	}
	log.Logger().InfoF("myConnector1: %+v\n", myConnector1)
}
