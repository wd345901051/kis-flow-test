package test

import (
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/log"
	"testing"
)

func TestNewFuncConfig(t *testing.T) {
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

	log.Logger().InfoF("funcName1: %+v\n", myFunc1)
}
