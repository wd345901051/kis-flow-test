package test

import (
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/log"
	"testing"
)

func TestNewFlowConfig(t *testing.T) {
	flowFuncParams1 := config.KisFlowFunctionParam{
		FuncName: "funcName1",
		Params:   config.FParam{"flowSetFuncParam1": "v1", "flowSetFuncParam2": "v2"},
	}
	flowFuncParams2 := config.KisFlowFunctionParam{
		FuncName: "funcName2",
		Params:   config.FParam{"default": "v"},
	}

	myFlow1 := config.NewFlowConfig("flowName1", common.FlowEnable)
	myFlow1.AppendFunctionConfig(flowFuncParams1)
	myFlow1.AppendFunctionConfig(flowFuncParams2)
	log.Logger().InfoF("myFlow1: %+v\n", myFlow1)
}
