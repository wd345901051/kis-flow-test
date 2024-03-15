package flow

import (
	"context"
	"errors"
	"fmt"
	"kis-flow/kis"
	"kis-flow/log"
)

func (flow *KisFlow) dealAction(ctx context.Context, fn kis.Function) (kis.Function, error) {

	if flow.action.DataReuse {
		if err := flow.commitReuseData(ctx); err != nil {
			return nil, err
		}
	} else {
		if err := flow.commitCurData(ctx); err != nil {
			return nil, err
		}
	}

	if flow.action.Abort && flow.action.ForceEntryNext {
		if err := flow.commitVoidData(ctx); err != nil {
			return nil, err
		}
		flow.action.Abort = false
	}

	if flow.action.JumpFunc != "" {
		if _, ok := flow.Funcs[flow.action.JumpFunc]; !ok {
			//当前JumpFunc不在flow中
			return nil, errors.New(fmt.Sprintf("Flow Jump -> %s is not in Flow", flow.action.JumpFunc))
		}
		jumpFunction := flow.Funcs[flow.action.JumpFunc]

		flow.PrevFunctionId = jumpFunction.GetId()
		fn = jumpFunction

		// 如果设置跳跃，强制跳跃
		flow.abort = false

	} else {
		// 更新上一层 FuncitonId 游标
		flow.PrevFunctionId = flow.ThisFunctionId
		fn = fn.Next()
	}

	if flow.action.Abort {
		flow.abort = true
	}

	flow.action = kis.Action{}

	return fn, nil
}

func (flow *KisFlow) commitReuseData(ctx context.Context) error {

	// 判断上层是否有结果数据, 如果没有则退出本次Flow Run循环
	if len(flow.data[flow.PrevFunctionId]) == 0 {
		flow.abort = true
		return nil
	}

	// 本层结果数据等于上层结果数据(复用上层结果数据到本层)
	flow.data[flow.ThisFunctionId] = flow.data[flow.PrevFunctionId]

	// 清空缓冲Buf (如果是ReuseData选项，那么提交的全部数据，都将不会携带到下一层)
	flow.buffer = flow.buffer[0:0]

	log.Logger().DebugFX(ctx, " ====> After commitReuseData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}
