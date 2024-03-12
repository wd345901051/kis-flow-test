package flow

import (
	"context"
	"kis-flow/kis"
)

func (flow *KisFlow) dealAction(ctx context.Context, fn kis.Function) (kis.Function, error) {
	if err := flow.commitCurData(ctx); err != nil {
		return nil, err
	}

	flow.PrevFunctionId = flow.ThisFunctionId
	fn = fn.Next()

	if flow.action.Abort {
		flow.abort = true
	}

	flow.action = kis.Action{}

	return fn, nil
}
