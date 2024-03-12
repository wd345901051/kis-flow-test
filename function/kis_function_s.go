package function

import (
	"context"
	"fmt"
	"kis-flow/kis"
	"kis-flow/log"
)

type KisFunctionS struct {
	BaseFunction
}

func (f *KisFunctionS) Call(ctx context.Context, flow kis.Flow) error {
	fmt.Printf("KisFunctionS, flow = %+v\n", flow)

	// 通过KisPool 路由到具体的执行计算Function中
	if err := kis.Pool().CallFunction(ctx, f.Config.FName, flow); err != nil {
		log.Logger().ErrorFX(ctx, "Function Called Error err = %s\n", err)
		return err
	}

	return nil
}
