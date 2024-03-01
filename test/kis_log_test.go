package test

import (
	"context"
	"kis-flow/log"
	"testing"
)

func TestKisLogger(t *testing.T) {
	ctx := context.Background()
	log.Logger().InfoFX(ctx, "TestKisLogger InfoFx")
	log.Logger().ErrorFX(ctx, "TestKisLogger ErrorFx")
	log.Logger().DebugFX(ctx, "TestKisLogger DebugFx")

	log.Logger().InfoF("TestKisLogger InfoF")
	log.Logger().ErrorF("TestKisLogger ErrorF")
	log.Logger().DebugF("TestKisLogger DebugF")
}
