package flow

import (
	"context"
	"fmt"
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/log"
	"time"

	"github.com/patrickmn/go-cache"
)

func (flow *KisFlow) CommitRow(row interface{}) error {
	flow.buffer = append(flow.buffer, row)
	return nil
}

// commitSrcData 提交当前Flow的数据源数据, 表示首次提交当前Flow的原始数据源
// 将flow的临时数据buffer，提交到flow的data中,(data为各个Function层级的源数据备份)
// 会清空之前所有的flow数据
func (flow *KisFlow) commitSrcData(ctx context.Context) error {
	// 制作批量数据batch
	dataCnt := len(flow.buffer)
	batch := make(common.KisRowArr, 0, dataCnt)

	batch = append(batch, flow.buffer...)

	flow.clearData()

	// 首次提交，记录flow原始数据
	// 因为首次提交，所以PrevFunctionId为FirstVirtual 因为没有上一层Function
	flow.data[common.FunctionIdFirstVirtual] = batch

	// 清空缓冲buf
	flow.buffer = flow.buffer[0:0]

	log.Logger().DebugFX(ctx, "====> After CommitSrcData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}

func (flow *KisFlow) clearData() {
	for k := range flow.data {
		flow.data[k] = nil
	}
}

// commitCurData 提交Flow当前执行Function的结果数据
func (flow *KisFlow) commitCurData(ctx context.Context) error {
	//判断本层计算是否有结果数据,如果没有则退出本次Flow Run循环
	if len(flow.buffer) == 0 {
		flow.abort = true
		return nil
	}

	// 制作批量数据batch
	batch := make(common.KisRowArr, 0, len(flow.buffer))

	batch = append(batch, flow.buffer...)

	//将本层计算的缓冲数据提交到本层结果数据中
	flow.data[flow.ThisFunctionId] = batch

	//清空缓冲Buf
	flow.buffer = flow.buffer[0:0]

	log.Logger().DebugFX(ctx, " ====> After commitCurData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)

	return nil
}

// getCurData 获取flow当前Function层级的输入数据
func (flow *KisFlow) getCurData() (common.KisRowArr, error) {
	if flow.PrevFunctionId == "" {
		return nil, fmt.Errorf("flow.PrevFunctionId is not set")
	}

	if _, ok := flow.data[flow.PrevFunctionId]; !ok {
		return nil, fmt.Errorf("[%s] is not in flow.data", flow.PrevFunctionId)
	}

	return flow.data[flow.PrevFunctionId], nil
}

// Input 得到flow当前执行Function的输入源数据
func (flow *KisFlow) Input() common.KisRowArr {
	return flow.inPut
}

func (flow *KisFlow) commitVoidData(ctx context.Context) error {
	if len(flow.buffer) != 0 {
		return nil
	}
	batch := make(common.KisRowArr, 0)

	flow.data[flow.ThisFunctionId] = batch

	log.Logger().DebugFX(ctx, " ====> After commitVoidData, flow_name = %s, flow_id = %s\nAll Level Data =\n %+v\n", flow.Name, flow.Id, flow.data)
	return nil
}

func (flow *KisFlow) GetCacheData(key string) interface{} {
	if data, found := flow.cache.Get(key); found {
		return data
	}
	return nil
}

func (flow *KisFlow) SetCacheData(key string, value interface{}, Exp time.Duration) {
	if Exp == common.DefaultExpiration {
		flow.cache.Set(key, value, cache.DefaultExpiration)
	} else {
		flow.cache.Set(key, value, Exp)
	}
}

// GetMetaData 得到当前Flow对象的临时数据
func (flow *KisFlow) GetMetaData(key string) interface{} {
	flow.mLock.RLock()
	defer flow.mLock.RUnlock()
	if value, ok := flow.metaData[key]; ok {
		return value
	}
	return nil
}

// SetMetaData 设置当前Flow对象的临时数据
func (flow *KisFlow) SetMetaData(key string, value interface{}) {
	flow.mLock.Lock()
	defer flow.mLock.Unlock()

	flow.metaData[key] = value
}

// GetFuncParam 得到Flow的当前正在执行的Function的配置默认参数，取出一对key-value
func (flow *KisFlow) GetFuncParam(key string) string {
	flow.fplock.RLock()
	defer flow.fplock.RUnlock()

	if param, ok := flow.funcParams[flow.ThisFunctionId]; ok {
		if value, vok := param[key]; vok {
			return value
		}
	}

	return ""
}

// GetFuncParamAll 得到Flow的当前正在执行的Function的配置默认参数，取出全部Key-Value
func (flow *KisFlow) GetFuncParamAll() config.FParam {
	flow.fplock.RLock()
	defer flow.fplock.RUnlock()

	param, ok := flow.funcParams[flow.ThisFunctionId]
	if !ok {
		return nil
	}

	return param
}
