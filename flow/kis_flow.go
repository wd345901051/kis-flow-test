package flow

import (
	"context"
	"kis-flow/common"
	"kis-flow/config"
	"kis-flow/id"
	"kis-flow/kis"
	"kis-flow/log"
	"sync"
)

// KisFlow 用于贯穿整条流式计算的上下文环境
type KisFlow struct {
	Id   string                // Flow的分布式实例ID(用于KisFlow内部区分不同实例)
	Name string                // Flow的可读名称
	Conf *config.KisFlowConfig // Flow配置策略

	// Function列表
	Funcs          map[string]kis.Function // 当前flow拥有的全部管理的全部Function对象, key: FunctionID
	FloowHead      kis.Function            // 当前Flow所拥有的Function列表表头
	FloowTail      kis.Function            // 当前Flow所拥有的Function列表表尾
	flock          sync.RWMutex            // 管理链表插入读写的锁
	ThisFunction   kis.Function            // Flow当前正在执行的KisFunction对象
	ThisFunctionId string                  // 当前执行到的Function ID (策略配置ID)
	PrevFunctionId string                  // 当前执行到的Function 上一层FunctionID(策略配置ID)

	// Function列表参数
	funcParams map[string]config.FParam // flow在当前Function的自定义固定配置参数,Key:function的实例KisID, value:FParam
	fplock     sync.RWMutex             // 管理funcParams的读写锁

	// ++++++++ 数据 ++++++++++
	buffer common.KisRowArr  // 用来临时存放输入字节数据的内部Buf, 一条数据为interface{}, 多条数据为[]interface{} 也就是KisBatch
	data   common.KisDataMap // 流式计算各个层级的数据源
	inPut  common.KisRowArr  // 当前Function的计算输入数据
}

// TODO for test
// NewKisFlow 创建一个KisFlow.
func NewKisFlow(conf *config.KisFlowConfig) kis.Flow {

	flow := new(KisFlow)

	//基础信息
	flow.Id = id.KisID(common.KisIdTypeFlow)
	flow.Name = conf.FlowName

	// Function列表
	flow.Funcs = make(map[string]kis.Function)
	flow.funcParams = make(map[string]config.FParam)

	// ++++++++ 数据data +++++++
	flow.data = make(common.KisDataMap)

	return flow
}

// Run 调度Flow，依次调度Flow中的Function并且执行
func (flow *KisFlow) Run(ctx context.Context) error {

	var fn kis.Function
	fn = flow.FloowHead

	if flow.Conf.Status == int(common.FlowDisable) {
		//flow被配置关闭
		return nil
	}

	// ========= 数据流 新增 ===========
	// 因为此时还没有执行任何Function, 所以PrevFunctionId为FirstVirtual 因为没有上一层Function
	flow.PrevFunctionId = common.FunctionIdFirstVirtual

	if err := flow.commitSrcData(ctx); err != nil {
		return err
	}
	// ========= 数据流 新增 ===========

	//流式链式调用
	for fn != nil {
		// ========= 数据流 新增 ===========
		// flow记录当前执行到的Function 标记
		fid := fn.GetId()
		flow.ThisFunction = fn
		flow.ThisFunctionId = fid

		// 得到当前Function要处理的源数据
		if inputData, err := flow.getCurData(); err != nil {
			log.Logger().ErrorFX(ctx, "flow.Run(): getCurData err = %s\n", err.Error())
			return err
		} else {
			flow.inPut = inputData
		}
		// ========= 数据流 新增 ===========

		if err := fn.Call(ctx, flow); err != nil {
			// Error
			return err
		} else {
			// Success

			// ========= 数据流 新增 ===========
			if err := flow.commitCurData(ctx); err != nil {
				return err
			}

			// 更新上一层FuncitonId游标
			flow.PrevFunctionId = flow.ThisFunctionId
			// ========= 数据流 新增 ===========

			fn = fn.Next()
		}
	}

	return nil
}

// Link 将Flow中的Function按照配置文件中的配置进行连接
func (flow *KisFlow) Link(fConf *config.KisFuncConfig, fParam config.FParam) error {
	return nil
}
