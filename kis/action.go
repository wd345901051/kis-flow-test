package kis

// Action KisFlow执行流程Actions
type Action struct {
	// DataReuse 是否复用上层Function数据
	DataReuse bool

	// Abort 终止Flow的执行
	Abort bool

	// 默认Next()为如果本层Function计算结果为0条数据，之后Function将不会继续执行
	// ForceEntryNext 为忽略上述默认规则，没有数据强制进入下一层Function
	ForceEntryNext bool
}

// ActionFunc KisFlow Functional Option 类型
type ActionFunc func(ops *Action)

func LoadActions(acts []ActionFunc) Action {
	action := Action{}

	if acts == nil {
		return action
	}

	for _, a := range acts {
		a(&action)
	}

	return action
}

// ActionAbort 终止Flow的执行
func ActionAbort(action *Action) {
	action.Abort = true
}

// ActionDataReuse Next复用上层Function数据Option
func ActionDataReuse(action *Action) {
	action.DataReuse = true
}

func ActionForceEntryNext(action *Action) {
	action.ForceEntryNext = true
}
