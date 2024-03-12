package kis

// Action KisFlow执行流程Actions
type Action struct {
	// Abort 终止Flow的执行
	Abort bool
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
