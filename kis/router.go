package kis

import "context"

// FaaS Function as a Service
type Faas func(context.Context, Flow) error

// funcRouter
// key: Function Name
// value: Function 回调自定义业务
type funcRouter map[string]Faas

// flowRouter
// key: Flow Name
// value: Flow
type flowRoutert map[string]Flow
