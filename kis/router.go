package kis

import (
	"context"
	"kis-flow/common"
)

// funcRouter
// key: Function Name
// value: Function 回调自定义业务
type funcRouter map[string]*FaaSDesc

// flowRouter
// key: Flow Name
// value: Flow
type flowRoutert map[string]Flow

/*
	Connector Init
*/
// ConnInit Connector 第三方挂载存储初始化
type ConnInit func(conn Connector) error

// connInitRouter
// 为管理ConnInit的路由，key为ConnName。
type connInitRouter map[string]ConnInit

/*
	Connector Call
*/
// CaaS Connector的存储读取业务实现
type CaaS func(context.Context, Connector, Function, Flow, interface{}) error

// connFuncRouter 通过FunctionName索引到CaaS回调存储业务的映射关系
// key: Function Name
// value: Connector的存储读取业务实现
type connFuncRouter map[string]CaaS

// connSL 通过KisMode 将connFuncRouter分为两个子树
// key: Function KisMode S/L
// value: NsConnRouter
type connSL map[common.KisMode]connFuncRouter

// connTree
// key: Connector Name
// value: connSL 二级树
type connTree map[string]connSL
