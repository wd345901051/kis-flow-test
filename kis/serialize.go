package kis

import (
	"kis-flow/common"
	"kis-flow/serialize"
	"reflect"
)

// Serialize 数据序列化接口
type Serialize interface {
	// UnMarshal 用于将 KisRowArr 反序列化为指定类型的值。
	UnMarshal(common.KisRowArr, reflect.Type) (reflect.Value, error)
	// Marshal 用于将指定类型的值序列化为 KisRowArr。
	Marshal(interface{}) (common.KisRowArr, error)
}

var defaultSerialize = &serialize.DefaultSerialize{}

func isSerialize(paramType reflect.Type) bool {
	return paramType.Implements(reflect.TypeOf((*Serialize)(nil)).Elem())
}
