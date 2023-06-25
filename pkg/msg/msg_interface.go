package msg

type MsgInter interface {
	NewMsg() interface{}
	String() string
}
