package Comm

import (
	"net"
	"sync"
)

//消息类型
const MSG_TYPE_PROBE = 1
const MSG_TYPE_PROBE_RET = 2
const MSG_TYPE_COMMIT = 3
const MSG_TYPE_COMMIT_RET = 4
const MSG_TYPE_REVERSE = 5
const MSG_TYPE_REVERSE_RET = 6
const MSG_TYPE_CONFIRM = 7
const MSG_TYPE_CONFIRM_RET = 8

//路径
type Path struct {
	Nid		[]int
}

type Potential_path struct {
	Nid			[]int
	Capacity	float64
	PreCommit	float64
}


type ByCapacity []Potential_path

//节点信息
type NodeInfo struct {
	NodeID	int		`json:"nid"`
	Ip		string	`json:"ip"`
	Port	int		`json:"port"`
	Cap		float64		`json:"cap"`
	SubnetIDs  []int    `json:"subnet_ids"`  // 子网ID
}

//节点配置
type NodeConf struct {
	Conf	NodeInfo
}

//邻居节点配置
type NeigConf struct {
	Conf	[]NodeInfo
}

//交易
type Trans struct {
	Src			int
	Dst			int
	Volume		float64	
}

//消息
type Msg struct {
	Type		int				`json:"type"`
	//TransID		int				`json:"tid"`
	ReqID		int				`json:"rid"`
	Src			int				`json:"src"`
	Dst			int				`json:"dst"`
	//Reverse		float64			`json:"rev"`
	Path		[]int			`json:"path"`
	Cap			[]float64		`json:"cap"`
	P1c			[]float64		`json:"p1c"`		//记录commit过程中锁定的资金 
	Commit		float64			`json:"commit"`  //发送者发起的支付金额
	//ActCommit	float64			`json:"actcom"`  //路径真实可提交的支付金额
	CommitState	bool			`json:"comstate"`  // 用于标志commit成功or失败
}

// node-related 
type Session struct {
	NI		NodeInfo		// I copy an capacity outside it
	Msg_ch	chan Msg
	Conn	net.TCPConn
	Cap		float64
	CapLock	sync.Mutex		// lock for var Cap
	N_msg_probe	int
	N_msg_commit	int
	N_msg_reverse	int
	N_msg_confirm	int
}

//节点
type Node struct {
	NI		NodeInfo
	Ln		*net.TCPListener
	Sess_out	[]Session
	Sess_in		[]Session
}
