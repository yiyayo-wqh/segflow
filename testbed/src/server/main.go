package main

import (
	"sort"
	"fmt"
	"log"
	"time"
	"comm"
	"net"
	"os"
	"io"
	"bufio"
	"os/signal"
	"io/ioutil"
	"strings"
	"strconv"
	"encoding/json"
	"math/rand"
	"math"
)


//算法选择
var g_algo int

//LND算法参数配置
const LND_attempt_cnt int = 3  //最大尝试次数

//Flash算法参数配置
const FS_thresh float64 = 1789.99425
const Flash_path_cnt int = 4  //路由表数量
const Flash_probe_cnt int = 20  //最大流算法迭代次数

//Spider、Waterfilling算法参数配置
const Spider_path_cnt int = 4

//SegFlow算法参数配置
const SegFlow_path_cnt int = 4 //k最短路径数量

//节点
var nd	Comm.Node
//节点ID
var g_nodeid int
//节点配置
var node_conf Comm.NodeInfo
//邻居节点配置
var neig_conf Comm.NeigConf
//本地路由表：int为目标节点，[]Comm.Path为对应路径集合
var P map[int][]Comm.Path

//全网拓扑与备份
var G map[int]map[int]float64
var bk_G map[int]map[int]float64
//子网拓扑与备份
var subGs map[int]map[int]map[int]float64
var bk_subGs map[int]map[int]map[int]float64

//交易集合
var all_trans []Comm.Trans
//传输消息的通道
var trans_ch chan Comm.Msg

var is_active bool = true


// 深拷贝函数
func deepCopy(original map[int]map[int]float64) map[int]map[int]float64 {
	copy := make(map[int]map[int]float64)
	for k, v := range original {
		copy[k] = make(map[int]float64)
		for innerK, innerV := range v {
			copy[k][innerK] = innerV
		}
	}
	return copy
}

//加载全网拓扑
func init_G(g_filename string) bool {

	// Initialize
	G = make(map[int]map[int]float64)
	bk_G = make(map[int]map[int]float64)

	file, err := os.Open(g_filename)
	if err != nil {
		log.Println("error reading graph file", err)
		return false
	}
	defer file.Close()

	br := bufio.NewReader(file)
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		fields := strings.Split(string(line), ",")
		if len(fields) != 3 {
			log.Println("bad format in graph file")
			return false
		}
		src, _ := strconv.Atoi(fields[0])
		dst, _ := strconv.Atoi(fields[1])
		src += 1
		dst += 1

		if G[src] == nil {
			G[src] = make(map[int]float64)
		}
		G[src][dst] = Comm.PATH_INITED //PATH_INITED = -1
	}
	// Deep copy to bk_G
	bk_G = deepCopy(G)
	
	return true
}

// 初始化子网拓扑 (可能多个)
func init_subGs(subnetIDs []int) bool {
	subGs = make(map[int]map[int]map[int]float64)
	bk_subGs = make(map[int]map[int]map[int]float64)

	for _, sid := range subnetIDs {
		filename := fmt.Sprintf("subgraph%d.txt", sid)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("[ERROR] failed to open subnet file %s: %v", filename, err)
			return false
		}
		defer file.Close()

		subGs[sid] = make(map[int]map[int]float64)
		br := bufio.NewReader(file)
		for {
			line, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			fields := strings.Split(string(line), ",")
			if len(fields) != 3 {
				log.Printf("[ERROR] bad format in %s: %s", filename, line)
				continue
			}
			src, _ := strconv.Atoi(fields[0])
			dst, _ := strconv.Atoi(fields[1])
			src += 1
			dst += 1

			if subGs[sid][src] == nil {
				subGs[sid][src] = make(map[int]float64)
			}
			subGs[sid][src][dst] = Comm.PATH_INITED
		}

		// 备份当前子网拓扑
		bk_subGs[sid] = deepCopy(subGs[sid])
	}

	return true
}

//加载交易
func load_trans(trans_filename string) bool {
	var ret bool = true
	var trans Comm.Trans
	
	//打开交易文件tri.txt
	file, err := os.Open(trans_filename)
	if err != nil {
		ret = false
		log.Println("error reading trans file", err)
		return ret
	}
	defer file.Close()
	br := bufio.NewReader(file)

	//将交易导入all_trans
	for {
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		// split by ','
		lines := strings.Split(string(line), ",")
		if 3 != len(lines) {
			log.Println("bad format in trans file")
			ret = false
			break
		}
		trans.Src, _ = strconv.Atoi(lines[0])
		trans.Dst, _ = strconv.Atoi(lines[1])
		tmp_float64, _ := strconv.ParseFloat(lines[2], 64)
		trans.Volume = float64(tmp_float64)

		all_trans = append(all_trans, trans)
	}
	return ret	
}

//加载路由表
func load_paths (paths_filename string) bool {
	var ret bool = true

	P = make(map[int][]Comm.Path)

	//打开路径文件tri.txt
	file, err := os.Open(paths_filename)
	if err != nil {
		ret = false
		log.Println("error reading trans file", err)
		return ret
	}
	defer file.Close()
	br := bufio.NewReader(file)

	//将路径导入P map[int][]Comm.Path中
	for {
		var path Comm.Path
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		// split by ','
		lines := strings.Split(string(line), ",")
		if 2 > len(lines) {
			log.Println("bad format in trans file")
			ret = false
			break
		}

		dst_id, _ := strconv.Atoi(lines[0])
		for i:=1; i<len(lines); i++ {
			node_id, _ := strconv.Atoi(lines[i])
			path.Nid = append(path.Nid, node_id)
		}

		P[dst_id] = append(P[dst_id], path)
	}

	//根据算法需要截取路径数量
	var path_needed int = 0
	if g_algo == 1 {
		path_needed = Flash_path_cnt
	} else if g_algo == 3 {
		path_needed = Spider_path_cnt
	} else if g_algo == 4 {
		path_needed = Spider_path_cnt
	} else if g_algo == 6 {
		path_needed = SegFlow_path_cnt
	}
	for k, _ := range P {
		if len(P[k]) > path_needed {
			P[k] = P[k][:path_needed]
		}
	}
	return ret
}

//系统信号处理
func handle_sig() {
	for is_active {
		c := make(chan os.Signal)
		signal.Notify(c)
		// s := <-c
		<-c
		is_active = false
		// panic("Want stack trace")
		os.Exit(-1)
	}
}

//向nid发送消息
func fwdmsg_SessOut_by_nid(msg *Comm.Msg, nid int) {
	found := false
	for i, ses := range nd.Sess_out {
		if ses.NI.NodeID == nid {
			// 固定通信延迟
			time.Sleep(10 * time.Millisecond)

			// 随机通信延迟（5-10ms）
			// delay := rand.Intn(6) + 5
			//time.Sleep(time.Duration(delay) * time.Millisecond)

			//将消息插入相应通道
			nd.Sess_out[i].Msg_ch <- *msg
			found = true
			break
		}
	}
	if !found {
		log.Printf("Warning: No session found for NodeID %d\n", nid)
	}
}

func waterfilling(paths []Comm.Potential_path, demand float64) bool {
	//路径可用余额总和
	var tot_avail_bal float64 = 0
	for i:=0; i<len(paths); i++ {
		tot_avail_bal += paths[i].Capacity
	}

	//如果不满足支付需求，则路由失败
	if tot_avail_bal < demand {
		return false
	}
	
	// 路径排序
	sort.Slice(paths, func(i, j int) bool {
		return paths[i].Capacity > paths[j].Capacity
	})

	var remain_vol float64 = demand
	var commit_vol float64

	for remain_vol > 0 {
		//当前最大余额
		var largest float64 = paths[0].Capacity
		//当前次大余额
		var seclargest float64 = 0
		//当前并列第一余额的路径索引集合
		var set_largest []int
		
		for ind:=0; ind < len(paths); ind++ {
			if paths[ind].Capacity == largest {
				set_largest = append(set_largest, ind)
			} else {
				seclargest = paths[ind].Capacity
				break
			}
		}

		//余额差
		diff := largest - seclargest

		//如果余额差之和大于支付需求，则均分支付需求
		if float64(len(set_largest)) * diff > remain_vol {
			commit_vol = float64(remain_vol)/float64(len(set_largest))
			remain_vol = 0
		} else {
			commit_vol = diff
			remain_vol -= diff * float64(len(set_largest))
		}

		//更新路径信息
		for _, ind := range set_largest {
			paths[ind].Capacity -= commit_vol
			paths[ind].PreCommit += commit_vol
		}
	}
	return true
}

// Spider (Waterfilling without transaction units): thread for handling transactions
func Waterfilling_HandleTrans () {
	//创建交易通道
	trans_ch = make(chan Comm.Msg, 1000)

	//所有commit成功的交易
	var committed_msg []Comm.Msg
	
	//初始化统计变量
	var sum_num int = 0
	var succ_num int = 0
	var sum_volume float64 = 0
	var succ_volume float64 = 0
	var sum_time float64 = 0
	
	// 处理前停滞，等待连接完成
	time.Sleep(20*time.Second)

	//依次处理交易
	for i, tr := range all_trans {
		fmt.Printf("[Trans %d/%d] ********************************Start process********************************\n", i+1, len(all_trans))
		
		//统计总支付数量/金额
		sum_num += 1
		sum_volume = sum_volume + tr.Volume

		// handle each transaction
		var fwd_msg Comm.Msg
		var recv_msg Comm.Msg
		var trans_succ_flag bool = true

		//开始计时
		t_st := time.Now()

		//路由表 ————> 潜在路径集合
		var p_paths []Comm.Potential_path
		for _, s_path := range P[tr.Dst] {
			var p_path Comm.Potential_path
			p_path.Capacity = -1
			p_path.PreCommit = 0
			p_path.Nid = s_path.Nid

			p_paths = append(p_paths, p_path)
		}

		//逐个探测潜在路径余额
		for ind, s_path := range p_paths {
			/************************************Probe*****************************/
			//构建消息
			fwd_msg.Type = Comm.MSG_TYPE_PROBE
			fwd_msg.ReqID = ind
			fwd_msg.Src = tr.Src
			fwd_msg.Dst = tr.Dst
			fwd_msg.Path = s_path.Nid
			fwd_msg.Cap = nil //重置Cap

			//发送消息
			fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
			fmt.Printf("Probe message: %v.\n", fwd_msg)

			//接收ret消息
			recv_msg = <- trans_ch
			fmt.Printf("Probe ret message: %v.\n", recv_msg)
			/************************************Probe end*****************************/

			//获取路径可用余额
			avail_bal := checkAvailBal(recv_msg.Path, recv_msg.Cap)
			if avail_bal < 0 {
				fmt.Println("[ERROR] negative capacity!")
				avail_bal = 0	
			}
			p_paths[ind].Capacity = avail_bal
		}

		if waterfilling(p_paths, tr.Volume) == false {
			trans_succ_flag = false
		} else {
			// 迭代Commit
			for _, s_path := range p_paths {
				/************************************Commit*****************************/
				//构建消息
				fwd_msg.Type = Comm.MSG_TYPE_COMMIT
				fwd_msg.Src = tr.Src
				fwd_msg.Dst = tr.Dst
				fwd_msg.Path = s_path.Nid
				fwd_msg.P1c = nil
				fwd_msg.Commit = s_path.PreCommit
				fwd_msg.CommitState = true

				//发送消息
				fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
				fmt.Printf("Commit message: %v.\n", fwd_msg)

				//接收ret消息
				recv_msg = <- trans_ch
				fmt.Printf("Commit ret message: %v.\n", recv_msg)
				/************************************Commit end*****************************/

				//保存所有commit ret消息
				committed_msg = append(committed_msg, recv_msg)

				//如果CommitState为false，则路由失败
				if recv_msg.CommitState == false {
					trans_succ_flag = false
					break
				}
			}
		}
		
		if trans_succ_flag == false {
			// reverse all the committed
			for _, c_msg := range committed_msg {
				/************************************Reverse*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_REVERSE
				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Reverse message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Reverse end*****************************/
			}
			fmt.Printf("Reverse success.\n")
		} else {
			// confirm all the committed
			for _, c_msg := range committed_msg {
				/************************************Confirm*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_CONFIRM
				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Confirm message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Confirm end*****************************/
			}
			fmt.Printf("Confirm success.\n")

			succ_num += 1
			succ_volume += tr.Volume
		}
		
		fmt.Printf("[Trans %d/%d] ******************************** End process ********************************\n", i+1, len(all_trans))

		t_ed := time.Now()
		t_elapsed := t_ed.Sub(t_st)
		//总处理时间
		sum_time += float64(t_elapsed)/float64(time.Millisecond)

		// 恢复拓扑
		G = deepCopy(bk_G)
		
		committed_msg = committed_msg[0:0]
	}

	// 处理后停滞，等待其他节点完成
	time.Sleep(30*time.Second)

	//统计消息数
	var sum_N_msg_probe int = 0
	var sum_N_msg_commit int = 0
	var sum_N_msg_reverse int = 0
	var sum_N_msg_confirm int = 0
	for ind, _ := range nd.Sess_out {
		sum_N_msg_probe += nd.Sess_out[ind].N_msg_probe
		sum_N_msg_commit += nd.Sess_out[ind].N_msg_commit
		sum_N_msg_reverse += nd.Sess_out[ind].N_msg_reverse
		sum_N_msg_confirm += nd.Sess_out[ind].N_msg_confirm
		//输出最终通道余额
		fmt.Printf("balance to %d: %f\n", nd.Sess_out[ind].NI.NodeID, nd.Sess_out[ind].Cap)
	}

	//输出结果
	fmt.Printf("finished %f %d %d %f %f %d %d %d %d 0 0 0 0 0\n", sum_time, 
	succ_num, sum_num, succ_volume, sum_volume,
	sum_N_msg_probe, sum_N_msg_commit, sum_N_msg_reverse, sum_N_msg_confirm)
}

// Spider (with transaction units): thread for handling transactions
func Spider_HandleTrans () {
	//创建交易通道
	trans_ch = make(chan Comm.Msg, 1000)
	
	//初始化统计变量
	var sum_num int = 0
	var succ_num int = 0
	var sum_volume float64 = 0
	var succ_volume float64 = 0
	var sum_time float64 = 0
	
	// 处理前停滞，等待连接完成
	time.Sleep(20*time.Second)

	//依次处理交易
	for i, tr := range all_trans {
		fmt.Printf("[Trans %d/%d] ********************************Start process********************************\n", i+1, len(all_trans))
		
		//统计总支付数量/金额
		sum_num += 1
		sum_volume = sum_volume + tr.Volume

		// handle each transaction
		var fwd_msg Comm.Msg
		var recv_msg Comm.Msg
		var trans_succ_flag bool = false

		//开始计时
		t_st := time.Now()

		//路由表 ————> 潜在路径集合
		var p_paths []Comm.Potential_path
		for _, s_path := range P[tr.Dst] {
			var p_path Comm.Potential_path
			p_path.Capacity = -1
			p_path.PreCommit = 0
			p_path.Nid = s_path.Nid

			p_paths = append(p_paths, p_path)
		}

		// 每笔支付被分割为单位为 1 的支付单元
		remain_vol := tr.Volume
		failure_count := 0

		// 在p_paths上循环commit支付单元
		for ind := 0; ; ind = (ind + 1) % len(p_paths) {

			// 如果剩余支付需求不满1，则commit全部
			commit_amount := 1.0
			if remain_vol < commit_amount {
				commit_amount = remain_vol
			}

			/************************************Commit*****************************/
			//构建消息
			fwd_msg.Type = Comm.MSG_TYPE_COMMIT
			fwd_msg.Src = tr.Src
			fwd_msg.Dst = tr.Dst
			fwd_msg.Path = p_paths[ind].Nid
			fwd_msg.P1c = nil
			fwd_msg.Commit = commit_amount
			fwd_msg.CommitState = true

			//发送消息
			fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
			fmt.Printf("Commit message: %v.\n", fwd_msg)

			//接收ret消息
			recv_msg = <- trans_ch
			fmt.Printf("Commit ret message: %v.\n", recv_msg)
			/************************************Commit end*****************************/

			if recv_msg.CommitState == false {
				failure_count++
				/************************************Reverse*****************************/
				//构建、发送消息
				recv_msg.Type = Comm.MSG_TYPE_REVERSE
				fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
				fmt.Printf("Reverse message: %v.\n", recv_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Reverse end*****************************/
			} else {
				// 更新相关值
				p_paths[ind].PreCommit += commit_amount
				remain_vol -= commit_amount
			}

			if remain_vol <= 0 {  //若remain_vol小于等于0，则路由成功
				trans_succ_flag = true
				break
			}

			if failure_count >= 10 {  //若失败次数大于10，则路由失败
				break
			}

		}

		var c_msg Comm.Msg
		if trans_succ_flag == false {
			// reverse all the path
			for ind, s_path := range p_paths {
				
				p1c := make([]float64, len(s_path.Nid)-1)
				for i := range p1c {
					p1c[i] = s_path.PreCommit  // 均为PreCommit
				}

				/************************************Reverse*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_REVERSE
				c_msg.ReqID = ind
				c_msg.Src = tr.Src
				c_msg.Dst = tr.Dst
				c_msg.Path = s_path.Nid
				c_msg.P1c = p1c  //根据PreCommit赋值

				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Reverse message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Reverse end*****************************/
			}
			fmt.Printf("Reverse success.\n")
		} else {
			// confirm all the committed
			for ind, s_path := range p_paths {

				/************************************Confirm*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_CONFIRM
				c_msg.ReqID = ind
				c_msg.Src = tr.Src
				c_msg.Dst = tr.Dst
				c_msg.Path = s_path.Nid
				c_msg.Commit = s_path.PreCommit  //根据PreCommit赋值

				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Confirm message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Confirm end*****************************/
			}
			fmt.Printf("Confirm success.\n")

			succ_num += 1
			succ_volume += tr.Volume
		}
		
		fmt.Printf("[Trans %d/%d] ******************************** End process ********************************\n", i+1, len(all_trans))

		t_ed := time.Now()
		t_elapsed := t_ed.Sub(t_st)
		//总处理时间
		sum_time += float64(t_elapsed)/float64(time.Millisecond)

		// 恢复拓扑
		G = deepCopy(bk_G)
	}

	// 处理后停滞，等待其他节点完成
	time.Sleep(30*time.Second)

	//统计消息数
	var sum_N_msg_probe int = 0
	var sum_N_msg_commit int = 0
	var sum_N_msg_reverse int = 0
	var sum_N_msg_confirm int = 0
	for ind, _ := range nd.Sess_out {
		sum_N_msg_probe += nd.Sess_out[ind].N_msg_probe
		sum_N_msg_commit += nd.Sess_out[ind].N_msg_commit
		sum_N_msg_reverse += nd.Sess_out[ind].N_msg_reverse
		sum_N_msg_confirm += nd.Sess_out[ind].N_msg_confirm
		//输出最终通道余额
		fmt.Printf("balance to %d: %f\n", nd.Sess_out[ind].NI.NodeID, nd.Sess_out[ind].Cap)
	}

	//输出结果
	fmt.Printf("finished %f %d %d %f %f %d %d %d %d 0 0 0 0 0\n", sum_time, 
	succ_num, sum_num, succ_volume, sum_volume,
	sum_N_msg_probe, sum_N_msg_commit, sum_N_msg_reverse, sum_N_msg_confirm)
}

// LND: thread for handling transactions
func LND_HandleTrans () {
	//创建交易通道
	trans_ch = make(chan Comm.Msg, 1000)

	//初始化统计变量
	var sum_num int = 0
	var succ_num int = 0
	var sum_volume float64 = 0
	var succ_volume float64 = 0
	var sum_time float64 = 0

	// 处理前停滞，等待连接完成
	time.Sleep(20*time.Second)

	//依次处理交易
	for i, tr := range all_trans {
		fmt.Printf("[Trans %d/%d] ********************************Start process********************************\n", i+1, len(all_trans))
		
		//统计总支付数量/金额
		sum_num += 1
		sum_volume = sum_volume + tr.Volume
		
		// handle each transaction
		var fwd_msg Comm.Msg
		var recv_msg Comm.Msg
		var trans_succ_flag bool = false
		var num_attempts int = LND_attempt_cnt //重复尝试次数

		//开始计时
		t_st := time.Now()

		for num_attempts > 0 {
			num_attempts -= 1
			
			//寻路
			path, if_found := Comm.Bfs_shortest_path(G, tr.Src, tr.Dst)
			if if_found == false { // no more path
				break
			}

			//直接支付
			/************************************Commit*****************************/
			//构建消息
			fwd_msg.Type = Comm.MSG_TYPE_COMMIT
			fwd_msg.ReqID = num_attempts
			fwd_msg.Commit = tr.Volume
			fwd_msg.Src = tr.Src
			fwd_msg.Dst = tr.Dst
			fwd_msg.Path = path
			fwd_msg.P1c = nil
			fwd_msg.CommitState = true
			
			//发送消息
			fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
			fmt.Printf("Commit message: %v.\n", fwd_msg)

			//接收ret消息
			recv_msg = <- trans_ch
			fmt.Printf("Commit ret message: %v.\n", recv_msg)
			/************************************Commit end*****************************/

			//如果CommitState为true，则路由成功；否则，再次寻路
			if recv_msg.CommitState == true {
				/************************************Confirm*****************************/
				//构建、发送消息
				recv_msg.Type = Comm.MSG_TYPE_CONFIRM
				fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
				fmt.Printf("Confirm message: %v.\n", recv_msg)
	
				//接收ret消息
				_ = <- trans_ch
				/************************************Confirm end*****************************/

				trans_succ_flag = true
				break
			} else {
				/************************************Reverse*****************************/
				//构建、发送消息
				recv_msg.Type = Comm.MSG_TYPE_REVERSE
				fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
				fmt.Printf("Reverse message: %v.\n", recv_msg)
	
				//接收ret消息
				_ = <- trans_ch
				/************************************Reverse end*****************************/
				
				//更新拓扑，删除余额不够的通道（简单置0）
				for i, value := range recv_msg.P1c {
					if value == 0 {
						G[recv_msg.Path[i]][recv_msg.Path[i+1]] = 0
						break
					}
				}
			}
		}
		
		// 若支付成功，计数（前面已完成释放/确认过程）
		if trans_succ_flag == false {
			fmt.Printf("Reverse success.\n")
		} else {
			fmt.Printf("Confirm success.\n")
			succ_num += 1
			succ_volume += tr.Volume
		}

		fmt.Printf("[Trans %d/%d] ******************************** End process ********************************\n", i+1, len(all_trans))
		
		t_ed := time.Now()
		t_elapsed := t_ed.Sub(t_st)
		//总处理时间
		sum_time += float64(t_elapsed)/float64(time.Millisecond)

		// 恢复拓扑
		G = deepCopy(bk_G)
	}

	// 处理后停滞，等待其他节点完成
	time.Sleep(30*time.Second)

	//统计消息数
	var sum_N_msg_probe int = 0
	var sum_N_msg_commit int = 0
	var sum_N_msg_reverse int = 0
	var sum_N_msg_confirm int = 0
	for ind, _ := range nd.Sess_out {
		sum_N_msg_probe += nd.Sess_out[ind].N_msg_probe
		sum_N_msg_commit += nd.Sess_out[ind].N_msg_commit
		sum_N_msg_reverse += nd.Sess_out[ind].N_msg_reverse
		sum_N_msg_confirm += nd.Sess_out[ind].N_msg_confirm
		//输出最终通道余额
		fmt.Printf("balance to %d: %f.\n", nd.Sess_out[ind].NI.NodeID, nd.Sess_out[ind].Cap)
	}

	//输出结果
	fmt.Printf("finished %f %d %d %f %f %d %d %d %d 0 0 0 0 0\n", sum_time, 
	succ_num, sum_num, succ_volume, sum_volume,
	sum_N_msg_probe, sum_N_msg_commit, sum_N_msg_reverse, sum_N_msg_confirm)
}

// SP: thread for handling transactions
func SP_HandleTrans () {
	//创建交易通道
	trans_ch = make(chan Comm.Msg, 1000)

	//初始化统计变量
	var sum_num int = 0
	var succ_num int = 0
	var sum_volume float64 = 0
	var succ_volume float64 = 0
	var sum_time float64 = 0

	// 处理前停滞，等待连接完成
	time.Sleep(20*time.Second)

	//依次处理交易
	for i, tr := range all_trans {
		fmt.Printf("[Trans %d/%d] ********************************Start process********************************\n", i+1, len(all_trans))
		
		//统计总支付数量/金额
		sum_num += 1
		sum_volume = sum_volume + tr.Volume
		
		// handle each transaction
		var fwd_msg Comm.Msg
		var recv_msg Comm.Msg
		var trans_succ_flag bool = false

		t_st := time.Now() //开始计时

		//寻路
		path, if_found := Comm.Bfs_shortest_path(G, tr.Src, tr.Dst)
		if if_found == false { // no more path
			break
		}

		//直接支付
		/************************************Commit*****************************/
		//构建消息
		fwd_msg.Type = Comm.MSG_TYPE_COMMIT
		fwd_msg.ReqID = 1 //简单置1
		fwd_msg.Commit = tr.Volume
		fwd_msg.Src = tr.Src
		fwd_msg.Dst = tr.Dst
		fwd_msg.Path = path
		fwd_msg.P1c = nil
		fwd_msg.CommitState = true
		
		//发送消息
		fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
		fmt.Printf("Commit message: %v.\n", fwd_msg)

		//接收ret消息
		recv_msg = <- trans_ch
		fmt.Printf("Commit ret message: %v.\n", recv_msg)
		/************************************Commit end*****************************/

		//若CommitState为true，则路由成功
		if recv_msg.CommitState == true {
			trans_succ_flag = true
		}
		
		// 若commit失败，则释放锁定资金；若成功，则进行支付确认
		if trans_succ_flag == false {
			/************************************Reverse*****************************/
			//构建、发送消息
			recv_msg.Type = Comm.MSG_TYPE_REVERSE
			fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
			fmt.Printf("Reverse message: %v.\n", recv_msg)

			//接收ret消息
			_ = <- trans_ch
			/************************************Reverse end*****************************/

			fmt.Printf("Reverse success.\n")

		} else {
			/************************************Confirm*****************************/
			//构建、发送消息
			recv_msg.Type = Comm.MSG_TYPE_CONFIRM
			fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
			fmt.Printf("Confirm message: %v.\n", recv_msg)

			//等待ret消息
			_ = <- trans_ch
			/************************************Confirm end*****************************/

			fmt.Printf("Confirm success.\n")

			succ_num += 1
			succ_volume += tr.Volume
		}

		fmt.Printf("[Trans %d/%d] ******************************** End process ********************************\n", i+1, len(all_trans))
		
		t_ed := time.Now()
		t_elapsed := t_ed.Sub(t_st)

		//总处理时间
		sum_time += float64(t_elapsed)/float64(time.Millisecond)

	}

	// 处理后停滞，等待其他节点完成
	time.Sleep(30*time.Second)

	//统计消息数
	var sum_N_msg_probe int = 0
	var sum_N_msg_commit int = 0
	var sum_N_msg_reverse int = 0
	var sum_N_msg_confirm int = 0
	for ind, _ := range nd.Sess_out {
		sum_N_msg_probe += nd.Sess_out[ind].N_msg_probe
		sum_N_msg_commit += nd.Sess_out[ind].N_msg_commit
		sum_N_msg_reverse += nd.Sess_out[ind].N_msg_reverse
		sum_N_msg_confirm += nd.Sess_out[ind].N_msg_confirm
		//输出最终通道余额
		fmt.Printf("balance to %d: %f.\n", nd.Sess_out[ind].NI.NodeID, nd.Sess_out[ind].Cap)
	}

	//输出结果
	fmt.Printf("finished %f %d %d %f %f %d %d %d %d 0 0 0 0 0\n", sum_time, 
	succ_num, sum_num, succ_volume, sum_volume,
	sum_N_msg_probe, sum_N_msg_commit, sum_N_msg_reverse, sum_N_msg_confirm)
}

// Flash: thread for handling transactions
func Flash_HandleTrans () {
	//创建交易通道
	trans_ch = make(chan Comm.Msg, 1000)

	//所有commit成功的交易
	var committed_msg []Comm.Msg

	//初始化统计变量
	var sum_num int = 0
	var succ_num int = 0
	var sum_volume float64 = 0
	var succ_volume float64 = 0
	var sum_time float64 = 0
	
	// 处理前停滞，等待连接完成
	time.Sleep(20*time.Second)

	//依次处理交易
	for i, tr := range all_trans {
		fmt.Printf("[Trans %d/%d] ********************************Start process********************************\n", i+1, len(all_trans))
		
		//统计总支付数量/金额
		sum_num += 1
		sum_volume = sum_volume + tr.Volume
		
		// handle each transaction
		var fwd_msg Comm.Msg
		var recv_msg Comm.Msg
		var trans_succ_flag bool = false
		var is_short_flow bool = false

		//开始计时
		t_st := time.Now()

		//判断支付类型：大象or老鼠
		if tr.Volume < FS_thresh {
			is_short_flow = true
		}
		
		if is_short_flow == false {  //处理大象支付
			fmt.Printf("It is a big payment, amount: %f.\n", tr.Volume)

			var num_probe int = Flash_probe_cnt  //最大迭代次数
			var max_flow float64 = 0  //最大流
			var probed_paths []Comm.Potential_path //记录探测路径

			//1. 迭代探测路径
			for {
				//剩余迭代次数
				num_probe = num_probe - 1
				if (num_probe < 0) {
					break
				}

				//寻路
				path, if_found := Comm.Bfs_shortest_path(G, tr.Src, tr.Dst)
				if if_found == false { // no more path
					break
				}

				/************************************Probe*****************************/
				//构建消息
				fwd_msg.Type = Comm.MSG_TYPE_PROBE
				fwd_msg.ReqID = num_probe  //剩余探测次数
				fwd_msg.Src = tr.Src
				fwd_msg.Dst = tr.Dst
				fwd_msg.Path = path
				fwd_msg.Cap = nil //重置Cap

				//发送消息
				fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
				fmt.Printf("Probe message: %v.\n", fwd_msg)

				//接收ret消息
				recv_msg = <- trans_ch
				fmt.Printf("Probe ret message: %v.\n", recv_msg)
				/************************************Probe end*****************************/
				
				//更新拓扑余额（仅未探测的边）
				for i:=0; i<=len(recv_msg.Path)-2; i++ {
					if G[recv_msg.Path[i]][recv_msg.Path[i+1]] == Comm.PATH_INITED {
						G[recv_msg.Path[i]][recv_msg.Path[i+1]] = recv_msg.Cap[i]
					}
				}

				//获取路径可用余额
				avail_bal := G[recv_msg.Path[0]][recv_msg.Path[1]]
				for i := 1; i <= len(recv_msg.Path)-2; i++ {
					u := recv_msg.Path[i]
					v := recv_msg.Path[i+1]
					if G[u][v] < avail_bal {
						avail_bal = G[u][v]
					}
				}

				//再次更新拓扑
				if avail_bal < 0 {
					fmt.Println("[ERROR] negative capacity!")
					continue
				} else {
					// 再次更新拓扑余额
					for i:=0; i<=len(recv_msg.Path)-2; i++ { 
						G[recv_msg.Path[i]][recv_msg.Path[i+1]] = G[recv_msg.Path[i]][recv_msg.Path[i+1]] - avail_bal
						//TODO: 若反向余额未被探测，这里是-1 + avail_bal
						G[recv_msg.Path[i+1]][recv_msg.Path[i]] = G[recv_msg.Path[i+1]][recv_msg.Path[i]] + avail_bal
					}
				}

				//记录路径
				var probed_path Comm.Potential_path
				probed_path.Nid = recv_msg.Path
				probed_path.Capacity = avail_bal
				probed_paths = append(probed_paths, probed_path)

				//更新最大流
				max_flow = max_flow + avail_bal
			}

			//打印最大流、支付金额、路径信息
			fmt.Printf("max_flow: %f, tr.Volume: %f.\n", max_flow, tr.Volume)
			for ind, probed_path := range probed_paths {
				fmt.Printf("Probed_path %d: %v, avail_bal：%f.\n", ind, probed_path.Nid, probed_path.Capacity)
			}

			//2. 根据探测结果支付
			if max_flow < tr.Volume {  //若不足以支付，则路由失败
				// do nothing
				fmt.Printf("[Warning] max_flow < tr.Volume, routing fails.\n")
			} else {
				// 迭代commit
				remain_vol := tr.Volume
				for ind, probed_path := range probed_paths {
					
					if remain_vol <= 0 { //若remain_vol小于等于0，则路由成功
						trans_succ_flag = true
						break
					}

					//计算本次commit金额
					var commit_vol float64 = 0
					if remain_vol <= probed_path.Capacity {
						commit_vol = remain_vol
						remain_vol = 0
					} else {
						commit_vol = probed_path.Capacity
						remain_vol -= probed_path.Capacity
					}

					/************************************Commit*****************************/
					//构建消息
					fwd_msg.Type = Comm.MSG_TYPE_COMMIT
					fwd_msg.ReqID = ind
					fwd_msg.Cap = nil
					fwd_msg.Commit = commit_vol  //commit金额
					fwd_msg.Path = probed_path.Nid
					fwd_msg.P1c = nil
					fwd_msg.CommitState = true

					//发送消息
					fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
					fmt.Printf("Commit message: %v.\n", fwd_msg)
	
					//接收ret消息
					recv_msg = <- trans_ch
					fmt.Printf("Commit ret message: %v.\n", recv_msg)
					/************************************Commit end*****************************/

					//保存所有commit ret消息
					committed_msg = append(committed_msg, recv_msg)

					//如果CommitState为false，则路由失败
					if recv_msg.CommitState == false {
						break
					}
				}
			}
		} else { //处理老鼠支付
			fmt.Printf("It is a small payment, amount: %f.\n", tr.Volume)
		
			var n_path_probed int = 0
			var n_path int = len(P[tr.Dst])  //路由表大小
			var path_visited[Flash_path_cnt] bool
			
			//初始化路径探测情况
			for p_i := 0; p_i < n_path; p_i++ {
				path_visited[p_i] = false
				fmt.Printf("Routing table %d/%d: %v.\n", p_i+1, n_path, P[tr.Dst][p_i].Nid)
			}

			//迭代commit ——> probe ——> commit
			remain_vol := tr.Volume
			for {
				if remain_vol <= 0 {  //若remain_vol小于等于0，则路由成功
					trans_succ_flag = true
					break
				}

				//记录探测次数
				n_path_probed += 1
				if n_path_probed > n_path {
					break
				}

				//随机选一条未探测的路
				p_ind := rand.Intn(n_path)
				for path_visited[p_ind] == true {
					p_ind += 1
					p_ind %= n_path
				}
				path_visited[p_ind] = true  //标记为已探测
				
				/************************************Commit*****************************/
				//构建消息
				fwd_msg.Type = Comm.MSG_TYPE_COMMIT
				fwd_msg.ReqID = p_ind // indicate which cached route
				fwd_msg.Src = tr.Src
				fwd_msg.Dst = tr.Dst
				fwd_msg.Cap = nil
				fwd_msg.Commit = remain_vol  //commit金额
				fwd_msg.Path = P[tr.Dst][p_ind].Nid
				fwd_msg.P1c = nil
				fwd_msg.CommitState = true

				//提交消息
				fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
				fmt.Printf("Commit message: %v.\n", fwd_msg)

				//接收ret消息
				recv_msg = <- trans_ch
				fmt.Printf("Commit ret message: %v.\n", recv_msg)
				/************************************Commit end*****************************/

				//若commit失败，则释放、探测并部分commit
				if recv_msg.CommitState == false {
					/************************************Reverse*****************************/
					//构建、发送消息
					recv_msg.Type = Comm.MSG_TYPE_REVERSE
					fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
					fmt.Printf("Reverse message: %v.\n", recv_msg)

					//接收ret消息
					_ = <- trans_ch
					/************************************Reverse end*****************************/

					//迭代probe ——> commit (防止再次commit失败)
					for {
						/************************************Probe*****************************/
						//构建、发送消息
						fwd_msg.Type = Comm.MSG_TYPE_PROBE
						fwd_msg.Cap = nil //重置Cap
						fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
						fmt.Printf("Probe message: %v.\n", fwd_msg)
		
						//接收ret消息
						recv_msg = <- trans_ch
						fmt.Printf("Probe ret message: %v.\n", recv_msg)
						/************************************Probe end*****************************/
						
						//检查路径可用余额
						avail_bal := checkAvailBal(recv_msg.Path, recv_msg.Cap)
						if avail_bal < 0 {
							fmt.Println("[ERROR] negative capacity!")
							avail_bal = 0
						}
						
						//计算本次commit金额
						var commit_vol float64 = 0
						if remain_vol <= avail_bal {
							commit_vol = remain_vol
							remain_vol = 0
						} else {
							commit_vol = avail_bal
							remain_vol -= avail_bal
						}

						/************************************Commit*****************************/
						//构建消息
						fwd_msg.Type = Comm.MSG_TYPE_COMMIT
						fwd_msg.Commit = commit_vol  //commit金额
						fwd_msg.P1c = nil
						fwd_msg.CommitState = true

						//发送消息
						fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
						fmt.Printf("Commit message: %v.\n", fwd_msg)

						//接收ret消息
						recv_msg = <- trans_ch
						fmt.Printf("Commit ret message: %v.\n", recv_msg)
						/************************************Commit end*****************************/
	
						//若commit失败，释放，continue
						if recv_msg.CommitState == false {
							//回滚支付需求
							remain_vol = remain_vol + commit_vol
							
							/************************************Reverse*****************************/
							//构建、发送消息
							recv_msg.Type = Comm.MSG_TYPE_REVERSE
							fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
							fmt.Printf("Reverse message: %v.\n", recv_msg)

							//接收ret消息
							_ = <- trans_ch
							/************************************Reverse end*****************************/

							continue
						} else {	
							//保存“成功的”commit ret消息
							committed_msg = append(committed_msg, recv_msg)
							break
						}
					}
				} else {
					//如果直接支付成功，则支付需求被满足
					remain_vol = 0

					//保存“成功的”commit ret消息
					committed_msg = append(committed_msg, recv_msg)
				}
			}
		}

		if trans_succ_flag == false {
			// reverse all the committed
			for _, c_msg := range committed_msg {
				/************************************Reverse*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_REVERSE
				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Reverse message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Reverse end*****************************/
			}
			fmt.Printf("Reverse success.\n")
		} else {
			// confirm all the committed
			for _, c_msg := range committed_msg {
				/************************************Confirm*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_CONFIRM
				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Confirm message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Confirm end*****************************/
			}
			fmt.Printf("Confirm success.\n")

			succ_num += 1
			succ_volume += tr.Volume
		}
		
		fmt.Printf("[Trans %d/%d] ******************************** End process ********************************\n", i+1, len(all_trans))

		t_ed := time.Now()
		t_elapsed := t_ed.Sub(t_st)
		//总处理时间
		sum_time += float64(t_elapsed)/float64(time.Millisecond)
				
		// 恢复拓扑
		G = deepCopy(bk_G)
	
		//清空切片
		committed_msg = committed_msg[0:0]
	}

	// 处理后停滞，等待其他节点完成
	time.Sleep(30*time.Second)

	//统计消息数
	var sum_N_msg_probe int = 0
	var sum_N_msg_commit int = 0
	var sum_N_msg_reverse int = 0
	var sum_N_msg_confirm int = 0
	for ind, _ := range nd.Sess_out {
		sum_N_msg_probe += nd.Sess_out[ind].N_msg_probe
		sum_N_msg_commit += nd.Sess_out[ind].N_msg_commit
		sum_N_msg_reverse += nd.Sess_out[ind].N_msg_reverse
		sum_N_msg_confirm += nd.Sess_out[ind].N_msg_confirm
		//输出最终通道余额
		fmt.Printf("balance to %d: %f\n", nd.Sess_out[ind].NI.NodeID, nd.Sess_out[ind].Cap)
	}
	
	//输出结果
	fmt.Printf("finished %f %d %d %f %f %d %d %d %d 0 0 0 0 0\n", sum_time, 
	succ_num, sum_num, succ_volume, sum_volume,
	sum_N_msg_probe, sum_N_msg_commit, sum_N_msg_reverse, sum_N_msg_confirm)
}


// 根据给定的子网 ID 集合，将对应的子图合并为一个图
func mergeSubGs(subnetIDs []int) map[int]map[int]float64 {
	merged := make(map[int]map[int]float64)

	for _, sid := range subnetIDs {
		graph := subGs[sid]

		for u, neighbors := range graph {
			if merged[u] == nil {
				merged[u] = make(map[int]float64)
			}
			for v, cap := range neighbors {
				merged[u][v] = cap
			}
		}
	}
	return merged
}

func getSubnetIDs(nid int) []int {
	var subnetIDs []int
	for sid, graph := range subGs {
		if _, exists := graph[nid]; exists {
			subnetIDs = append(subnetIDs, sid)
		}
	}
	return subnetIDs
}

// SegFlow: thread for handling transactions
func SegFlow_HandleTrans () {
	//创建交易通道
	trans_ch = make(chan Comm.Msg, 1000)

	//所有commit成功的交易
	var committed_msg []Comm.Msg

	//初始化统计变量
	var sum_num int = 0
	var succ_num int = 0
	var sum_volume float64 = 0
	var succ_volume float64 = 0
	var sum_time float64 = 0

	var subnet_num int = 0
	var subnet_succ_num int = 0
	var subnet_volume float64 = 0
	var subnet_succ_volume float64 = 0
	var subnet_time float64 = 0

	// 处理前停滞，等待连接完成
	time.Sleep(20*time.Second)

	//依次处理交易
	for i, tr := range all_trans {
		fmt.Printf("[Trans %d/%d] ********************************Start process********************************\n", i+1, len(all_trans))
		
		//统计总支付数量/金额
		sum_num += 1
		sum_volume = sum_volume + tr.Volume

		// handle each transaction
		var fwd_msg Comm.Msg
		var recv_msg Comm.Msg
		var trans_succ_flag bool = false
		var num_attempts int = LND_attempt_cnt //重复尝试次数

		var is_subnet_flow bool = false

		//开始计时
		t_st := time.Now()

		//判断支付类型：子网内or子网间
		inter_set := getSubnetIDs(tr.Dst)
		if len(inter_set) != 0 {
			fmt.Printf("inter set: %v.\n", inter_set)
			is_subnet_flow = true
			subnet_num += 1
			subnet_volume += tr.Volume
		}

		if is_subnet_flow == true {  //处理子网内支付
			fmt.Printf("It is a subnet payment, amount: %f.\n", tr.Volume)

			var subG map[int]map[int]float64
			if len(inter_set) > 1 {
				subG = mergeSubGs(inter_set)
				fmt.Printf("merge subGs!.\n")
			}else {
				subG = subGs[inter_set[0]]
			}

			for num_attempts > 0 {
				num_attempts -= 1
			
				//子网寻路
				path, if_found := Comm.Bfs_shortest_path(subG, tr.Src, tr.Dst)
				if if_found == false { // no more path
					fmt.Printf("no more path!.\n")
					break
				}

				/************************************Commit*****************************/
				//构建消息
				fwd_msg.Type = Comm.MSG_TYPE_COMMIT
				fwd_msg.ReqID = num_attempts
				fwd_msg.Commit = tr.Volume
				fwd_msg.Src = tr.Src
				fwd_msg.Dst = tr.Dst
				fwd_msg.Path = path
				fwd_msg.P1c = nil
				fwd_msg.CommitState = true
			
				//发送消息
				fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
				fmt.Printf("Commit message: %v.\n", fwd_msg)

				//接收ret消息
				recv_msg = <- trans_ch
				fmt.Printf("Commit ret message: %v.\n", recv_msg)
				/************************************Commit end*****************************/

				//如果CommitState为true，则路由成功；否则，再次寻路
				if recv_msg.CommitState == true {
					/************************************Confirm*****************************/
					//构建、发送消息
					recv_msg.Type = Comm.MSG_TYPE_CONFIRM
					fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
					fmt.Printf("Confirm message: %v.\n", recv_msg)
	
					//接收ret消息
					_ = <- trans_ch
					/************************************Confirm end*****************************/

					trans_succ_flag = true
					break
				} else {
					/************************************Reverse*****************************/
					//构建、发送消息
					recv_msg.Type = Comm.MSG_TYPE_REVERSE
					fwdmsg_SessOut_by_nid(&recv_msg, recv_msg.Path[1])
					fmt.Printf("Reverse message: %v.\n", recv_msg)
	
					//接收ret消息
					_ = <- trans_ch
					/************************************Reverse end*****************************/
				
					//更新拓扑，删除余额不够的通道（简单置0）
					for i, value := range recv_msg.P1c {
						if value == 0 {
							subG[recv_msg.Path[i]][recv_msg.Path[i+1]] = 0
							break
						}
					}
				}
			}
		}

		if trans_succ_flag == true { 
			subnet_succ_num += 1
			subnet_succ_volume += tr.Volume
		} else {  //处理跨子网支付
			fmt.Printf("It is a non-subnet payment, amount: %f.\n", tr.Volume)

			//1. 迭代探测路由表中路径，并更新至temp_G
			temp_G := make(map[int]map[int]float64)
			for ind, s_path := range P[tr.Dst] {
				/************************************Probe*****************************/
				//构建消息
				fwd_msg.Type = Comm.MSG_TYPE_PROBE
				fwd_msg.ReqID = ind
				fwd_msg.Src = tr.Src
				fwd_msg.Dst = tr.Dst
				fwd_msg.Path = s_path.Nid
				fwd_msg.Cap = nil //重置Cap

				//发送消息
				fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
				fmt.Printf("Probe message: %v.\n", fwd_msg)

				//接收ret消息
				recv_msg = <- trans_ch
				fmt.Printf("Probe ret message: %v.\n", recv_msg)
				/************************************Probe end*****************************/

				//更新temp_G
				for i:=0; i<=len(recv_msg.Path)-2; i++ {
					u := recv_msg.Path[i]
					v := recv_msg.Path[i+1]
					if temp_G[u] == nil {
						temp_G[u] = make(map[int]float64)
					}
					if temp_G[v] == nil {
						temp_G[v] = make(map[int]float64)
					}
					temp_G[u][v] = recv_msg.Cap[i]
					temp_G[v][u] = 0
				}
			}

			//2. 计算k路径最大流
			max_flow, paths, flows := Comm.MaxFlow(temp_G, tr.Src, tr.Dst)
			fmt.Printf("Max flow: %f.\n", max_flow)

			//3. 根据最大流结果commit支付
			if max_flow < tr.Volume {  // 若不足以支付，则路由失败
				// do nothing
				fmt.Printf("[Warning] max_flow < tr.Volume, inter-subnet routing fails.\n")
			} else {
				remain_vol := tr.Volume
				for i := 0; i < len(paths); i++ {

					//计算本次commit金额
					cap := flows[i]
					commit_vol := cap
					if cap > remain_vol {
						commit_vol = remain_vol
						remain_vol = 0
					} else {
						remain_vol -= cap
					}

					/************************************Commit*****************************/
					//构建消息
					fwd_msg.Type = Comm.MSG_TYPE_COMMIT
					fwd_msg.ReqID = i
					fwd_msg.Cap = nil
					fwd_msg.Commit = commit_vol  //commit金额
					fwd_msg.Path = paths[i]
					fwd_msg.P1c = nil
					fwd_msg.CommitState = true

					//发送消息
					fwdmsg_SessOut_by_nid(&fwd_msg, fwd_msg.Path[1])
					fmt.Printf("Commit message: %v.\n", fwd_msg)
	
					//接收ret消息
					recv_msg = <- trans_ch
					fmt.Printf("Commit ret message: %v.\n", recv_msg)
					/************************************Commit end*****************************/

					//保存所有commit ret消息
					committed_msg = append(committed_msg, recv_msg)

					//如果CommitState为false，则路由失败
					if recv_msg.CommitState == false {
						break
					}

					if remain_vol <= 1e-6 {  //若remain_vol小于等于0，则路由成功
						trans_succ_flag = true
						break
					}

				}
			}

		}

		// 若支付失败/成功，释放/确认资金，并计数
		if trans_succ_flag == false {
			// reverse all the committed
			for _, c_msg := range committed_msg {
				/************************************Reverse*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_REVERSE
				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Reverse message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Reverse end*****************************/
			}
			
			fmt.Printf("Reverse success.\n")
		} else {
			// confirm all the committed
			for _, c_msg := range committed_msg {
				/************************************Confirm*****************************/
				//构建、发送消息
				c_msg.Type = Comm.MSG_TYPE_CONFIRM
				fwdmsg_SessOut_by_nid(&c_msg, c_msg.Path[1])
				fmt.Printf("Confirm message: %v.\n", c_msg)

				//接收ret消息
				_ = <- trans_ch
				/************************************Confirm end*****************************/
			}

			fmt.Printf("Confirm success.\n")
			succ_num += 1
			succ_volume += tr.Volume
		}

		fmt.Printf("[Trans %d/%d] ******************************** End process ********************************\n", i+1, len(all_trans))
		
		t_ed := time.Now()
		t_elapsed := t_ed.Sub(t_st)
		//总处理时间
		sum_time += float64(t_elapsed)/float64(time.Millisecond)
		if is_subnet_flow == true {  // 所有子网内支付的处理时间
			subnet_time += float64(t_elapsed)/float64(time.Millisecond)
		}

		// 恢复子网拓扑
		for sid, backup := range bk_subGs {
			subGs[sid] = deepCopy(backup)
		}
		// 恢复拓扑
		G = deepCopy(bk_G)

		//清空切片
		committed_msg = committed_msg[0:0]
	}

	// 处理后停滞，等待其他节点完成
	time.Sleep(30*time.Second)

	//统计消息数
	var sum_N_msg_probe int = 0
	var sum_N_msg_commit int = 0
	var sum_N_msg_reverse int = 0
	var sum_N_msg_confirm int = 0
	for ind, _ := range nd.Sess_out {
		sum_N_msg_probe += nd.Sess_out[ind].N_msg_probe
		sum_N_msg_commit += nd.Sess_out[ind].N_msg_commit
		sum_N_msg_reverse += nd.Sess_out[ind].N_msg_reverse
		sum_N_msg_confirm += nd.Sess_out[ind].N_msg_confirm
		//输出最终通道余额
		fmt.Printf("balance to %d: %f.\n", nd.Sess_out[ind].NI.NodeID, nd.Sess_out[ind].Cap)
	}

	//输出结果 (末尾5个输出值，与子网内支付相关)
	//总时间，成功数，总支付数，成功金额，总金额，Probe消息，Commit消息，Reverse消息，Confirm消息，子网支付时间，子网支付成功数，子网支付总数，子网支付成功金额，子网支付总金额
	fmt.Printf("finished %f %d %d %f %f %d %d %d %d %f %d %d %f %f\n", sum_time, 
	succ_num, sum_num, succ_volume, sum_volume,
	sum_N_msg_probe, sum_N_msg_commit, sum_N_msg_reverse, sum_N_msg_confirm, subnet_time, subnet_succ_num, subnet_num, subnet_succ_volume, subnet_volume)

}


func HandleSesOut (ses *Comm.Session) {
	//依次处理会话通道内的消息
	for msg := range ses.Msg_ch {

		//输出Sess_out队列长度
		//log.Printf("Current Sess_out to %d message queue length: %d.\n", ses.NI.NodeID, len(ses.Msg_ch))

		ses.CapLock.Lock()
		if msg.Type == Comm.MSG_TYPE_PROBE {          //Probe消息
			ses.N_msg_probe += 1
			//附上通道余额
			msg.Cap = append(msg.Cap, ses.Cap)
		} else if msg.Type == Comm.MSG_TYPE_PROBE_RET {          //Probe_Ret消息
			// do nothing
		} else if msg.Type == Comm.MSG_TYPE_COMMIT {          //Commit消息
			
			if msg.CommitState == true {  // 若commit未失败
				if msg.Commit <= ses.Cap { // 若通道资金充足
					ses.Cap = ses.Cap - msg.Commit // 锁定相应资金
					msg.P1c = append(msg.P1c, msg.Commit) // 记录锁定资金
					ses.N_msg_commit += 1 //仅成功commit时，才计数
				} else {  // 若通道资金不足，则标记为失败commit，等待释放
					msg.CommitState = false
					msg.P1c = append(msg.P1c, 0) // 记录锁定资金
				}
			} else {  // 若commit已失败
				msg.P1c = append(msg.P1c, 0) // 记录锁定资金
			}
		} else if msg.Type == Comm.MSG_TYPE_COMMIT_RET {          //Commit_Ret消息			
			// do nothing
		} else if msg.Type == Comm.MSG_TYPE_REVERSE {          //Reverse消息
			ses.N_msg_reverse += 1

			var next_hopNid int = -1
			var updt_cap_ind int = -1

			// 确定目标节点
			for i, nid := range(msg.Path) {
				if nid == node_conf.NodeID {
					next_hopNid = msg.Path[i+1]
					updt_cap_ind = i  //P1c位置
					break
				}
			}
			
			//fmt.Printf("[Reverse] sess_out to: %d, reverse_cap: %f.\n", next_hopNid, msg.P1c[updt_cap_ind])

			if updt_cap_ind < 0 {
				log.Println("[ERROR] fatal error when release.")
			}
			//根据P1c记录释放资金
			releaseSesOutCap(&msg, next_hopNid, updt_cap_ind)
		} else if msg.Type == Comm.MSG_TYPE_REVERSE_RET {          //Reverse_Ret消息
			// do nothing
		} else if msg.Type == Comm.MSG_TYPE_CONFIRM {          //Confirm消息
			// do nothing
			ses.N_msg_confirm += 1
		} else if msg.Type == Comm.MSG_TYPE_CONFIRM_RET {          //Confirm_Ret消息
			//转移资金
			ses.Cap = ses.Cap + msg.Commit
		} else {
			log.Printf("[OUT] error message type, HandleSesOut to [%d]\n", ses.NI.NodeID)
		}
		ses.CapLock.Unlock()

		// 消息编码为JSON格式
		b, err := json.Marshal(msg)
		if err != nil {
			log.Println("[OUT] error HandleSesOut Json Marshal", err)
			continue
		}
		// send over network
		wr_len, err := ses.Conn.Write(b)
		// 如果实际写入的字节数与数据长度不相等，则报错
		if err!=nil || wr_len!=len(b) {
			log.Println("[OUT] error HandleSesOut send data", err)
			ses.Conn.Close()
			break
		}
	}
}

//检查路径可用余额
func checkAvailBal(path []int, cap []float64) float64 {
	availBal := float64(math.MaxFloat64)

	for i:=0; i<=len(path)-2; i++ {
		if cap[i] < availBal {
			availBal = cap[i]
		}
	}
	return availBal
}

//释放锁定资金
func releaseSesOutCap (msg *Comm.Msg, prev_nodeid int, updt_ind int) {
	var test_found_flag = false
	
	// 遍历Sess_out集合，寻找合适的Session(out)会话
	for i, _ := range nd.Sess_out {
		if nd.Sess_out[i].NI.NodeID == prev_nodeid {
			test_found_flag = true
			
			//释放锁定金额
			//nd.Sess_out[i].CapLock.Lock()
			nd.Sess_out[i].Cap = nd.Sess_out[i].Cap + msg.P1c[updt_ind]
			
			if nd.Sess_out[i].Cap < 0 {
				fmt.Printf("[ERROR] update sess out capacity error.")
			}
			//nd.Sess_out[i].CapLock.Unlock()
			break
		}
	}
	if test_found_flag == false {
		log.Println("[ERROR] update sess out capacity error.")
	}
}

func HandleSesIn (conn *net.TCPConn) {
	data := make([]byte, 102400)  //消息缓冲区:服务器尝试1500-200000(节点数-交易数)时使用

	//循环接收并解析消息
	for {
		rd_len, err := conn.Read(data)
		if err != nil {
			log.Println("[IN] error HandleSesIn receive data from:", conn.RemoteAddr().String(), err)
			conn.Close()
			break
		}

		//解码JSON消息
		j_dec := json.NewDecoder(strings.NewReader(string(data[:rd_len])))
		for {

			var json_msg Comm.Msg

			//尝试将其解码为Msg类型的消息
			if err = j_dec.Decode(&json_msg); err == io.EOF {
				break
			} else if err != nil {
				log.Println("[IN] error json", err, rd_len)
				break
			}

			// handle MSG in
			var next_hopNid int = -1
			var need_forward bool = false
			var test_found_flag bool = false
			
			if node_conf.NodeID==json_msg.Dst {
				// 1a. 如果当前节点是接收者
				need_forward = true  //表示消息需要转发
				
				// 确定消息转发的目标节点
				next_hopNid = json_msg.Path[len(json_msg.Path)-2]
				if json_msg.Type==Comm.MSG_TYPE_PROBE {
					json_msg.Type = Comm.MSG_TYPE_PROBE_RET
				} else if json_msg.Type==Comm.MSG_TYPE_COMMIT {
					json_msg.Type = Comm.MSG_TYPE_COMMIT_RET
				} else if json_msg.Type==Comm.MSG_TYPE_REVERSE {
					json_msg.Type = Comm.MSG_TYPE_REVERSE_RET
				} else if json_msg.Type==Comm.MSG_TYPE_CONFIRM {
					json_msg.Type = Comm.MSG_TYPE_CONFIRM_RET
				} else {
					log.Println("[ERROR] Message type error!")
				}
			} else if node_conf.NodeID==json_msg.Src {
				// 1b. 如果当前节点是发送者

				if json_msg.Type==Comm.MSG_TYPE_PROBE_RET {
					trans_ch <- json_msg  //消息置入交易消息通道
				} else if json_msg.Type==Comm.MSG_TYPE_COMMIT_RET {
					trans_ch <- json_msg  //消息置入交易消息通道
				} else if json_msg.Type==Comm.MSG_TYPE_REVERSE_RET {
					// just consume it
					trans_ch <- json_msg  //消息置入交易消息通道
				} else if json_msg.Type==Comm.MSG_TYPE_CONFIRM_RET {
					// just consume it
					trans_ch <- json_msg  //消息置入交易消息通道
				} else {
					log.Println("[IN] error, supposed to be RET msgs")
				}
			} else {
				// 2. 如果当前节点是中间节点
				need_forward = true  //表示消息需要转发

				// 确定消息转发的目标节点
				for i, nid := range(json_msg.Path) {
					if nid == node_conf.NodeID {
						if json_msg.Type==Comm.MSG_TYPE_PROBE || 
						json_msg.Type==Comm.MSG_TYPE_COMMIT ||
						json_msg.Type==Comm.MSG_TYPE_REVERSE ||
						json_msg.Type==Comm.MSG_TYPE_CONFIRM {
							next_hopNid = json_msg.Path[i+1]
						} else {
							next_hopNid = json_msg.Path[i-1]
						}
						break
					}
				}
				// just forward to related out_Session since we do calculating at out_Session
			}
			
			//打印消息
			log.Printf("Message: %v.\n", json_msg)

			// if needed, forward msg
			if need_forward == true {
				test_found_flag = false
				//确定目标节点，并转发消息
				for i, _ := range(nd.Sess_out) {
					if next_hopNid == nd.Sess_out[i].NI.NodeID {
						test_found_flag = true
						nd.Sess_out[i].Msg_ch <- json_msg
						break
					}
				}
				if test_found_flag == false {
					log.Println("[IN] error forward msg!!!", json_msg)
				}
			}
		}

	}
}

func connect_neig() {
	// just sleep some time to wait other nodes
	time.Sleep(20*time.Second) //服务器尝试500-100000时使用

	// connect to neighbour
	var err error = nil
	var conn *net.TCPConn 
	var sess Comm.Session
	for _, ngcf := range(neig_conf.Conf) {
		
		//建立TCP连接
		conn, err = net.DialTCP("tcp4", nil, &net.TCPAddr{net.ParseIP(ngcf.Ip), ngcf.Port, ""})
		if err != nil {
			log.Printf("[ERROR] connect %s: %d\n", ngcf.Ip, ngcf.Port)
		}

		//输出系统分配的IP
		localAddr := conn.LocalAddr().(*net.TCPAddr)
		log.Printf("Local IP:%s: %d", localAddr.IP.String(), localAddr.Port)

		//保持TCP连接，保活探测包的发送间隔为5s
		// NOTE: NoDelay is default on
		err = conn.SetKeepAlive(true)
		if err != nil {
			log.Printf("[ERROR]", err)
		}
		err = conn.SetKeepAlivePeriod(5*time.Second)
		if err != nil {
			log.Printf("[ERROR]", err)
		}

		//创建新会话Session(out)，并添加至Sess_out集合
		sess.NI = ngcf
		sess.Conn = *conn
		sess.Cap = ngcf.Cap		
		sess.Msg_ch = make(chan Comm.Msg, 2000) // make channel
		sess.N_msg_probe = 0
		sess.N_msg_commit = 0
		sess.N_msg_reverse = 0
		sess.N_msg_confirm = 0
		nd.Sess_out = append(nd.Sess_out, sess)
		
		log.Printf("[Connect] to nid[%d] cap[%f], Sess_out: %v.\n", sess.NI.NodeID, sess.Cap, sess)
	}

	//对于每个Session(out)，创建一个goroutine调用HandleSesOut函数来处理输出会话的消息
	for i, _ := range nd.Sess_out {
		go HandleSesOut(&(nd.Sess_out[i]))
	}

	//创建一个goroutine调用HandleTrans函数来处理交易
	if g_algo == 1 {
		go Flash_HandleTrans()
	} else if g_algo == 2 {
		go SP_HandleTrans()
	} else if g_algo == 3 {
		go Waterfilling_HandleTrans()
	} else if g_algo == 4 {
		go Spider_HandleTrans()
	} else if g_algo == 5 {
		go LND_HandleTrans()
	} else if g_algo == 6 {
		go SegFlow_HandleTrans()
	} else {
		fmt.Println("algo not defined")
		os.Exit(-1)
	}
}

//读取节点相关配置
func read_conf(conf_filename, neig_conf_filename string) {
	//解析节点配置文件
	conf_data, err := ioutil.ReadFile(conf_filename)
	if err != nil {
		log.Println("err read node_conf file")
		os.Exit(-1)
	}

	err = json.Unmarshal(conf_data, &node_conf)
	if err != nil {
		log.Println("err node_conf json parse")
		os.Exit(-1)
	}
	nd.NI = node_conf

	//解析邻居节点配置文件
	conf_data, err = ioutil.ReadFile(neig_conf_filename)
	if err != nil{
		log.Println("err read neig_conf file")
		os.Exit(-1)
	}

	err = json.Unmarshal(conf_data, &neig_conf.Conf)
	if err != nil {
		log.Println("err neig_conf json parse")
		os.Exit(-1)
	}
}

func main() {

	//系统信号处理
	go handle_sig()

	var err error = nil
	
	//系统参数检查
	if 8 > len(os.Args) {
		log.Println("[ERROR] Usage: ./server <\\${n}.json> <n\\${n}.json> <graph file> <tr\\${n}.txt> <pa\\${n}.txt> <algo> <node id>")
		os.Exit(-1)
	}

	//获取目标算法
	g_algo, _ = strconv.Atoi(os.Args[6])
	//获取节点ID
	g_nodeid, _ = strconv.Atoi(os.Args[7])
	log.Println("Node ID: ", g_nodeid)

	//加载全网拓扑、交易、路由表
	if init_G(os.Args[3]) == false {
		log.Println("[ERROR] read graph file error")
		os.Exit(-1)
	}
	fmt.Println("[Init] Graph loaded.")
	if load_trans(os.Args[4]) == false {
		log.Println("[ERROR] read trans file error")
		os.Exit(-1)
	}
	fmt.Println("[Init] Trans loaded.")
	if load_paths(os.Args[5]) == false {
		log.Println("[ERROR] read paths file error")
		os.Exit(-1)
	}
	fmt.Println("[Init] Paths loaded.")

	//读取节点相关配置
	read_conf(os.Args[1], os.Args[2])

	//加载子网拓扑subG
	if init_subGs(node_conf.SubnetIDs) == false {
		log.Println("[ERROR] read subgraph file error")
		os.Exit(-1)
	}
	fmt.Println("[Init] subGraph loaded.")

	//创建TCP监听器，监听节点IP端口
	nd.Ln, err = net.ListenTCP("tcp4", &net.TCPAddr{net.ParseIP(nd.NI.Ip), nd.NI.Port, ""})
	if err != nil {
		log.Println("[ERROR] node initilization error,", err)
		os.Exit(-1)
	}

	//与邻居节点建立连接
	go connect_neig()
	
	//处理接入TCP连接
	var sess Comm.Session
	for is_active {
		//监听接入连接
		conn, err := nd.Ln.AcceptTCP()

		if err != nil {
			log.Printf("[ERROR] accept connection error,", err)
			continue
		}

		//创建新会话Session(in)，并置入Sess_in集合
		sess.Conn = *conn
		sess.Msg_ch = make(chan Comm.Msg, 1000)
		nd.Sess_in = append(nd.Sess_in, sess)
		log.Printf("[Connect] by nid[%d], ip[%s], cap[%f], Sess_in: %v.\n", sess.NI.NodeID, conn.RemoteAddr().String(), sess.Cap, sess)

		go HandleSesIn(conn)
	}

}
