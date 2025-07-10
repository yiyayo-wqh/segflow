package main

//TODO:转为python

import (
	"fmt"
	"os"
	"strconv"
	"io"
	"bufio"
	"strings"
	"encoding/json"

	//
	"comm"
)

type Edge struct {
	Dst		int
	Cap		float64
}

type Tran struct {
	Dst		int
	Vol		float64
}

type Path struct {
	Dst		int
	Pa		[]int
}


var G map[int][]Edge
var T map[int][]Tran
var P map[int][]Path


// write to file
var node_cgf map[int]Comm.NodeInfo
var neig_cgf map[int][]Comm.NodeInfo
//
var NIP string
var N_node int


// 加载节点-子网ID映射 (node, subnetid1, subnetid2, ...)
func load_subnet_map(filename string) map[int][]int {
	subnetMap := make(map[int][]int)

	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("error reading subnet map file:", err)
		os.Exit(-1)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}
		fields := strings.Split(string(line), ",")
		if len(fields) < 2 {
			fmt.Println("invalid subnet map line:", string(line))
			continue
		}

		nodeID, _ := strconv.Atoi(fields[0])
		for i := 1; i < len(fields); i++ {
			subnetID, _ := strconv.Atoi(fields[i])
			subnetMap[nodeID] = append(subnetMap[nodeID], subnetID)
		}
	}

	return subnetMap
}


func main() {
	fmt.Println("Parse Graph")
	G = make(map[int][]Edge)
	T = make(map[int][]Tran)
	P = make(map[int][]Path)
	node_cgf = make(map[int]Comm.NodeInfo)
	neig_cgf = make(map[int][]Comm.NodeInfo)

	if 6 != len(os.Args) {
		fmt.Println("usage: ./parse_graph <graph file> <transactions file> <paths file> <node ip> <number of nodes>")
		os.Exit(-1)
	}

	NIP = os.Args[4] //节点IP: 127.0.0.1
	N_node, _ = strconv.Atoi(os.Args[5]) //节点数量

	/******************************* handle graph file *******************************/
	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println("error reading graph file", err)
		os.Exit(-1)
	}

	defer file.Close()

	br := bufio.NewReader(file)

	for {
		var edge Edge
		line, _, err := br.ReadLine()
		if err == io.EOF {
			break;
		}
		// split by ','
		lines := strings.Split(string(line), ",")
		if 3 != len(lines) {
			fmt.Println("bad format in graph file qnmlgb")
			break;
		}

		src, _ := strconv.Atoi(lines[0])
		src += 1
		dst, _ := strconv.Atoi(lines[1])
		dst += 1
		tmp_float, _ := strconv.Atoi(lines[2])

		edge.Dst = dst
		edge.Cap = float64(tmp_float)

		G[src] = append(G[src], edge)
	}
	/*******************************  end *******************************/


	// 导入{节点-子网ID}映射
	subnetMap := load_subnet_map("subnet_map.csv")
	/******************************* end *******************************/

	/******************************* generate nodeinfo and neiginfo *******************************/
	for k, v := range G {
		var nodeinfo Comm.NodeInfo
		var neiginfo Comm.NodeInfo
		nodeinfo.NodeID = k
		nodeinfo.Ip = NIP
		nodeinfo.Port = 20001+k
		nodeinfo.Cap = 0
		nodeinfo.SubnetIDs = subnetMap[k-1]
		node_cgf[k] = nodeinfo
		for _, neig := range v {
			neiginfo.NodeID = neig.Dst
			neiginfo.Ip = NIP
			neiginfo.Port = 20001+neig.Dst
			neiginfo.Cap = neig.Cap
			neiginfo.SubnetIDs = subnetMap[neig.Dst-1]
			neig_cgf[k] = append(neig_cgf[k], neiginfo)
		}
	}
	/******************************* end *******************************/



	// handle trans file
	trfile, err := os.Open(os.Args[2])
	if err != nil {
		fmt.Println("error reading graph file", err)
		os.Exit(-1)
	}

	defer trfile.Close()

	trbr := bufio.NewReader(trfile)

	for {
		var tran Tran
		line, _, err := trbr.ReadLine()
		if err == io.EOF {
			break;
		}
		// split by ','
		lines := strings.Split(string(line), ",")
		if 3 != len(lines) {
			fmt.Println("bad format in trans file qnmlgb")
			break;
		}

		src, _ := strconv.Atoi(lines[0])
		src += 1
		dst, _ := strconv.Atoi(lines[1])
		dst += 1
		tmp_float, _ := strconv.ParseFloat(lines[2], 32)

		tran.Dst = dst
		tran.Vol = float64(tmp_float)
		fmt.Printf("%d %d %f\n", src, tran.Dst, tran.Vol)

		T[src] = append(T[src], tran)
	}

	// handle path file
	pathfile, err := os.Open(os.Args[3])
	if err != nil {
		fmt.Println("error reading path file", err)
		os.Exit(-1)
	}
	defer pathfile.Close()

	pathbr := bufio.NewReader(pathfile)

	for {
		var pa Path
		line, _, err := pathbr.ReadLine()
		if err == io.EOF {
			break;
		}
		// split by ','
		lines := strings.Split(string(line), ",")
		if 4 > len(lines) {
			fmt.Println("bad format in path file qnmlgb")
			break;
		}

		src, _ := strconv.Atoi(lines[0])
		src += 1
		dst, _ := strconv.Atoi(lines[1])
		dst += 1
		var t_path []int = nil
		for ind:=2; ind<len(lines); ind++ {
			node_id, _ := strconv.Atoi(lines[ind])
			node_id += 1
			t_path = append(t_path, node_id)
		}

		pa.Dst = dst
		pa.Pa = t_path

		P[src] = append(P[src], pa)
	}


	// write node conf file
	for k, v := range node_cgf {
		n_filename := fmt.Sprintf("%d.json", k)
		fp, err := os.Create(n_filename)
		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		b, err := json.Marshal(v)

		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		_, err = fp.WriteString(string(b))
		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		fp.Close()
	}


	// write node neig conf file
	for k, v := range neig_cgf {
		n_filename := fmt.Sprintf("n%d.json", k)
		fp, err := os.Create(n_filename)
		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		b, err := json.Marshal(v)

		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		_, err = fp.WriteString(string(b))
		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		fp.Close()
	}

	// write trans file

	for k:=1; k<=N_node; k++ {
		tr_filename := fmt.Sprintf("tr%d.txt", k)
		fp, err := os.Create(tr_filename)
		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		for _, s_tran := range T[k] {
			line := fmt.Sprintf("%d,%d,%f\n", k, s_tran.Dst, s_tran.Vol)
			_, err = fp.WriteString(line)
			if err != nil {
				fmt.Println("error =", err)
				continue
			}
		}

		fp.Close()
	}

	// write path file
	for k:=1; k<=N_node; k++ {
		pa_filename := fmt.Sprintf("pa%d.txt", k)
		fp, err := os.Create(pa_filename)
		if err != nil {
			fmt.Println("error =", err)
			continue
		}

		for _, t_path := range P[k] {
			line := fmt.Sprintf("%d,", t_path.Dst)
			for ind:=0; ind<len(t_path.Pa)-1; ind++ {
				line = fmt.Sprintf("%s%d,", line, t_path.Pa[ind])
			}
			line = fmt.Sprintf("%s%d\n", line, t_path.Pa[len(t_path.Pa)-1])

			_, err = fp.WriteString(line)
			if err != nil {
				fmt.Println("error =", err)
				continue
			}
		}

		fp.Close()
	}
}
