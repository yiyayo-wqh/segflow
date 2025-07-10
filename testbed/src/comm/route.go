package Comm

import "math"

const PATH_INITED = -1

func Bfs_shortest_path(graph map[int]map[int]float64, src, dst int) ([]int, bool) {
	visited := make(map[int]bool)
	prev := make(map[int]int)
	queue := []int{src}
	visited[src] = true

	// BFS遍历
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		// 遍历所有邻居
		for neighbor, cap := range graph[curr] {
			if !visited[neighbor] && (cap>0 || cap==PATH_INITED) {
				visited[neighbor] = true
				prev[neighbor] = curr
				queue = append(queue, neighbor)

				// 提前终止
				if neighbor == dst {
					goto BUILD_PATH
				}
			}
		}
	}
	// 未找到路径
	return nil, false

BUILD_PATH:
	// 回溯构建路径
	var path []int
	for at := dst; at != src; at = prev[at] {
		path = append([]int{at}, path...)
	}
	path = append([]int{src}, path...)
	return path, true
}


func MaxFlow(graph map[int]map[int]float64, src, dst int) (float64, [][]int, []float64) {
	var maxFlow float64 = 0
	var allPaths [][]int
	var allFlows []float64

	for {
		// BFS 查找增广路径
		path, if_found := Bfs_shortest_path(graph, src, dst)
		if !if_found {
			break // 如果没有增广路径，结束
		}

		// 计算路径余额
		pathFlow := math.MaxFloat64
		for i := 1; i < len(path); i++ {
			u := path[i-1]
			v := path[i]
			if graph[u][v] < pathFlow {
				pathFlow = graph[u][v]
			}
		}

		// 更新图
		for i := 1; i < len(path); i++ {
			u := path[i-1]
			v := path[i]
			graph[u][v] -= pathFlow
			graph[v][u] += pathFlow
		}

		// 记录路径和流量
		allPaths = append(allPaths, path)
		allFlows = append(allFlows, pathFlow)
		maxFlow += pathFlow
	}

	return maxFlow, allPaths, allFlows
}