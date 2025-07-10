import numpy as np
import random
from datetime import datetime
random.seed(datetime.now().timestamp())


# assign coordinates to all nodes using BFS
# first consider nodes with directional channels; then nodes with unidirectional channels
def setRoute(G, landmarks, nb, coordinate, parent):
	L = len(landmarks)
	N = len(G)

	# 分配坐标
	for l in range(L):
		q = []  # 创建队列
		root = landmarks[l]
		q.append(root)
		bi_flag = True  # 第一轮遍历(分配双向通道) or 第二轮遍历(分配单向通道)
		child_index = np.zeros(N)  # 记录孩子节点数，作为编号用于计算坐标
		while len(q) != 0:
			node = q.pop(0)
			for n in nb[node]:
				if n != root and len(coordinate[l][n]) == 0:  # 该邻居节点非根节点，且未分配坐标
					if (G[node][n]["balance"] > 0 and G[n][node]["balance"] > 0) or (not bi_flag):  # 若拥有双向通道，或为第二轮遍历
						parent[l][n] = node
						child_index[node] += 1
						current_index = child_index[node]
						coordinate[l][n] = coordinate[l][node] + [current_index]  # 计算坐标
						q.append(n)
			if len(q) == 0 and bi_flag:  # 双向通道处理完毕，准备第二轮遍历
				bi_flag = False
				for n in range(N):
					if len(coordinate[l][n]) > 0:
						q.append(n)
	return coordinate, parent


# react to channel state changes
def setCred(nb, landmarks, parent, coordinate, u, v, c, old, G):
	L = len(landmarks)
	N = len(G)

	reset_nodes = {}
	for l in range(L):
		reset_nodes[l] = []

	for l in range(L):
		reset = -1  # node whose coordinate should change
		# case: add link
		if old == 0 and c > 0:
			if len(coordinate[l][v]) == 0 and len(coordinate[l][u]) > 0:
				reset = v
			if len(coordinate[l][u]) == 0 and len(coordinate[l][v]) > 0:
				reset = u
			if reset == -1:
				if G[u][v]["balance"] > 0 and G[v][u]["balance"] > 0:
					a1 = (G[u][parent[l][u]]["balance"] == 0) or (G[parent[l][u]][u]["balance"] == 0)
					a2 = (G[v][parent[l][v]]["balance"] == 0) or (G[parent[l][v]][v]["balance"] == 0)
					if a1 and (not a2):
						reset = v
					if a2 and (not a1):
						reset = u
		# case: remove link
		if old > 0 and c == 0:
			if parent[l][u] == v:
				reset = u
			if parent[l][v] == u:
				reset = v

		# change coordinate
		if reset != -1:
			# 清空所有需要重新计算坐标的节点的坐标
			cc = coordinate[l][reset]
			coordinate[l][reset] = []
			reset_nodes[l].append(reset)
			# 清空上述节点后代的坐标
			for n in range(N):
				if len(coordinate[l][n]) > len(cc) and coordinate[l][n][0:len(cc)] == cc:  # all descendants
					coordinate[l][n] = []
					reset_nodes[l].append(n)

			# 为相关节点重新计算坐标
			coordinate, parent = setRoute(G, landmarks, nb, coordinate, parent)
	return coordinate, parent


# 随机分割交易金额
def random_split(payment_size, L):
	# 在 (0, payment_size) 之间生成 L-1 个随机点，然后排序
	split_points = sorted(np.random.uniform(0, payment_size, L - 1))
	# 在 0 和 payment_size 两端添加边界值，然后计算每段的长度
	split_points = [0] + split_points + [payment_size]
	# 计算相邻分割点之间的差值，得到每份的金额
	c = np.diff(split_points)
	return c


# 计算距离
def dist(c1, c2):
	common_prefix_length = 0
	shorter_length = np.minimum(len(c1), len(c2))
	for i in range(shorter_length):
		if c1[i] == c2[i]:
			common_prefix_length += 1
		else:
			break
	return len(c1) + len(c2) - 2 * common_prefix_length


def routePay(G, nb, landmarks, coordinate, parent, src, dst, payment_size):
	probing_messages = 0
	commit_messages = 0
	L = len(landmarks)
	N = len(G)

	# randomly split payment into L shares
	c = random_split(payment_size, L)

	path = {}
	fail = False
	modified_edges = {}  # 记录修改前的余额

	# 寻找L条路径（TODO: 该部分时间开销最大）
	for l in range(L):
		path[l] = []
		v = src  # current node
		while (not fail) and (v != dst):
			# 初始化临时参数
			next_hop = -1
			min_dist = N * N

			# 寻找下一节点
			for n in nb[v]:
				c1 = coordinate[l][v]
				c2 = coordinate[l][n]
				c3 = coordinate[l][dst]
				if dist(c2, c3) < dist(c1, c3) and G[v][n]["balance"] >= c[l]:
					if dist(c2, c3) < min_dist:
						min_dist = dist(c2, c3)
						next_hop = n

			# 若找到合适的下一节点，则更新，否则路由失败
			if next_hop != -1:
				path[l].append((v, next_hop))
				# 记录修改前的通道余额
				if (v, next_hop) not in modified_edges:
					modified_edges[(v, next_hop)] = G[v][next_hop]["balance"]
				if (next_hop, v) not in modified_edges:
					modified_edges[(next_hop, v)] = G[next_hop][v]["balance"]

				G[v][next_hop]["balance"] -= c[l]
				G[next_hop][v]["balance"] += c[l]
				v = next_hop
			else:
				fail = True

	# 如果路由失败，则回滚资金，否则更新坐标系统
	if fail:
		for l in range(L):
			probing_messages += len(path[l])
			for e in path[l]:
				G[e[0]][e[1]]["balance"] += c[l]
				G[e[1]][e[0]]["balance"] -= c[l]
		return G, 0, coordinate, parent, probing_messages, 0
	else:
		# 按需更新生成树及坐标
		for l in range(L):
			probing_messages += len(path[l])-1
			commit_messages += len(path[l])-1
			for e in path[l]:
				u = e[0]
				v = e[1]
				c1 = G[u][v]["balance"]
				c2 = G[v][u]["balance"]
				old1 = modified_edges.get((u, v), G[u][v]["balance"])
				old2 = modified_edges.get((v, u), G[v][u]["balance"])
				# 更新坐标
				coordinate, parent = setCred(nb, landmarks, parent, coordinate, u, v, c1, old1, G)
				coordinate, parent = setCred(nb, landmarks, parent, coordinate, v, u, c2, old2, G)
		return G, payment_size, coordinate, parent, probing_messages, commit_messages


def routing(G, payments, L):
	# select landmarks (nodes with high degrees)
	landmarks = []
	sorted_nodes = sorted(G.degree, key=lambda x: x[1], reverse=True)
	for l in range(L):
		landmarks.append(sorted_nodes[l][0])

	# get node number and all edges
	N = len(G)
	edges = G.edges()

	# 创建邻接表
	nb = {}
	for i in range(N):
		nb[i] = []
	for e in edges:
		nb[e[0]].append(e[1])

	# 初始化坐标系统
	coordinate = {}
	parent = {}
	for l in range(L):
		coordinate[l] = []
		parent[l] = []
		for i in range(N):
			coordinate[l].append([])
			parent[l].append([])
	# 分配坐标
	coordinate, parent = setRoute(G, landmarks, nb, coordinate, parent)

	# 统计信息
	throughput = 0
	num_delivered = 0
	total_probing_messages = 0
	total_commit_messages = 0

	# 迭代支付
	for payment in payments:
		src = payment[0]
		dst = payment[1]
		payment_size = payment[2]

		# 路由
		G, delivered, coordinate, parent, probing_messages, commit_messages = routePay(G, nb, landmarks, coordinate, parent, src, dst, payment_size)

		# 统计
		total_probing_messages += probing_messages
		total_commit_messages += commit_messages
		if delivered >= payment_size:
			num_delivered += 1
		throughput += delivered

	return throughput, num_delivered, total_probing_messages, total_commit_messages
