import numpy as np
import networkx as nx
import operator
import sys
from scipy.spatial import Delaunay
from sklearn.manifold import MDS
import random


def build_spanning_tree(G, root):

	tree = nx.Graph() 
	visited = set()
	queue = [root]

	while queue:
		node = queue.pop(0)  # 取出队列头部的节点
		visited.add(node)

		for neighbor in G.neighbors(node):
			if neighbor not in visited:
				tree.add_edge(node, neighbor) 
				queue.append(neighbor)

	return tree

"""
def build_spanning_tree(G, root, visited=None, tree=None):

	if visited is None:
		visited = set()
	if tree is None:
		tree = nx.Graph()  # 初始化生成树

	visited.add(root)  # 标记 root 已访问

	for neighbor in G.neighbors(root):  # 遍历所有邻居
		if neighbor not in visited:  # 避免回路
			tree.add_edge(root, neighbor)  # 加入生成树
			build_spanning_tree(G, neighbor, visited, tree)  # 递归处理邻居节点

	return tree
"""


def compute_anchors_coordinates(G, anchors):
	# 计算锚点之间的最短跳数矩阵
	k = len(anchors)
	d = k-1
	dist_matrix = np.zeros((k, k))

	for i in range(k):
		for j in range(k):
			if i != j:
				dist_matrix[i][j] = nx.shortest_path_length(G, source=anchors[i], target=anchors[j])

	# 使用 MDS 计算锚点的 d 维欧几里得坐标
	mds = MDS(n_components=d, dissimilarity='precomputed')
	anchor_coords = mds.fit_transform(dist_matrix)
	return anchor_coords


# 计算所有节点的坐标
def compute_node_coordinates(G, trees, anchors, anchor_coords):
	node_coords = {}
	for node in G.nodes:
		if node in anchors:
			node_coords[node] = anchor_coords[list(anchors).index(node)]
		else:
			# 计算普通节点到所有锚点的跳数
			distances = np.array([nx.shortest_path_length(trees[anchor], source=node, target=anchor) for anchor in anchors])
			# 用最小二乘法拟合坐标
			A = np.vstack([anchor_coords.T, np.ones(len(anchors))]).T
			x, _, _, _ = np.linalg.lstsq(A, distances, rcond=None)
			node_coords[node] = x[:-1]  # 只取坐标部分（去掉偏置项）

	return node_coords


# 计算 MDS 坐标
def compute_mds_coordinates(G, d):
	# 随机选择 d+1 个锚点
	nodes = list(G.nodes)
	anchors = np.random.choice(nodes, d+1, replace=False)
	#print(f"Selected anchors: {anchors}.\n")
	
	# 生成树
	trees = {}
	for anchor in anchors:
		print(f"Build spanning tree. Root: {anchor}")
		trees[anchor] = build_spanning_tree(G, anchor)

	# 计算锚点坐标
	#print("/***Compute anchors coordinates***/")
	anchor_coords = compute_anchors_coordinates(G, anchors)
	#print(f"Anchor_coords: {anchor_coords}.\n")
	
	# 计算节点坐标
	#print("/***Compute nodes coordinates***/")
	node_coords = compute_node_coordinates(G, trees, anchors, anchor_coords)
	#print(f"Node_coords: {node_coords}.\n")

	return node_coords


# 计算 DT 邻居
def compute_delaunay_neighbors(node_coords):
	nodes = list(node_coords.keys())
	coords = np.array([node_coords[node] for node in nodes])
	tri = Delaunay(coords)

	DT_neighbors = {node: set() for node in nodes}
	for simplex in tri.simplices:
		for i in range(len(simplex)):
			for j in range(i+1, len(simplex)):
				node_i, node_j = nodes[simplex[i]], nodes[simplex[j]]
				DT_neighbors[node_i].add(node_j)
				DT_neighbors[node_j].add(node_i)

	return DT_neighbors


# 构建 MDT 结构
def build_mdt_structure(G, DT_neighbors):
	MDT = {}
	for node in G.nodes:
		real_neighbors = set(G.neighbors(node))
		MDT[node] = {
			'DT_neighbors': DT_neighbors[node],
			'virtual_links': {}
		}

		for dt_neigh in DT_neighbors[node]:
			if dt_neigh not in real_neighbors:
				path = nx.shortest_path(G, source=node, target=dt_neigh)
				MDT[node]['virtual_links'][dt_neigh] = path

	return MDT


# WebFlow 转发协议
def webflow_forwarding(MDT, node_coords, G, source, target, demand):
	current = source
	path = [current]
	
	probing_messages = 0

	while current != target:
		direct_neighbors = set(G.neighbors(current))
		DT_neighbors = MDT[current]['DT_neighbors']

		# Case 1: 直接邻居
		direct_candidates = [
			(v, G[current][v]['balance'])
			for v in direct_neighbors
			if np.linalg.norm(node_coords[v] - node_coords[target]) < np.linalg.norm(node_coords[current] - node_coords[target])
		]
		
		print(f"direct_candidates_1: {direct_candidates}")
		
		direct_candidates = [v for v, bal in direct_candidates if bal >= demand]
		
		print(f"direct_candidates_2: {direct_candidates}")

		if direct_candidates:
			next_hop = min(direct_candidates, key=lambda v: np.linalg.norm(node_coords[v] - node_coords[target]))
			path.append(next_hop)
			current = next_hop
			probing_messages += 1 # probe消息 + 1
			continue

		# Case 2: DT 邻居
		dt_candidates = [
			(v, MDT[current]['virtual_links'][v])
			for v in DT_neighbors
			if v in MDT[current]['virtual_links'] and np.linalg.norm(node_coords[v] - node_coords[target]) < np.linalg.norm(node_coords[current] - node_coords[target])
		]

		for v, virtual_path in dt_candidates:
			balances = [G[u][w]['balance'] for u, w in zip(virtual_path, virtual_path[1:])]
			probing_messages += len(virtual_path)-1 # probe消息 + path_len
			
			if min(balances) >= demand:
				path.extend(virtual_path[1:])
				current = v
				break
		else:
			# print(f"Routing failed at node {current}.")
			return None, probing_messages

	return path, probing_messages

# WebFlow 路由主函数
def routing(G, payments, d):

	throughput = 0
	num_delivered = 0
	total_probing_messages = 0
	total_commit_messages = 0

	# 计算坐标
	G_undi = G.to_undirected()
	node_coords = compute_mds_coordinates(G_undi, d)

	# 构建MDT
	# print("/***Build MDT***/")
	DT_neighbors = compute_delaunay_neighbors(node_coords)
	MDT = build_mdt_structure(G, DT_neighbors)

	print("/***Start routing***/")
	# 路由
	for payment in payments:
		
		print(f"Payments: {payment}")
		
		src, dst, payment_size = payment
		
		payment_size = payment_size/10

		path, probing_messages = webflow_forwarding(MDT, node_coords, G, src, dst, payment_size)
		
		print(f"Path: {path}, Probe: {probing_messages}\n")
		
		
		total_probing_messages += probing_messages
		
		# success
		if path:
			for i in range(len(path) - 1):
				G[path[i]][path[i + 1]]["balance"] -= payment_size
				G[path[i + 1]][path[i]]["balance"] += payment_size
			num_delivered += 1
			throughput += payment_size
			total_commit_messages += len(path)-1

	return throughput, num_delivered, total_probing_messages, total_commit_messages


"""
# 测试 WebFlow

# 生成支付通道网络
def generate_graph():
	G = nx.DiGraph()
	edges = [
		('A', 'B', 15), ('B', 'C', 10), ('C', 'D', 15),
		('A', 'E', 8), ('B', 'F', 12), ('C', 'G', 6), ('D', 'H', 10),
		('E', 'F', 9), ('F', 'G', 14), ('G', 'H', 7),
		('E', 'I', 11), ('F', 'J', 4), ('G', 'K', 10), ('H', 'L', 13),
		('I', 'J', 5), ('J', 'K', 8), ('K', 'L', 9),
		
		('B', 'A', 15), ('C', 'B', 10), ('D', 'C', 15),
		('E', 'A', 8), ('F', 'B', 12), ('G', 'C', 6), ('H', 'D', 10),
		('F', 'E', 9), ('G', 'F', 14), ('H', 'G', 7),
		('I', 'E', 11), ('J', 'F', 4), ('K', 'G', 10), ('L', 'H', 13),
		('J', 'I', 5), ('K', 'J', 8), ('L', 'K', 9)
	]
	for u, v, balance in edges:
		G.add_edge(u, v, balance=balance)
	return G


G = generate_graph()
payments = [('A', 'L', 10), ('B', 'K', 8), ('C', 'H', 5)]
d=3
results = routing(G, payments, d)

for e in G.edges:
	bal=G[e[0]][e[1]]["balance"]
	print(f"{e[0]}, {e[1]}, {bal}")

print(f"Throughput: {results[0]}, Successful Payments: {results[1]}, Probing Messages: {results[2]}, Commit Messages: {results[3]}")
"""
