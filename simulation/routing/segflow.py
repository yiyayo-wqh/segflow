import networkx as nx
import csv
import random
from itertools import islice
import collections


def bfs_path(G, src, dst):
	visited = []
	queue = collections.deque([(src, [src])])
	while queue:
		(vertex, path) = queue.popleft()
		for next in set(list(G.neighbors(vertex))) - set(visited):
			if G[vertex][next]["balance"] > 0:
				visited.append(next)
				if next == dst:
					return path + [next]
				else:
					queue.append((next, path + [next]))
	return []


def max_flow_of_kpaths(G_kpaths, src, dst):
	local_G = G_kpaths.copy()

	path_set = []
	cap_set = []
	probing_messages = 0

	# 寻找最大流
	while True:
		# 查找路径，没有找到路径则停止
		path = bfs_path(local_G, src, dst)
		if not path:
			break

		# 记录路径和路径容量信息
		path_set.append(path)
		path_cap = min(local_G[path[i]][path[i + 1]]["balance"] for i in range(len(path) - 1))
		cap_set.append(path_cap)

		# 更新探测消息数
		probing_messages += len(path) - 1

		# 更新local_G
		for i in range(len(path) - 1):
			u, v = path[i], path[i + 1]
			local_G[u][v]["balance"] -= path_cap
			if local_G.has_edge(v, u):
				local_G[v][u]["balance"] += path_cap

	return path_set, cap_set, probing_messages


def extract_edge_disjoint_paths(paths):
	edge_disjoint_paths = []

	# 存储已选路径的边
	used_edges = set()

	for path in paths:
		# 将路径的边转换为一组 (u, v) 元组
		path_edges = set((path[i], path[i + 1]) for i in range(len(path) - 1))

		# 检查路径的边是否与已选路径的边相交
		if path_edges.isdisjoint(used_edges):
			# 如果不相交，将路径的边加入已选路径集合
			edge_disjoint_paths.append(path)
			used_edges.update(path_edges)

	return edge_disjoint_paths


def extract_k_shortest_paths(paths, k):

	unique_paths = []
	for path in paths: 
		if path not in unique_paths:
			unique_paths.append(path)

	sorted_paths = sorted(unique_paths, key=len)

	return sorted_paths[:k]


def connect_paths(paths_a, paths_b):
	connected_paths = []

	# 遍历集合 A 中的路径
	for path_a in paths_a:
		end_node_a = path_a[-1]  # 获取路径 A 的最后一个节点

		# 遍历集合 B 中的路径
		for path_b in paths_b:
			start_node_b = path_b[0]  # 获取路径 B 的第一个节点

			# 如果路径 A 的最后一个节点与路径 B 的第一个节点相同
			if end_node_a == start_node_b:
				# 将路径 A 和路径 B 连接
				connected_path = path_a[:-1] + path_b
				connected_paths.append(connected_path)

	return connected_paths


def compress_path(path, subnet_ids):
	# 初始化压缩后的路径和keys
	compressed_path = [path[0]]
	compressed_subnet_ids = []

	# 遍历路径的相邻节点
	i = 0
	while i < len(path) - 1:
		u, v = path[i], path[i + 1]
		cur_subnet_id = subnet_ids[i]

		# 检查下一个相邻边
		j = i + 1
		while j < len(path) - 1:
			u_next, v_next = path[j], path[j + 1]
			next_subnet_id = subnet_ids[j]

			# 如果当前边和下一条边权重相同，则跳过这条边，继续看下一个节点
			if cur_subnet_id == next_subnet_id:
				j += 1
			else:
				break

		# 将压缩后的路径加入
		compressed_path.append(path[j])
		compressed_subnet_ids.append(cur_subnet_id)

		# 设置i的值，继续下一轮判断
		i = j

	return compressed_path, compressed_subnet_ids


def get_path_attributes(index_path, keys, index_topo):
	path_length = 0
	subnet_ids = []
	for i in range(len(index_path) - 1):
		edge_data = index_topo.get_edge_data(index_path[i], index_path[i+1])
		
		path_length += edge_data[keys[i]]['length']
		
		subnet_ids.append(edge_data[keys[i]]['subnet'])

	return path_length, subnet_ids


def dijkstra_for_multigraph(G, source, target, weight):
	# Get the shortest path
	try:
		path = nx.shortest_path(G, source, target, weight=weight)
	except nx.NetworkXNoPath:
		#print(f"No path between {source} and {target} in current graph.")
		return [], []

	# Get the key of each edge in the path
	path_keys = []
	for i in range(len(path) - 1):
		u, v = path[i], path[i + 1]
		edge_data = G.get_edge_data(u, v)

		weights = [data[weight] for data in edge_data.values()]
		min_weight = min(weights) # 获取最小权重
		keys_with_min_weight = [key for key, data in edge_data.items() if data[weight] == min_weight] # 获取拥有最小权重的边的key
		random_key = random.choice(keys_with_min_weight)
		# print(f"[{u}, {v}]  min_weight: {min_weight}, keys_with_min_weight: {keys_with_min_weight}, random_key: {random_key}.")

		path_keys.append(random_key)

	return path, path_keys


def yen_k_shortest_paths_for_multigraph(G, source, target, k, weight):

	k_shortest_paths = []
	potential_paths = []

	# Compute the first shortest path
	first_path, first_path_keys = dijkstra_for_multigraph(G, source, target, weight)
	if not first_path:
		return []
	k_shortest_paths.append((first_path, first_path_keys))


	for i in range(1,k):
		# 获取第i条路径
		cur_shortest_path = k_shortest_paths[i-1][0]
		cur_shortest_path_keys = k_shortest_paths[i-1][1]

		# 遍历第i条路径的边
		for j in range(len(cur_shortest_path) - 1):
			spur_node = cur_shortest_path[j]
			root_path = cur_shortest_path[:j + 1]
			root_path_keys = cur_shortest_path_keys[:j]

			# 复制原图
			tmp_G = G.copy() 
			# 移除边
			for path, keys in k_shortest_paths:
				if path[:j + 1] == root_path and keys[:j] == root_path_keys:
					if tmp_G.has_edge(path[j], path[j + 1], keys[j]):
						tmp_G.remove_edge(path[j], path[j + 1], key = keys[j])
			# 移除节点
			for node in root_path[:-1]:
				tmp_G.remove_node(node)

			# 计算从spurnode到target的最短路径
			spur_path, spur_path_keys = dijkstra_for_multigraph(tmp_G, spur_node, target, weight)

			# 拼接路径 root_path + spur_path
			if spur_path:
				total_path = root_path[:-1] + spur_path
				total_path_keys = root_path_keys + spur_path_keys

				if not any(p[0] == total_path and p[1] == total_path_keys for p in potential_paths):
					potential_paths.append((total_path, total_path_keys))

		# Choose the best potential path (the shortest one)
		if potential_paths:
			best_path = []
			best_path_keys = []
			min_weight = float("inf")
			
			for path, keys in potential_paths:
				cur_weight = 0
				for i in range(len(path)-1):
					edge_data = G.get_edge_data(path[i], path[i+1])
					cur_weight += edge_data[keys[i]][weight]
				
				if cur_weight < min_weight: 
					min_weight = cur_weight
					best_path = path
					best_path_keys = keys

			k_shortest_paths.append((best_path,best_path_keys))
			potential_paths.remove((best_path, best_path_keys)) # 移除选中的路径

		else:
			break

	return k_shortest_paths


def get_k_shortest_paths(src, dst, extended_index_topo, subGraphs, k):
	iterations = 0
	request_messages = 0

	# 计算k最短索引路径
	k_shortest_index_paths = yen_k_shortest_paths_for_multigraph(extended_index_topo, src, dst, k, weight="length")

	# 遍历k最短索引路径
	temp_paths_1 = []
	for index_path, keys in k_shortest_index_paths:

		path_length, subnet_ids = get_path_attributes(index_path, keys, extended_index_topo)
		
		# 若列表里的路径数 = k，且下一索引路径的长度 >= 列表里任意路径的长度，则终止循环
		if len(temp_paths_1) == k: 
			if path_length >= len(temp_paths_1[-1]) - 1: 
				break
		
		# 统计迭代次数
		iterations += 1

		# 压缩索引路径（压缩子路由请求）
		compressed_index_path, compressed_subnet_ids = compress_path(index_path, subnet_ids)
		
		request_messages += len(compressed_index_path) - 1

		# 遍历压缩后的索引路径
		temp_paths_2 = []
		for i in range(len(compressed_index_path) - 1):
			u = compressed_index_path[i]
			v = compressed_index_path[i + 1]


			k_partial_paths = list(islice(nx.shortest_simple_paths(subGraphs[compressed_subnet_ids[i]], u, v), k))

			if not temp_paths_2:
				temp_paths_2 = k_partial_paths
			else:
				temp_paths_2 = connect_paths(temp_paths_2, k_partial_paths)

			temp_paths_2 = extract_k_shortest_paths(temp_paths_2, k)

		temp_paths_1.extend(temp_paths_2)
		temp_paths_1 = extract_k_shortest_paths(temp_paths_1, k)

	
	return temp_paths_1, request_messages


def inter_subnet_routing(src, dst, extended_index_topo, subGraphs, payment_size, k, G):

	# 寻路
	k_shortest_paths, request_messages = get_k_shortest_paths(src, dst, extended_index_topo, subGraphs, k)
	
	# 创建一个新图，只包含这些k条路径上的边
	G_kpaths = nx.DiGraph()
	for path in k_shortest_paths:
		for i in range(len(path) - 1):
			u, v = path[i], path[i + 1]
			if not G_kpaths.has_edge(u, v):
				G_kpaths.add_edge(u, v, balance=G[u][v]["balance"])

	
	# 使用最大流算法来获取结果
	path_set, cap_set, probing_messages = max_flow_of_kpaths(G_kpaths, src, dst)

	if sum(cap_set) < payment_size:
		path_set = []
		cap_set = []
		return path_set, cap_set, probing_messages, request_messages
	
	return path_set, cap_set, probing_messages, request_messages


def extend_index_topo(index_topo, src, dst, node_subnet_map, subGraphs):
	# 创建扩展索引图
	extended_index_topo = index_topo.copy()

	subnets_src = node_subnet_map.get(src)
	subnets_dst = node_subnet_map.get(dst)

	# 添加 src -> boundary nodes 的边
	if len(subnets_src)==1:
		subnet_id = next(iter(subnets_src))
		boundary_nodes = set(index_topo.nodes()) & set(subGraphs[subnet_id].nodes())
		#print(f"src_subnet_id: {subnet_id}, number of boundary_nodes: {len(boundary_nodes)}.")
		for u in boundary_nodes:
			try:
				path = nx.shortest_path(subGraphs[subnet_id], src, u)
				extended_index_topo.add_edge(src, u, length=len(path)-1, subnet = subnet_id)
			except nx.NetworkXNoPath:
				continue

	# 添加 boundary nodes -> dst 的边
	if len(subnets_dst)==1:
		subnet_id = next(iter(subnets_dst))
		boundary_nodes = set(index_topo.nodes()) & set(subGraphs[subnet_id].nodes())
		#print(f"dst_subnet_id: {subnet_id}, number of boundary_nodes: {len(boundary_nodes)}.")
		for u in boundary_nodes:
			try:
				path = nx.shortest_path(subGraphs[subnet_id], u, dst)
				extended_index_topo.add_edge(u, dst, length=len(path)-1, subnet = subnet_id)
			except nx.NetworkXNoPath:
				continue

	return extended_index_topo


def intra_subnet_routing_lnd(src, dst, subgraph, a, G):
	
	commit_messages = 0
	valid_edges = set()

	for _ in range(3):
		try:
			path = nx.shortest_path(subgraph, src, dst)

			invalid_edge = None
			for i in range(len(path) - 1):
				commit_messages += 1  # 模拟commit
				if G[path[i]][path[i + 1]].get("balance", 0) < a:
					invalid_edge = (path[i], path[i + 1])
					bal = G[path[i]][path[i + 1]].get("balance", 0)
					#print(f"{invalid_edge}: {bal} < {a}!!!")
					break

			if not invalid_edge:
				# 如果没有无效边，路径可行，返回路径
				return path, commit_messages

			# 删除无效边
			subgraph.remove_edge(*invalid_edge)


		except nx.NetworkXNoPath:
			# 如果没有路径可用，返回 None
			return [], commit_messages

	return [], commit_messages


def merge_subgraphs(subGraphs, inter_set):
	merged_graph = nx.DiGraph()

	for partition_id in inter_set:
		merged_graph = nx.compose(merged_graph, subGraphs[partition_id])

	return merged_graph


def routing(subGraphs, index_topo, node_subnet_map, cur_payments, G):
	
	# 跨子网路由路径数
	k = 4

	# 统计去重后的index_topo边数量
	unique_edges = set()
	for u, v in index_topo.edges():
		unique_edges.add((u, v))
	print(f"Number of unique indexedges: {len(unique_edges)}")
	
	# 统计信息
	subnet_delivered = 0 # 子网内成功次数
	non_subnet_delivered = 0 # 子网间成功次数
	
	subnet_throughput = 0 # 子网内成功金额 
	non_subnet_throughput = 0 # 子网间成功金额 

	total_probing_messages = 0
	total_commit_messages = 0
	total_request_messages = 0

	# 统计子网内/子网间支付数量
	subpayment_count = 0
	crosspayment_count = 0
	for payment in cur_payments:
		partitions_src = node_subnet_map.get(payment[0])
		partitions_dst = node_subnet_map.get(payment[1])
		if partitions_src.intersection(partitions_dst):
			subpayment_count += 1
		else: 
			crosspayment_count += 1
	print('Intra-subnet payment count:', subpayment_count, 'Inter-subnet payment count:', crosspayment_count)

	# 迭代支付
	for payment in cur_payments:

		src = payment[0]
		dst = payment[1]
		payment_size = payment[2]

		path = []
		path_set = []

		# 判断是否为子网内路由
		partitions_src = node_subnet_map.get(src)
		partitions_dst = node_subnet_map.get(dst)
		inter_set = partitions_src.intersection(partitions_dst)
		
		#print(f"\ncurrent payment: {payment}; partitions_src: {partitions_src}; partitions_dst: {partitions_dst}.")

		# 子网内路由
		if inter_set:
			merged_graph = merge_subgraphs(subGraphs, inter_set) # 若同时属于多个子图，合并子图
			path, commit_messages = intra_subnet_routing_lnd(src, dst, merged_graph, payment_size, G) # 路由
			total_commit_messages += commit_messages
			total_commit_messages = total_commit_messages - (len(path) - 1)  # LND已经将该部分算在内了，因此先删除，在后边更新余额时再加上
			

		# 若子网内路由成功
		if path:
			# 进行单路径支付
			for i in range(len(path) - 1):
				u, v = path[i], path[i + 1]
				G[u][v]["balance"] -= payment_size
				G[v][u]["balance"] += payment_size

			# 统计信息
			subnet_delivered += 1
			subnet_throughput += payment_size
			total_commit_messages += len(path) - 1

		else: # 否则，进行子网间路由
			extended_index_topo = extend_index_topo(index_topo, src, dst, node_subnet_map, subGraphs) # 扩展索引拓扑
			path_set, cap_set, probing_messages, request_messages = inter_subnet_routing(src, dst, extended_index_topo, subGraphs, payment_size, k, G) # 路由
			total_probing_messages += probing_messages
			total_request_messages += request_messages

		# 若子网间路由成功
		if path_set:
			# 进行多路径支付
			remaining_amount = payment_size
			for path, path_cap in zip(path_set, cap_set):
				# 计算当前路径上能够发送的流量
				flow = min(path_cap, remaining_amount)
				remaining_amount -= flow

				# 更新每条边的容量
				for i in range(len(path) - 1):
					u, v = path[i], path[i + 1]
					G[u][v]["balance"] -= flow
					G[v][u]["balance"] += flow

				total_commit_messages += len(path) - 1

				# 如果支付需求已满足，停止分配
				if remaining_amount <= 1e-6:
					break

			# 统计信息
			non_subnet_delivered += 1
			non_subnet_throughput += payment_size

	return (subnet_throughput + non_subnet_throughput), (subnet_delivered + non_subnet_delivered), total_probing_messages, total_commit_messages, subnet_throughput, subnet_delivered

