import networkx as nx
import csv
import random
from collections import deque


def assign_edge_to_partition(G, src, dst, partitions, balance_lambda, payment_lambda, payment_frequency):

	max_load = max(partition.number_of_nodes() for partition in partitions)
	min_load = min(partition.number_of_nodes() for partition in partitions)

	best_score = float("-inf")
	candidates = []

	# 预计算支付频率分值
	payment_frequency_for_partitions = [0] * len(partitions)
	payment_frequency_src = payment_frequency.get(src, {})
	payment_frequency_dst = payment_frequency.get(dst, {})
	for i, partition in enumerate(partitions):
		for node, value in payment_frequency_src.items():	
			if node in partition.nodes():
				payment_frequency_for_partitions[i] += value

		for node, value in payment_frequency_dst.items():	
			if node in partition.nodes():
				payment_frequency_for_partitions[i] += value

	max_frequency = max(payment_frequency_for_partitions)
	min_frequency = min(payment_frequency_for_partitions)

	# 计算分值
	for i, partition in enumerate(partitions):
		degree_src = G.degree[src]
		degree_dst = G.degree[dst]
		theta_src = degree_src / (degree_src + degree_dst)
		theta_dst = degree_dst / (degree_src + degree_dst)

		g_src = 2.0 - theta_src if src in partition else 0
		g_dst = 2.0 - theta_dst if dst in partition else 0

		replication_score = g_src + g_dst
		balance_score = balance_lambda * (max_load - partition.number_of_nodes()) / (max_load - min_load + 1e-9)
		payment_score = payment_lambda * (payment_frequency_for_partitions[i] - min_frequency) / (max_frequency - min_frequency + 1e-9)

		total_score = replication_score + balance_score + payment_score

		if total_score > best_score:
			best_score = total_score
			candidates = [i]
		elif total_score == best_score:
			candidates.append(i)
	
	chosen_partition = random.choice(candidates)
	
	partitions[chosen_partition].add_edge(src, dst, balance = G[src][dst]["balance"])

	return chosen_partition


def bfs_partitioning(G, num_partitions, balance_lambda, payment_lambda, payment_frequency):
	# 初始化分区
	partitions = [nx.Graph() for _ in range(num_partitions)]

	# 选择度值最高的节点作为起始节点
	degrees = G.degree()
	max_degree_node = max(degrees, key=lambda x: x[1])
	start_node = max_degree_node[0]

	"""BFS遍历无向图中所有边"""
	visited_nodes = set()  # 已访问节点集合
	visited_edges = set()  # 已访问边集合
	queue = deque([start_node])  # 初始化队列，起点入队
	visited_nodes.add(start_node)
	while queue:
		current_node = queue.popleft()  # 从队列中取出当前节点

		# 遍历当前节点的所有邻居节点
		for neighbor in G.neighbors(current_node):
			# 构造无向边 (u, v)，确保顺序一致
			edge = tuple(sorted((current_node, neighbor)))

			if edge not in visited_edges:
				visited_edges.add(edge)  # 标记边为已访问
				chosen_partition = assign_edge_to_partition(G, edge[0], edge[1], partitions, balance_lambda, payment_lambda, payment_frequency)

				# 如果邻居节点未访问，则加入队列
				if neighbor not in visited_nodes:
					visited_nodes.add(neighbor)
					queue.append(neighbor)

	return partitions


def find_shared_nodes_distribution(partitions, node_partition_map):
	# 统计割点
	shared_nodes = {node: partition_ids for node, partition_ids in node_partition_map.items() if len(partition_ids) > 1}

	# 统计每种复制因子的节点数
	distribution = {}
	for partition_ids in node_partition_map.values():
		partition_count = len(partition_ids)
		distribution[partition_count] = distribution.get(partition_count, 0) + 1

	return shared_nodes, distribution


def intra_transaction_ratio(trans, partitions, node_partition_map):
	# 统计分区内交易数量
	intra_partition_count = 0
	for src, dst, _ in trans:
		src_partitions = node_partition_map.get(src, set())
		dst_partitions = node_partition_map.get(dst, set())
		if src_partitions & dst_partitions:  # 如果发送方和接收方有共同的分区
			intra_partition_count += 1

	total_transactions = len(trans)

	return intra_partition_count / total_transactions


def network_partitioning(G_ori, trans, payment_frequency, config):
	# 有向图——>无向图
	G = G_ori.to_undirected()
	
	"""
	ag_degree = sum(dict(G.degree()).values()) / G.number_of_nodes()
	print(f"Ag degree: {ag_degree}.\n")
	"""
	
	# 导入参数
	num_partitions =  int(config['n'])
	balance_lambda = config['balance_lambda']
	payment_lambda = config['payment_lambda']

	#print("Partition parameter:")
	#print(f"n = {num_partitions}")
	#print(f"balance_lambda = {balance_lambda}")
	#print(f"payment_lambda = {payment_lambda}\n")
	
	# 网络划分
	partitions = bfs_partitioning(G, num_partitions, balance_lambda, payment_lambda, payment_frequency)

	# 建立节点到分区的映射
	node_partition_map = {}
	for i, partition in enumerate(partitions):
		for node in partition.nodes:
			if node not in node_partition_map:
				node_partition_map[node] = set()
			node_partition_map[node].add(i)


	# 输出划分结果
	print("\nPartition result:")
	for i, partition in enumerate(partitions):
		if len(partition.nodes) != 0:
			is_connected = nx.is_connected(partition)
		else:
			is_connected = 'Null'
		print(f"Partition {i}: {partition.number_of_edges()} edges, {partition.number_of_nodes()} nodes. [Connectivity: {is_connected}]")
		"""
		# 输出所有联通子图
		components = nx.connected_components(partition)
		for component in components:
			print(f"{len(component)};") """

	#total_vertices = sum(len(partition.nodes) for partition in partitions)
	#print(f"\nTotal nodes: {total_vertices}.")

	# 输出割点及分布
	shared_nodes, distribution = find_shared_nodes_distribution(partitions, node_partition_map)
	print(f"Total shared nodes: {len(shared_nodes)}.")
	"""
	print("Shared nodes distribution:")
	for partition_count, node_count in sorted(distribution.items()):
		print(f"Nodes in {partition_count} partitions: {node_count}")
	"""

	# 输出分区内交易占比
	ratio = intra_transaction_ratio(trans, partitions, node_partition_map)
	print(f"Intra-partition transaction ratio: {ratio:.2%}.")

	# 导出分区
	for i, partition in enumerate(partitions):
		file_name = f"partition_results/partition_{i}.csv"
		with open(file_name, mode='w', newline='') as file:
			writer = csv.writer(file)
			writer.writerow(['Node1', 'Node2', "Cap"])  # 写入表头
			for node1, node2, edge_data in partition.edges(data=True):
				balance = edge_data.get("balance")
				writer.writerow([node1, node2, balance * 2])

	# 导出割点
	sorted_shared_nodes = sorted(shared_nodes.items(), key=lambda x: len(x[1]), reverse=True)
	with open('partition_results/shared_nodes.csv', mode='w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(['Node ID', 'Degree', 'Number of Partitions'])  # 写入表头
		for node, partition_ids in sorted_shared_nodes:
			degree = G.degree[node]  # 获取节点的度
			num_partitions = len(partition_ids)
			writer.writerow([node, degree, num_partitions])


	return partitions, node_partition_map, ratio, len(shared_nodes)
