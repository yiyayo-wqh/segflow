import networkx as nx
import numpy as np
import csv
import random
from scipy import stats


# returns network topology and transactions for Lightning
def lightning_setup():
	file_path = "data/lightning/1.2_original_topology.csv"

	# load network topology
	G = nx.DiGraph()
	with open(file_path, 'r') as f:
		csv_reader = csv.reader(f)
		# 跳过表头
		next(csv_reader)
		for row in csv_reader:
			src = row[0]
			dst = row[1]
			capacity = float(row[2])

			# 在图中添加双向边，均分容量，随机交易成本
			G.add_edge(
				src,
				dst,
				balance = capacity / 2
			)
			G.add_edge(
				dst,
				src,
				balance = capacity / 2
			)

	# 最大联通子图
	components = nx.strongly_connected_components(G)
	largest_component = max(components, key=len)
	G = G.subgraph(largest_component).copy()

	"""
	# remove nodes with less than 2 neighbors, iteratively
	while True:
		nodes_to_remove = [node for node in G.nodes if len(list(G.neighbors(node))) < 2]
		if not nodes_to_remove:
			break
		G.remove_nodes_from(nodes_to_remove)
		# print(f"Removed nodes: {nodes_to_remove}")
	"""

	# relabel nodes
	mapping = dict(zip(G.nodes(), list(range(0, len(G)))))
	G = nx.relabel_nodes(G, mapping, copy=True)
	
	# collect data for stats printout later
	listC = []
	for e in G.edges(): 
		listC.append(G[e[0]][e[1]]['balance'])
	listC_sorted = np.sort(listC)

	# print stats
	print("number of nodes", len(G))
	print('num of channels', len(listC) / 2)
	print('average channel capacity', float(sum(listC))/(len(listC) / 2))
	print('medium channel balance', stats.scoreatpercentile(listC_sorted, 50))

	# load the transaction values from Bitcoin blockchain
	vals = []

	# BitcoinVal.txt
	# sampled_values.csv

	with open('data/lightning/BitcoinVal.txt', 'r') as f:
		for line in f:
			vals.append(float(line))

	# load src/dst from Ripple trace
	trans = []
	i = 0
	with open('data/ripple/ripple_val.csv', 'r') as f: 
		csv_reader = csv.reader(f, delimiter=',')
		for row in csv_reader:
			if float(row[2]) > 0:
				# map
				# TODO:该映射可能导致单个节点交易量过大
				tx_src = int(row[0]) % len(G)
				tx_dst = int(row[1]) % len(G)

				# 跳过自交易
				if tx_src == tx_dst: 
					continue
				
				val = vals[i]
				i += 1
				
				trans.append((int(tx_src), int(tx_dst), val))

	print('num of transactions', len(trans))

	return G, trans


# returns network topology and transactions for Ripple
def ripple_setup():
	file_path = "data/ripple/RP_topology.csv"

	# load network topology
	G = nx.DiGraph()
	with open(file_path, 'r') as f:
		csv_reader = csv.reader(f)
		# 跳过表头
		next(csv_reader)
		for row in csv_reader:
			src = row[0]
			dst = row[1]
			capacity = float(row[2])

			# 跳过自交易
			if src == dst:
				continue


			# 在图中添加双向边，均分容量，随机交易成本
			G.add_edge(
				src,
				dst,
				balance = capacity / 2
			)
			G.add_edge(
				dst,
				src,
				balance = capacity / 2
			)

	# 最大联通子图
	components = nx.strongly_connected_components(G)
	largest_component = max(components, key=len)
	G = G.subgraph(largest_component).copy()

	"""
	# remove nodes with less than 2 neighbors, iteratively
	while True:
		nodes_to_remove = [node for node in G.nodes if len(list(G.neighbors(node))) < 2]
		if not nodes_to_remove:
			break
		G.remove_nodes_from(nodes_to_remove)
		# print(f"Removed nodes: {nodes_to_remove}")
	"""

	# relabel nodes
	mapping = dict(zip(G.nodes(), list(range(0, len(G)))))
	G = nx.relabel_nodes(G, mapping, copy=True)
	
	# collect data for stats printout later
	listC = []
	for e in G.edges(): 
		listC.append(G[e[0]][e[1]]['balance'])
	listC_sorted = np.sort(listC)

	# print stats
	print("number of nodes", len(G))
	print('num of channels', len(listC) / 2)
	print('average channel capacity', float(sum(listC))/(len(listC) / 2))
	print('medium channel balance', stats.scoreatpercentile(listC_sorted, 50))

	# load transaction amounts and src/dst from Ripple trace
	trans = []
	with open('data/ripple/ripple_val.csv', 'r') as f: 
		csv_reader = csv.reader(f, delimiter=',')
		for row in csv_reader:
			if float(row[2]) > 0:
				# map each transaction to src/dst pair in the pruned graph
				# TODO:该映射可能导致单个节点交易量过大
				src = int(row[0]) % len(G)
				dst = int(row[1]) % len(G)

				# 跳过自交易
				if src == dst: 
					continue

				trans.append((int(src), int(dst), float(row[2])))

	print('num of transactions', len(trans))

	return G, trans


def scale_free_setup():

	num_node = 1000000

	#生成小世界拓扑
	#GG = nx.watts_strogatz_graph(num_node, 8, 0.8, 1)   # num_node nodes, connected to nearest 8 neighbors in ring topology, 0.8 probability of rewiring, random seed 1
	
	#生成无标度拓扑
	GG = nx.barabasi_albert_graph(num_node, 3, 1) # 每个新加入节点连接7个节点
	#GG = nx.powerlaw_cluster_graph(num_node, 3, 0.1) # p = 0.3314
	
	G = nx.DiGraph()
	for e in GG.edges():
		#设置通道余额
		cap = random.randint(20000,25000)
		G.add_edge(e[0], e[1], balance = float(cap) / 2)
		G.add_edge(e[1], e[0], balance = float(cap) / 2)

	# collect data for stats printout later
	listC = []
	for e in G.edges(): 
		listC.append(G[e[0]][e[1]]['balance'])
	listC_sorted = np.sort(listC)

	# print stats
	print("number of nodes", len(G))
	print('num of channels', len(listC) / 2)
	print('average channel capacity', float(sum(listC))/(len(listC) / 2))
	print('medium channel balance', stats.scoreatpercentile(listC_sorted, 50))

	# load transaction amounts and src/dst from Ripple trace
	trans = []
	with open('data/ripple/ripple_val.csv', 'r') as f: 
		csv_reader = csv.reader(f, delimiter=',')
		for row in csv_reader:
			if float(row[2]) > 0:
				# map each transaction to src/dst pair in the pruned graph
				# TODO:该映射可能导致单个节点交易量过大
				src = int(row[0]) % len(G)
				dst = int(row[1]) % len(G)

				# 跳过自交易
				if src == dst: 
					continue

				trans.append((int(src), int(dst), float(row[2])))

	print('num of transactions', len(trans))

	return G, trans
