import networkx as nx
import csv
import random
import sys
from itertools import islice

sys.path.append('./partition')
import network_partition
import index_topo_build

def compute_payment_frequency(trans):
	payment_frequency = {}

	for payment in trans:
		src = payment[0]
		dst = payment[1]

		# 添加到支付频率列表中
		if src not in payment_frequency:
			payment_frequency[src] = {}
		if dst not in payment_frequency:
			payment_frequency[dst] = {}
		if dst not in payment_frequency[src]:
			payment_frequency[src][dst] = 0
		if src not in payment_frequency[dst]:
			payment_frequency[dst][src] = 0

		payment_frequency[src][dst] += 1
		payment_frequency[dst][src] += 1

	return payment_frequency

def convert_to_directed(undi_graph):
	di_graph = nx.DiGraph()

	for u, v, data in undi_graph.edges(data=True):
		di_graph.add_edge(u, v, **data)
		di_graph.add_edge(v, u, **data)

	return di_graph


def main():
	# PARAMETERS
	num_node = 1500 # number of nodes
	nflows = 10000 # number of payments
	bal_range = [1000, 1500]

	percentage = 90 # Flash percentage
	# 划分参数
	config = {
		'n': 3,
		'balance_lambda': 1.6,
		'payment_lambda': 1
	}


	# NETWORK CONSTRUCTION
	random.seed(1)
	# 生成无标度拓扑
	GG = nx.barabasi_albert_graph(num_node, 3, 1)  # 每个新加入节点连接3个节点，1为随机数种子
	# GG = nx.powerlaw_cluster_graph(num_node, 2, 0.3314) # 0.3314

	G = nx.DiGraph()
	for e in GG.edges():
		#设置通道余额
		bal_01 = random.randint(bal_range[0],bal_range[1])
		bal_10 = random.randint(bal_range[0],bal_range[1])
		G.add_edge(e[0], e[1], balance=bal_01)
		G.add_edge(e[1], e[0], balance=bal_10)

	# LOAD TRANSACTIONS FROM RIPPLE DATASET
	node_list = list(G.nodes())
	trans = []
	with open('data/ripple_val.csv', 'r') as f:
		csv_reader = csv.reader(f, delimiter=',')
		for row in csv_reader:
			if float(row[2]) > 0:
				src = int(row[0]) % len(node_list)
				dst = int(row[1]) % len(node_list)
				if src == dst:
					continue
				trans.append((int(src), int(dst), float(row[2])))

	# 计算支付频率
	payment_frequency = compute_payment_frequency(trans)
	# 划分子网
	subGraphs_undi, node_subnet_map, intra_payment_ratio, num_boundary_nodes = network_partition.network_partitioning(G, trans, payment_frequency, config)
	# 索引拓扑构建（有向图）
	print('\n/**Construct index topology**/')
	index_topo_di = index_topo_build.build_index_topo(subGraphs_undi, node_subnet_map)
	# 无向图——>有向图
	subGraphs_di = [convert_to_directed(subgraph) for subgraph in subGraphs_undi]


	# 导出subnet_map.csv
	with open("subnet_map.csv", mode="w", newline="") as csvfile:
		writer = csv.writer(csvfile)
		for node_id, subnet_ids in node_subnet_map.items():
			row = [node_id] + sorted(list(subnet_ids))  # 先写node，再写它所属的所有子网ID
			writer.writerow(row)


	# COMPUTE THRESHOLD FOR Flash
	sorted_trans = sorted(trans, key=lambda x: x[2])
	threshold = sorted_trans[int(1.0*percentage/100*(len(sorted_trans)-1))][2]
	print('threshold for ripple trace', threshold)

	# 输出G
	with open("graph.txt", 'w') as f:
		for u, v, data in G.edges(data=True):
			bal = data['balance']
			f.write(f"{u},{v},{bal}\n")
	# 输出子网拓扑
	for i, subgraph in enumerate(subGraphs_di):
		with open(f"subgraph{i}.txt", 'w') as f:
			for u, v, data in subgraph.edges(data=True):
				bal = data['balance']
				f.write(f"{u},{v},{bal}\n")


	# GENERATE PAYMENTS
	f2 = open("payments.txt", 'w+')
	payments = []
	for k in range(nflows):
		while True:
			index = random.randint(0, len(trans)-1)
			tx = trans[index]
			if nx.has_path(G, tx[0], tx[1]):
				break
		payments.append((tx[0], tx[1], tx[2], 1, 0))
		f2.write("%d,%d,%f\n" % (tx[0], tx[1], tx[2]))
	f2.close()

	# GENERATE PATHS FOR PAYMENTS
	f3 = open("path.txt", 'w+')
	need_num_path = 10  # max paths to compute
	path_visited = [ [ 0 for x in range(num_node) ] for y in range(num_node) ]

	for s_payment in payments:
		src = s_payment[0]
		dst = s_payment[1]

		if path_visited[src][dst] == 1:
			continue

		path_visited[src][dst] = 1

		#path_set = list(islice(nx.edge_disjoint_paths(G, src, dst), need_num_path))
		path_set = list(islice(nx.shortest_simple_paths(G, src, dst), need_num_path))

		for path in path_set:
			f3.write("%d,%d," % (src, dst))
			f3.write(",".join([ str(i) for i in path ]) + "\n")
	f3.close()


if __name__ == "__main__":
	main()
