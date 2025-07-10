import networkx as nx
import numpy as np
import random
import sys
import copy
import csv

sys.path.append('./partition')
import data_load
import network_partition
import index_topo_build

sys.path.append('./routing')
import shortest_path
import lnd
import speedymurmurs
import spider
import flash
import webflow
import segflow


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


def get_threshold(trans, percentage):
	sorted_trans = sorted(trans, key=lambda x: x[2])
	threshold = sorted_trans[int(1.0*percentage/100*(len(sorted_trans)-1))]
	return threshold[2]


def get_topology_and_transactions(trace):
	G_ori = nx.DiGraph()
	trans = []
	
	print(f"\n/**Load [{trace}] topology and transactions**/")

	if trace == 'ripple':
		G_ori, trans = data_load.ripple_setup()
	elif trace == 'lightning':
		G_ori, trans = data_load.lightning_setup()
	elif trace == 'scale_free':
		G_ori, trans = data_load.scale_free_setup()

	return (G_ori, trans)


# generates payments from trans
def generate_payments(seed, nflows, trans, G):
	random.seed(seed)
	temp_trans = trans.copy()  # 创建 trans 的副本
	payments = []

	while len(payments) < nflows:
		# 检查是否还有交易可选
		if not temp_trans:
			print(f"Insufficient transactions to generate {nflows} payments.")
			break

		# 随机选择交易并从 temp_trans 中移除
		tx = random.choice(temp_trans)
		temp_trans.remove(tx)

		# 检查路径是否存在
		if not nx.has_path(G, tx[0], tx[1]):
			continue

		# 添加到支付列表中
		payments.append((tx[0], tx[1], tx[2]))

	return payments


def convert_to_directed(undi_graph):
	di_graph = nx.DiGraph()

	for u, v, data in undi_graph.edges(data=True):
		di_graph.add_edge(u, v, **data)
		di_graph.add_edge(v, u, **data)

	return di_graph


# scale capacity
def scale_topo_cap(G_ori, scale_factor):
	G = nx.DiGraph()

	for e in G_ori.edges():
		G.add_edge(e[0], e[1], balance = G_ori[e[0]][e[1]]['balance']*scale_factor)

	return G
# scale capacity for subnets
def scale_topo_cap_for_subnets(subGraphs_ori, scale_factor):
	subGraphs = [nx.DiGraph() for _ in range(len(subGraphs_ori))]

	for i,subgraph in enumerate(subGraphs_ori):
		for e in subgraph.edges():
			subGraphs[i].add_edge(e[0], e[1], balance = subgraph[e[0]][e[1]]['balance']*scale_factor)

	return subGraphs


def run_general(scheme, trace, nflows, nruns, scale_list, config):

	# initialize topology and transactions from the dataset
	G_ori, trans = get_topology_and_transactions(trace)
	# 导出映射后的交易列表
	with open('partition_results/mapped_trans.csv', mode='w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(['Src', 'Dst', 'Amount'])  # 写入表头
		for src, dst, amount in trans:
			writer.writerow([src, dst, amount])


	if scheme == 'segflow':
		payment_frequency = compute_payment_frequency(trans)
		# 网络划分（无向图）
		print(f"\n/**Start network partitioning**/")
		subGraphs_undi, node_subnet_map, intra_payment_ratio, num_boundary_nodes  = network_partition.network_partitioning(G_ori, trans, payment_frequency, config)

		print(aaa)

		# 索引拓扑构建（有向图）
		print('\n/**Construct index topology**/')
		index_topo_di = index_topo_build.build_index_topo(subGraphs_undi, node_subnet_map)

		# 无向图——>有向图
		subGraphs_di = [convert_to_directed(subgraph) for subgraph in subGraphs_undi]


	if scheme == 'flash':
		# find the threshold
		threshold = get_threshold(trans, 90)
		print(threshold)

	#结果记录（不同的扩展因子）
	res_volume = [] #成功金额
	res_ratio = [] #成功率
	res_probe_msg = [] #prob消息数量
	res_commit_msg = [] #commit消息数量
	res_subnet_volume = []
	res_subnet_ratio = [] #子网内路由率
	
	# 迭代每个容量倍数
	for scale_factor in scale_list:

		# 扩展通道容量
		G = scale_topo_cap(G_ori, scale_factor)
		if scheme == 'segflow':
			subGraphs = scale_topo_cap_for_subnets(subGraphs_di, scale_factor)

		# 每轮结果
		volume_list = []
		ratio_list = []
		probe_msg_list = []
		commit_msg_list = []
		subnet_volume_list = []
		subnet_ratio_list = []

		print('\n\nStart simulation for', trace, '- scheme', scheme, '- scale factor', scale_factor, '- nflows', nflows)
		
		# payments to send
		for seed in range(nruns):
			print(f"Run {seed}.")

			#随机抽取支付
			seed = seed + 0
			random.seed(seed)
			payments = generate_payments(seed, nflows, trans, G)

			#根据方案类型执行路由算法
			if scheme == 'sp':
				volume, num_delivered, total_probing_messages, total_commit_messages = shortest_path.routing(G.copy(), payments)
			elif scheme == 'lnd':
				volume, num_delivered, total_probing_messages, total_commit_messages = lnd.routing(G.copy(), payments)
			elif scheme == 'speedymurmurs':
				nlandmarks = 3
				volume, num_delivered, total_probing_messages, total_commit_messages = speedymurmurs.routing(G.copy(), payments, nlandmarks)
			elif scheme == 'spider':
				k_paths = 4
				volume, num_delivered, total_probing_messages, total_commit_messages = spider.routing(G.copy(), payments, k_paths)
			elif scheme == 'flash':
				num_max_cache = 4
				k_iterations = 20
				volume, num_delivered, total_probing_messages, total_commit_messages = flash.routing(G.copy(), payments, threshold, num_max_cache, k_iterations)
			elif scheme == 'webflow':
				dimension = 3
				volume, num_delivered, total_probing_messages, total_commit_messages = webflow.routing(G.copy(), payments, dimension)
			elif scheme == 'segflow':
				subGraphs_copy = [copy.deepcopy(subgraph) for subgraph in subGraphs]
				volume, num_delivered, total_probing_messages, total_commit_messages, subnet_volume, subnet_delivered = segflow.routing(subGraphs_copy, index_topo_di.copy(), node_subnet_map, payments, G.copy())

			print(f"{volume}, {num_delivered}, {total_probing_messages}, {total_commit_messages}")

			# record stats for the current run
			volume_list.append(1.0*volume)
			ratio_list.append(1.0*num_delivered/nflows)
			probe_msg_list.append(1.0*total_probing_messages)
			commit_msg_list.append(1.0*total_commit_messages)
			if scheme == 'segflow':
				subnet_volume_list.append(1.0*subnet_volume)
				subnet_ratio_list.append(1.0*subnet_delivered/num_delivered)

		# average over runs and store averages
		res_volume.append(sum(volume_list)/nruns)
		res_ratio.append(sum(ratio_list)/nruns)
		res_probe_msg.append(sum(probe_msg_list)/nruns)
		res_commit_msg.append(sum(commit_msg_list)/nruns)
		if scheme == 'segflow':
			res_subnet_volume.append(sum(subnet_volume_list)/nruns)
			res_subnet_ratio.append(sum(subnet_ratio_list)/nruns)

	# log results to file
	with open(f'routing_results/{trace}-{scheme}-{nflows}.txt', 'w') as filehandle:
		filehandle.write(', '.join([ str(e) for e in res_volume ]) + '\n')
		filehandle.write(', '.join([ str(e) for e in res_ratio ]) + '\n')
		filehandle.write(', '.join([ str(e) for e in res_probe_msg ]) + '\n')
		filehandle.write(', '.join([ str(e) for e in res_commit_msg ]) + '\n')
		if scheme == 'segflow':
			filehandle.write(', '.join([ str(e) for e in res_subnet_volume ]) + '\n')
			filehandle.write(', '.join([ str(e) for e in res_subnet_ratio ]) + '\n')


# MAIN CODE
def main():

	# 实验参数
	ALL_SCHEMES = ['flash'] # ['sp', 'lnd', 'speedymurmurs', 'spider', 'flash', 'webflow', 'segflow']
	trace = 'ripple' # ripple/lightning/scale_free
	scale_list = [1] # [1, 10, 20, 30, 40, 50, 60]
	nflows_list = [10000] # [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
	nruns = 5

	# 划分参数
	config = {
		'n': 10,
		'balance_lambda': 1.1,
		'payment_lambda': 0
	}

	for nflows in nflows_list:
		for scheme in ALL_SCHEMES:
			run_general(scheme, trace, nflows, nruns, scale_list, config)


if __name__ == "__main__":
	main()
