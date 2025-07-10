import networkx as nx
import numpy as np
import random
import sys
import copy
import csv

sys.path.append('./partition')
import data_load
import network_partition


def compute_rsd(data):
	mean = np.mean(data)  # 计算均值
	std_dev = np.std(data, ddof=0)  # 计算标准差，使用总体标准差 (ddof=0)
	rsd = (std_dev / mean) * 100  # 计算 RSD
	return rsd


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


def run_partition(G_ori, trans, config, payment_frequency, nruns):

	rsd_list = []
	intra_payment_ratio_list = []
	num_nodes_list = []

	for _ in range(nruns):
		# 网络划分（无向图）
		subGraphs_undi, node_subnet_map, intra_payment_ratio, num_boundary_nodes = network_partition.network_partitioning(G_ori, trans, payment_frequency, config)
		# 计算RSD
		size_of_subnets = []
		for subgraph in subGraphs_undi:
			size_of_subnets.append(subgraph.number_of_nodes())
		rsd = compute_rsd(size_of_subnets)
		
		#print(f'rsd = {rsd}.')

		rsd_list.append(1.0*rsd)
		intra_payment_ratio_list.append(1.0*intra_payment_ratio)
		num_nodes_list.append(1.0*num_boundary_nodes)

	# 去除最高和最低的 RSD 及其相关值
	rsd_sorted_indices = sorted(range(len(rsd_list)), key=lambda i: rsd_list[i])  # 按 RSD 排序的索引
	trimmed_indices = rsd_sorted_indices[1:-1]  # 去掉第一个（最低）和最后一个（最高）
	trimmed_rsd = [rsd_list[i] for i in trimmed_indices]
	trimmed_intra_ratio = [intra_payment_ratio_list[i] for i in trimmed_indices]
	trimmed_num_nodes = [num_nodes_list[i] for i in trimmed_indices]

	# 计算去除后的平均值
	avg_rsd = sum(trimmed_rsd) / len(trimmed_rsd)
	avg_intra_payment_ratio = sum(trimmed_intra_ratio) / len(trimmed_intra_ratio)
	avg_num_nodes = sum(trimmed_num_nodes) / len(trimmed_num_nodes)

	return avg_rsd, avg_intra_payment_ratio, avg_num_nodes


def run_varying_n(trace, n_list, balance_lambda, payment_lambda, nruns):

	# initialize topology and transactions
	G_ori, trans = get_topology_and_transactions(trace)
	# compute the payment frequency
	payment_frequency = compute_payment_frequency(trans)

	# record results
	res_rsd = []
	res_ratio = []
	res_num_nodes = []
	for n in n_list:

		# set parameters
		config = {
			'n': n, 	
			'balance_lambda': balance_lambda,
			'payment_lambda': payment_lambda
		}
		print(f'\nconfig = {config}.')
		
		# run partition
		rsd, intra_payment_ratio, num_boundary_nodes = run_partition(G_ori, trans, config, payment_frequency, nruns)
		print(f'rsd = {rsd}; intra_payment_ratio = {intra_payment_ratio}, num_boundary_nodes = {num_boundary_nodes}.')
		
		res_rsd.append(rsd)
		res_ratio.append(intra_payment_ratio)
		res_num_nodes.append(num_boundary_nodes)

	with open(f'partition_results/EXP3-{trace}-balance_lambda={balance_lambda}-payment_lambda={payment_lambda}.txt', 'w') as filehandle:
		for n, rsd, ratio, num_nodes in zip(n_list, res_rsd, res_ratio, res_num_nodes):
			line = f"{n}, {balance_lambda}, {payment_lambda}, {rsd}, {ratio}, {num_nodes}\n\n"
			filehandle.write(line)


def run_varying_lambda_2(trace, n, balance_lambda_list, payment_lambda_list, nruns):

	# initialize topology and transactions
	G_ori, trans = get_topology_and_transactions(trace)
	# compute the payment frequency
	payment_frequency = compute_payment_frequency(trans)

	for balance_lambda in balance_lambda_list:
	
		# record results
		res_rsd = []
		res_ratio = []
		res_num_nodes = []
		for payment_lambda in payment_lambda_list:
			# set parameters
			config = {
				'n': n, 	
				'balance_lambda': balance_lambda,
				'payment_lambda': payment_lambda
			}
			print(f'\nconfig = {config}.')
		
			# run partition
			rsd, intra_payment_ratio, num_boundary_nodes = run_partition(G_ori, trans, config, payment_frequency, nruns)
			print(f'rsd = {rsd}; intra_payment_ratio = {intra_payment_ratio}, num_boundary_nodes = {num_boundary_nodes}.')
		
			res_rsd.append(rsd)
			res_ratio.append(intra_payment_ratio)
			res_num_nodes.append(num_boundary_nodes)

		with open(f'partition_results/EXP2-{trace}-{n}-balance_lambda={balance_lambda}.txt', 'w') as filehandle:
			for payment_lambda, rsd, ratio, num_nodes in zip(payment_lambda_list, res_rsd, res_ratio, res_num_nodes):
				line = f"{n}, {balance_lambda}, {payment_lambda}, {rsd}, {ratio}, {num_nodes}\n\n"
				filehandle.write(line)


def run_varying_lambda_1(trace, n, balance_lambda_list, payment_lambda_list, nruns):

	# initialize topology and transactions
	G_ori, trans = get_topology_and_transactions(trace)
	# compute the payment frequency
	payment_frequency = compute_payment_frequency(trans)

	for payment_lambda in payment_lambda_list:

		# record results
		res_rsd = []
		res_ratio = []
		res_num_nodes = []
		for balance_lambda in balance_lambda_list:
			# set parameters
			config = {
				'n': n, 	
				'balance_lambda': balance_lambda,
				'payment_lambda': payment_lambda
			}
			print(f'\nconfig = {config}.')
		
			# run partition
			rsd, intra_payment_ratio, num_boundary_nodes = run_partition(G_ori, trans, config, payment_frequency, nruns)
			print(f'rsd = {rsd}; intra_payment_ratio = {intra_payment_ratio}, num_boundary_nodes = {num_boundary_nodes}.')
		
			res_rsd.append(rsd)
			res_ratio.append(intra_payment_ratio)
			res_num_nodes.append(num_boundary_nodes)

		with open(f'partition_results/EXP1-{trace}-{n}-payment_lambda={payment_lambda}.txt', 'w') as filehandle:
			for balance_lambda, rsd, ratio, num_nodes in zip(balance_lambda_list, res_rsd, res_ratio, res_num_nodes):
				line = f"{n}, {balance_lambda}, {payment_lambda}, {rsd}, {ratio}, {num_nodes}\n\n"
				filehandle.write(line)


# MAIN CODE
def main():

	# 实验参数
	trace = 'lightning' # ripple/lightning/scale_free
	nruns = 7 # 需要去除rsd最大/最小值

	"""
	# #################### EXP1: Partition with varying lambda_1 ####################
	n = 10
	
	# generate the list of lambda_1
	balance_lambda_list = np.arange(1, 10.1, 0.1)
	balance_lambda_list = np.round(balance_lambda_list, 1).tolist()
	print(f'balance_lambda_list = {balance_lambda_list}.')
	
	payment_lambda_list = [0.0, 0.5, 1.0, 1.5, 2.0]
	#payment_lambda_list = [3.0]
	print(f'payment_lambda_list = {payment_lambda_list}.')
	
	run_varying_lambda_1(trace, n, balance_lambda_list, payment_lambda_list, nruns)
	"""
	
	"""
	# #################### EXP2: Partition with varying lambda_2 ####################
	n = 50
	
	balance_lambda_list = [1.5]
	print(f'balance_lambda_list = {balance_lambda_list}.')
	
	# generate the list of lambda_2
	payment_lambda_list = np.arange(0.21, 1.01, 0.01)
	payment_lambda_list = np.round(payment_lambda_list, 2).tolist()
	print(f'payment_lambda_list = {payment_lambda_list}.')
	
	run_varying_lambda_2(trace, n, balance_lambda_list, payment_lambda_list, nruns)
	"""


	# #################### EXP3: Partition with varying n ####################
	#n_list = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
	n_list = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
	balance_lambda = 2.5
	payment_lambda = 1

	run_varying_n(trace, n_list, balance_lambda, payment_lambda, nruns)


if __name__ == "__main__":
	main()
