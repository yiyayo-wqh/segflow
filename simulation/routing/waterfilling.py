import numpy as np
import networkx as nx
import random
from random import shuffle
from itertools import islice
import sys
import time


def rank(cap, maxSet, secMaxSet):
	largest = -sys.maxsize - 1
	secLargest = -sys.maxsize - 1

	for i in range(len(cap)):
		if cap[i] > largest:
			secLargest = largest
			largest = cap[i]
		elif cap[i] > secLargest and cap[i] != largest:
			secLargest = cap[i]

	for i in range(len(cap)):
		if cap[i] == largest:
			maxSet.append(i)
		if cap[i] == secLargest:
			secMaxSet.append(i)

	if secLargest == -sys.maxsize - 1:
		return 0

	return largest - secLargest


def set_credits(val, mins):
	sum = 0

	for i in mins:
		sum += i
	if val > sum:
		return mins

	remainder = val
	minsCopy = list(mins)
	res = [0] * len(minsCopy)
	creditsToAssign = 0
	diff = 0

	while remainder > 0:
		maxSet = []
		secMaxSet = []
		diff = rank(minsCopy, maxSet, secMaxSet)
		creditsToAssign = diff if remainder > len(maxSet) * diff else (remainder / len(maxSet))

		if diff == 0:
			creditsToAssign = remainder / len(minsCopy)

		for index in maxSet:
			res[index] += creditsToAssign
			minsCopy[index] -= creditsToAssign
		remainder -= creditsToAssign * len(maxSet)
	return res


def routing(G, cur_payments, k):
	# 统计信息
	throughput = 0
	num_delivered = 0
	total_probing_messages = 0
	total_commit_messages = 0

	# 迭代支付
	for payment in cur_payments:
		src = payment[0]
		dst = payment[1]
		payment_size = payment[2]

		# 选择k不相交路径
		path_set = list(nx.edge_disjoint_paths(G, src, dst))  # TODO: use k edge-disjoint widest paths
		if len(path_set) > k:
			path_set = path_set[0:k]

		# 计算路径容量
		path_caps = [sys.maxsize] * len(path_set)
		index_p = 0
		for path in path_set:
			total_probing_messages += len(path) - 1
			for i in range(len(path) - 1):
				path_caps[index_p] = min(path_caps[index_p], G[path[i]][path[i + 1]]["balance"])
			index_p += 1

		# 注水式分配金额
		res = set_credits(payment_size, path_caps)

		# commit
		index_p = 0
		for path in path_set:
			for i in range(len(path) - 1):
				G[path[i]][path[i + 1]]["balance"] -= res[index_p]
				G[path[i + 1]][path[i]]["balance"] += res[index_p]
			total_commit_messages += len(path) - 1
			index_p += 1

		# 若失败则回滚，否则统计
		if sum(res) < payment[2] - 1e-6:
			for i in range(len(path_set)):
				p = path_set[i]
				for j in range(len(p) - 1):
					G[p[j]][p[j + 1]]["balance"] += res[i]
					G[p[j + 1]][p[j]]["balance"] -= res[i]
		else:
			num_delivered += 1
			throughput += payment[2]

	return throughput, num_delivered, total_probing_messages, total_commit_messages
