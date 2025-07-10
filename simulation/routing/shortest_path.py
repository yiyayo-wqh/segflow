import networkx as nx
import sys


def routing(G, cur_payments):
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
		path = nx.shortest_path(G, src, dst)

		# probe
		path_cap = sys.maxsize
		for i in range(len(path) - 1):
			path_cap = min(path_cap, G[path[i]][path[i + 1]]["balance"])

		sent = payment_size if (path_cap > payment_size) else path_cap

		# commit
		for i in range(len(path) - 1):
			G[path[i]][path[i + 1]]["balance"] -= sent
			G[path[i + 1]][path[i]]["balance"] += sent

		total_commit_messages += len(path) - 1

		# fail, roll back
		if sent < payment[2]:
			for i in range(len(path) - 1):
				G[path[i]][path[i + 1]]["balance"] += sent
				G[path[i + 1]][path[i]]["balance"] -= sent
		else:  # success, record
			throughput += sent
			num_delivered += 1

	return throughput, num_delivered, total_probing_messages, total_commit_messages
