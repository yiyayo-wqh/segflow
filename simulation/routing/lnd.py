import networkx as nx

def restore_edges(G, removed_edges):
	"""恢复删除的边及其属性"""
	for edge, attr in removed_edges:
		G.add_edge(edge[0], edge[1], **attr)

def lnd(src, dst, G, a):
	commit_messages = 0
	removed_edges = []

	for _ in range(3):# 迭代3次
		try:
			path = nx.shortest_path(G, src, dst)

			invalid_edge = None
			for i in range(len(path) - 1):
				commit_messages += 1  # 模拟commit
				if G[path[i]][path[i + 1]].get("balance", 0) < a:
					invalid_edge = (path[i], path[i + 1])
					break

			if not invalid_edge:
				# 如果没有无效边，则返回路径
				restore_edges(G, removed_edges)
				return path, commit_messages

			# 删除无效边
			removed_edges.append((invalid_edge, G[invalid_edge[0]][invalid_edge[1]].copy()))
			G.remove_edge(*invalid_edge)

		except nx.NetworkXNoPath:
			# 如果没有路径可用，返回 None
			break

	restore_edges(G, removed_edges)
	return [], commit_messages


def routing(G, payments):
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
		path, commit_messages = lnd(src, dst, G, payment_size)
		total_commit_messages += commit_messages

		# success
		if path:
			# update balance
			for i in range(len(path) - 1):
				G[path[i]][path[i + 1]]["balance"] -= payment_size
				G[path[i + 1]][path[i]]["balance"] += payment_size
			throughput += payment_size
			num_delivered += 1

	return throughput, num_delivered, total_probing_messages, total_commit_messages
