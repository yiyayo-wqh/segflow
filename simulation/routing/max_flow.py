import networkx as nx
import collections


def find_paths(G, src, dst, k_iterations):
	removed_edges = []  # 记录被删除的边和属性

	path_set = []
	cap_set = []
	probing_messages = 0

	# 寻找最大流
	for iteration_count in range(k_iterations):
		# 查找路径，没有找到路径则停止
		try:
			path = nx.shortest_path(G, source=src, target=dst)
		except nx.NetworkXNoPath:
			break

		# 记录路径和路径容量信息
		path_set.append(path)
		path_cap = min(G[path[i]][path[i + 1]]["balance"] for i in range(len(path) - 1))
		cap_set.append(path_cap)
		# 更新探测消息数
		probing_messages += len(path) - 1

		# 更新G
		for i in range(len(path) - 1):
			u, v = path[i], path[i + 1]
			G[u][v]["balance"] -= path_cap

			# 如果反向边不存在，则添加回来
			if not G.has_edge(v, u):
				for idx, ((ru, rv), attr) in enumerate(removed_edges):
					if ru == v and rv == u:
						G.add_edge(v, u, **attr)
						del removed_edges[idx]
						break
			G[v][u]["balance"] += path_cap


			if G[u][v]['balance'] <= 0:
				removed_edges.append(((u, v), G[u][v].copy()))
				G.remove_edge(u, v)

	# 恢复被删除的边
	for (u, v), attr in removed_edges:
		G.add_edge(u, v, **attr)

	return path_set, cap_set, probing_messages


def routing(G, payment, k_iterations):
	src, dst, payment_size = payment

	# 统计数据
	path_set = []
	cap_set = []
	probing_messages = 0
	commit_messages = 0

	# 最大流寻路
	path_set, cap_set, probing_messages = find_paths(G, src, dst, k_iterations)

	# 先恢复 G 的余额
	for path, path_cap in zip(path_set, cap_set):
		for i in range(len(path) - 1):
			u, v = path[i], path[i + 1]
			G[u][v]['balance'] += path_cap
			G[v][u]['balance'] -= path_cap

	# 若未找到足够容量，则返回
	if sum(cap_set) < payment_size:
		return 0, probing_messages, 0

	# 分配支付
	remaining_amount = payment_size
	for path, path_cap in zip(path_set, cap_set):
		flow = min(path_cap, remaining_amount)
		remaining_amount -= flow
		for i in range(len(path) - 1):
			u, v = path[i], path[i + 1]
			G[u][v]['balance'] -= flow
			G[v][u]['balance'] += flow
		commit_messages += len(path) - 1
		# 如果支付需求已满足，停止分配
		if remaining_amount <= 1e-6:
			break

	return payment_size, probing_messages, commit_messages
