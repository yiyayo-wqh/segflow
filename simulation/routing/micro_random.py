import networkx as nx
import sys
from itertools import islice


# function to find k shortest paths
def k_shortest_paths(G, source, target, k):
	return list(islice(nx.shortest_simple_paths(G, source, target), k))


def get_path(mega_table, src, dst):
	return mega_table.get(src, {}).get(dst, [])


def add_paths(mega_table, src, dst, k_paths):
	if src not in mega_table:
		mega_table[src] = {}
	if dst not in mega_table[src]:
		mega_table[src][dst] = []
	mega_table[src][dst].extend(k_paths)


def routing(G, payment, mega_table, num_max_cache):
	src, dst, payment_size = payment

	probing_messages = 0
	commit_messages = 0

	path_set = get_path(mega_table, src, dst)

	# 更新路由表
	if not path_set:
		k_paths = k_shortest_paths(G, src, dst, num_max_cache)
		add_paths(mega_table, src, dst, k_paths)
		path_set = get_path(mega_table, src, dst)

	visited_paths = []
	sent_list = []

	# 路径迭代
	for path in path_set:
		remaining_credits = payment_size - sum(sent_list)

		# 计算路径可用余额
		pathCap = sys.maxsize
		for i in range(len(path) - 1):
			pathCap = min(pathCap, G[path[i]][path[i + 1]]["balance"])

		# 确定发送余额
		sent = remaining_credits if (pathCap > remaining_credits) else pathCap

		# commit first; if commit fails, probe the path, then commit the probed value
		commit_messages += len(path) - 1
		if remaining_credits > pathCap:
			probing_messages += len(path) - 1
			commit_messages += len(path) - 1

		visited_paths.append(path)
		sent_list.append(sent)

		# update path balance
		for i in range(len(path) - 1):
			G[path[i]][path[i + 1]]["balance"] -= sent
			G[path[i + 1]][path[i]]["balance"] += sent

		if pathCap >= remaining_credits:
			break

	# if fails, roll back
	if sum(sent_list) < payment_size:
		for i in range(len(visited_paths)):
			p = visited_paths[i]
			for j in range(len(p) - 1):
				G[p[j]][p[j + 1]]["balance"] += sent_list[i]
				G[p[j + 1]][p[j]]["balance"] -= sent_list[i]
		return 0, probing_messages, commit_messages
	else:
		return payment_size, probing_messages, commit_messages
