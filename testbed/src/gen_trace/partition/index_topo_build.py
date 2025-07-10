import networkx as nx
import sys
import csv

from itertools import combinations
from concurrent.futures import ProcessPoolExecutor, as_completed

def process_graph(subgraph, boundary_nodes, subnet_id):
	print(f"\nStart build index channels in subnet {subnet_id}")

	index_topo_i = nx.DiGraph()

	for u, v in combinations(boundary_nodes, 2):  # 只考虑 u < v 的组合
		try:
			paths = list(nx.all_shortest_paths(subgraph, u, v))

			for path in paths:
				if any(node in boundary_nodes for node in path[1:-1]):  # 若任意路径经过其他边界节点，则该索引通道可以不建立
					break
			else:  # 如果没有路径经过边界节点
				length = len(paths[0]) - 1

				# 添加双向边
				index_topo_i.add_edge(u, v, length=length, subnet=subnet_id)
				index_topo_i.add_edge(v, u, length=length, subnet=subnet_id)

		except nx.NetworkXNoPath:
			#print(f"No path between {u} and {v} in subnet {subnet_id}.")
			continue

	# 输出索引拓扑情况
	print(f'Index topology of subnet {subnet_id}:')
	listC = []
	for e in index_topo_i.edges(): 
		listC.append(index_topo_i[e[0]][e[1]]['length'])
	print('number of nodes', len(index_topo_i))
	print('num of indexedges', len(listC))
	print('average length', float(sum(listC))/len(listC))

	# 导出索引拓扑
	file_name = f"partition_results/index_topology_{subnet_id}.csv"
	with open(file_name, mode='w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(['Node1', 'Node2', "Length", "Subnet"])  # 写入表头
		for node1, node2, edge_data in index_topo_i.edges(data=True):
			length = edge_data.get("length")
			sid = edge_data.get("subnet")
			writer.writerow([node1, node2, length, sid])
	
	print(f"End process subnet {subnet_id}\n")
	
	return index_topo_i


# 基于多最短路径的索引拓扑构建
def build_index_topo(subGraphs_undi, node_partition_map):
	# 筛选出割点
	cut_nodes = {node for node, partition_ids in node_partition_map.items() if len(partition_ids) > 1}

	boundary_nodes = []

	for i, subgraph in enumerate(subGraphs_undi):
		# 选出子网i中的边界节点
		boundary_nodes_in_i = cut_nodes.intersection(subgraph.nodes())
		boundary_nodes.append(boundary_nodes_in_i)
		print(f'Number of boundary nodes in subnet {i}: {len(boundary_nodes_in_i)} = {len(boundary_nodes[i])}')


	results = {}
	with ProcessPoolExecutor() as executor:

		# 提交所有任务到进程池
		future_to_subnet_id = {
			executor.submit(process_graph, subGraphs_undi[i], boundary_nodes[i], i): i
			for i in range(len(subGraphs_undi))
		}

		# 收集结果
		for future in as_completed(future_to_subnet_id):
			subnet_id = future_to_subnet_id[future]
			try:
				results[subnet_id] = future.result()
			except Exception as e:
				print(f"Error when processing subnet {subnet_id}: {e}")

	# 创建索引图（有向多重图）
	index_topo = nx.MultiDiGraph()

	# 合并索引图
	for subnet_id, index_topo_i in results.items():
		for u, v, data in index_topo_i.edges(data=True):
			index_topo.add_edge(u, v, **data)


	# 输出索引拓扑情况
	print("number of nodes", len(index_topo.nodes()))
	print('num of indexedges', len(index_topo.edges()))

	# 导出索引拓扑
	with open("partition_results/index_topology.csv", mode='w', newline='') as file:
		writer = csv.writer(file)
		writer.writerow(['Node1', 'Node2', "Length", "Subnet"])  # 写入表头
		for node1, node2, edge_data in index_topo.edges(data=True):
			length = edge_data.get("length")
			sid = edge_data.get("subnet")
			writer.writerow([node1, node2, length, sid])

	return index_topo
