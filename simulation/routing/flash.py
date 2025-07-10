import micro_random
import max_flow

def routing(G, payments, threshold, num_max_cache, k_iterations):
	# 统计信息
	throughput = 0
	num_delivered = 0
	total_probing_messages = 0
	total_commit_messages = 0

	mega_table = {}  # 路由表

	# 迭代支付
	for payment in payments:

		if payment[2] < threshold:  # 处理小额支付
			sent, probing_messages, commit_messages = micro_random.routing(G, payment, mega_table, num_max_cache)
		else:  # 处理大额支付
			sent, probing_messages, commit_messages = max_flow.routing(G, payment, k_iterations)

		total_probing_messages += probing_messages
		total_commit_messages += commit_messages
		if payment[2] <= sent:
			num_delivered += 1
			throughput += sent

	return throughput, num_delivered, total_probing_messages, total_commit_messages
