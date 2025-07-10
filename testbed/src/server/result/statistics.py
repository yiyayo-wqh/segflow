import re

# 节点数
file_count = 1500

def statistics(directory):
	successful_files = 0  # Track successfully processed files

	total_time = 0
	total_successful_transactions = 0
	total_transactions = 0
	total_successful_volume = 0
	total_volume = 0
	total_probe_messages = 0
	total_commit_messages = 0
	total_reverse_messages = 0
	total_confirm_messages = 0

	total_subnet_time = 0
	total_successful_subnet_transactions = 0
	total_subnet_transactions = 0
	total_successful_subnet_volume = 0
	total_subnet_volume = 0

	pattern = re.compile(r"finished ([\d\.]+) (\d+) (\d+) ([\d\.]+) ([\d\.]+) (\d+) (\d+) (\d+) (\d+) ([\d\.]+) (\d+) (\d+) ([\d\.]+) ([\d\.]+)")

	for i in range(1, file_count + 1):
		file_path = f"{directory}/lll{i}.log"
		isfound = False
		try:
			with open(file_path, 'r') as file:
				for line in file:
					if 'finished' in line:
						match = pattern.search(line)
						if match:
							time = float(match.group(1))
							successful_transactions = int(match.group(2))
							transactions = int(match.group(3))
							successful_volume = float(match.group(4))
							volume = float(match.group(5))
							probe_messages = int(match.group(6))
							commit_messages = int(match.group(7))
							reverse_messages = int(match.group(8))
							confirm_messages = int(match.group(9))

							subnet_time = float(match.group(10))
							successful_subnet_transactions = int(match.group(11))
							subnet_transactions = int(match.group(12))
							successful_subnet_volume = float(match.group(13))
							subnet_volume = float(match.group(14))

							#print(f"File{i}: time={time}.")

							total_time += time
							total_successful_transactions += successful_transactions
							total_transactions += transactions
							total_successful_volume += successful_volume
							total_volume += volume
							total_probe_messages += probe_messages
							total_commit_messages += commit_messages
							total_reverse_messages += reverse_messages
							total_confirm_messages += confirm_messages

							total_subnet_time += subnet_time
							total_successful_subnet_transactions += successful_subnet_transactions
							total_subnet_transactions += subnet_transactions
							total_successful_subnet_volume += successful_subnet_volume
							total_subnet_volume += subnet_volume

							successful_files += 1
							isfound = True
							break  # Stop after finding the 'finished' line
				if not isfound:
					print(f"Not found 'finished' in lll{i}.log")
		except FileNotFoundError:
			print(f"File {file_path} not found. Skipping.")
		except Exception as e:
			print(f"Error reading file {file_path}: {e}")

	if successful_files > 0:
		average_time = total_time / total_transactions
		total_messages = total_probe_messages + total_commit_messages + total_reverse_messages + total_confirm_messages

		print(f"Across {successful_files} files:")
		print(f"[TIME] Total Time: {total_time} ms, Average Time (all): {average_time} ms.")
		print(f"[TRANS] Total Transactions: {total_transactions}, Successful Transactions: {total_successful_transactions}, Total Volume: {total_volume}, Successful Volume: {total_successful_volume}.")
		print(f"[MESSAGES] Total Messages: {total_messages}, Probe Messages: {total_probe_messages}, Commit Messages: {total_commit_messages}, Reverse Messages: {total_reverse_messages}, Confirm Messages: {total_confirm_messages}.")
		print(f"[SUBNET] Total Time: {total_subnet_time}, Total Transactions: {total_subnet_transactions}, Successful Transactions: {total_successful_subnet_transactions}, Total Volume: {total_subnet_volume}, Successful Volume: {total_successful_subnet_volume}.")
	else:
		print("No files were successfully processed.")

# Specify the directory containing the .log files
print(f"******************Flash*****************")
directory_path = '1'
statistics(directory_path)

print(f"******************SP*****************")
directory_path = '2'
statistics(directory_path)

print(f"*****************Waterfilling******************")
directory_path = '3'
statistics(directory_path)

print(f"*****************Spider******************")
directory_path = '4'
statistics(directory_path)

print(f"*****************LND******************")
directory_path = '5'
statistics(directory_path)

print(f"*****************SegFlow******************")
directory_path = '6'
statistics(directory_path)
