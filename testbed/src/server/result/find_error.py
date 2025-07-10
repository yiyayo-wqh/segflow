directory_path = '2'
file_count = 1000  # 节点数
target_error = '[IN] error HandleSesIn receive data from'

def find_error(directory):
	for i in range(1, file_count + 1):
		file_path = f"{directory}/lll{i}.log"
		try:
			with open(file_path, 'r') as file:
				for line in file:
					if target_error in line:
						print(f"[FOUND] lll{i}.log: {line.strip()}")
						break  # 找到一条就跳出，继续下一个文件
		except FileNotFoundError:
			print(f"[SKIP] {file_path} not found.")
		except Exception as e:
			print(f"[ERROR] reading {file_path}: {e}")

find_error(directory_path)
