import pandas as pd

def process_csv(input_file, output_file):
    # 读取 CSV 文件，仅加载所需列
    df = pd.read_csv(input_file, usecols=['Account', 'issuer', 'value'])

    # 删除 Account 和 issuer 相同的行
    df = df.query("Account != issuer")

    # 合并相同 Account 和 issuer 的 value 值
    df = df.groupby(['Account', 'issuer'], as_index=False, sort=False)['value'].sum()

    # 保存处理后的数据到新的 CSV 文件
    df.to_csv(output_file, index=False)


# 调用
input_file = "trust sets_24-3-6.csv"  # 替换为实际输入文件路径
output_file = "RP_topology.csv"  # 替换为实际输出文件路径
process_csv(input_file, output_file)