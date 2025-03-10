import pandas as pd

# 加载 .xls 文件
xls_file = 'data.xls'  # 替换为你的文件路径
df = pd.read_excel(xls_file)

# 将数据保存为 .txt 文件
txt_file = 'data.txt'  # 你希望保存的.txt文件的路径和名称
df.to_csv(txt_file, index=False, sep='\t', encoding='utf-8')  # 使用制表符作为分隔符
