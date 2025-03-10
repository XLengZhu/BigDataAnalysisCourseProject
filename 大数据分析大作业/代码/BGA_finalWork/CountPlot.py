import matplotlib.pyplot as plt
import pandas as pd

# 读取数据
file_path = 'count.txt'
data = pd.read_csv(file_path, sep='\t', header=None, names=['Location', 'Count'])

# 选取次数最高的前 15 个地名
top_15_data = data.nlargest(15, 'Count')

# 确保 Matplotlib 可以处理中文字符
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用 'SimHei' 字体
plt.rcParams['axes.unicode_minus'] = False  # 确保负号显示正确

# 绘制柱状图
plt.figure(figsize=(15, 8))
plt.bar(top_15_data['Location'], top_15_data['Count'], color='skyblue')
plt.title('Top 15 Locations by Count')
plt.xlabel('Location')
plt.ylabel('Count')
plt.xticks(rotation=45)  # 旋转 x 轴标签以便于阅读

plt.tight_layout()
plt.show()
