import matplotlib.pyplot as plt
import pandas as pd
import matplotlib

# 设置 Matplotlib 字体以支持中文
matplotlib.rcParams['font.family'] = 'SimHei'
matplotlib.rcParams['font.sans-serif'] = ['SimHei']
matplotlib.rcParams['axes.unicode_minus'] = False

# 加载数据
data = pd.read_excel('data.xls')

# 筛选出震级在5级以上的地震
filtered_data = data[data['震级'] >= 4.5]

# 绘制经度和纬度的散点图
plt.figure(figsize=(10, 6))
plt.scatter(filtered_data['经度(°)'], filtered_data['纬度(°)'], c='red')
plt.title('震级≥4.5的地震的经度与纬度分布图')
plt.xlabel('经度(°)')
plt.ylabel('纬度(°)')
plt.grid(True)
plt.show()
