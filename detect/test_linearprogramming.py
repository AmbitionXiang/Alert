import numpy as np
from pulp import LpProblem, LpVariable, lpSum, LpMinimize, LpStatus

# 参数设置
n = 100000  # 每组整数的元素数量
m = 100  # 整数组的数量
L = 1000  # 下界
U = 2000  # 上界

# 生成随机整数数组
np.random.seed(0)  # 设置随机种子以保证结果可复现
A = np.random.randint(-100, 100, size=(m, n))

# 创建线性规划问题
prob = LpProblem("Integer_Linear_Programming", LpMinimize)

# 定义决策变量
c = [LpVariable(f'c{i}', cat='Integer') for i in range(n)]

# 定义目标函数（常数目标函数）
prob += 0

# 添加约束条件
for j in range(m):
    prob += lpSum([c[i] * A[j, i] for i in range(n)]) >= L
    prob += lpSum([c[i] * A[j, i] for i in range(n)]) <= U

# 求解线性规划问题
prob.solve()

# 输出结果
if LpStatus[prob.status] == 'Optimal':
    coefficients = [c[i].value() for i in range(n)]
    print("找到一组系数：", coefficients)
else:
    print("没有找到满足条件的系数")
    print("原因：", LpStatus[prob.status])
