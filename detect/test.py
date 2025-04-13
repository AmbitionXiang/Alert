import bisect
import itertools

def find_insert_position(sorted_list, num):
    """返回插入有序列表后的索引位置"""
    return bisect.bisect_left(sorted_list, num)

# 测试案例
sorted_nums = [1, 3, 6, 8]
print(find_insert_position(sorted_nums, 4))   # 输出: 2
print(find_insert_position(sorted_nums, 3))   # 输出: 1（插入到第一个3的左侧）
print(find_insert_position(sorted_nums, 9))   # 输出: 4（插入末尾）
print(find_insert_position(sorted_nums, 0))   # 输出: 0（插入开头）
print(list(itertools.chain.from_iterable([[(('x_2', 'y_2'), ('z_1', 'w_1')), (('x_1', 'y_1'), ('z_2', 'w_2'))], [(('x_2', 'y_2'), ('z_2', 'w_2')), (('x_1', 'y_1'), ('z_1', 'w_1'))]])))

def_set1 = {"kele","python"}
def_set2 = {"kele","xuebi"}
def_set1.difference_update(def_set2)
print(def_set1, def_set2)

def_set1 = {"kele","python"}
def_set2 = {"kele","xuebi"}
def_set1.symmetric_difference_update(def_set2)
print(def_set1, def_set2)
