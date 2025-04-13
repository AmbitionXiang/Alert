import math
import random
from sympy import symbols, diff, parse_expr, Matrix
import numpy as np
from time import time

eps = 1e-6

def gaussianElimination(mat: np.ndarray):
    n, m = mat.shape
    print(f"Gaussian Elimination: m={m}, n={n}")
    cur_rid = 0
    for i in range(m):
        rid = -1
        for k in range(cur_rid, n):
            if abs(mat[k][i]) > eps:
                rid = k
                break
        if rid == -1:
            continue
        if rid != cur_rid:
            mat[[cur_rid, rid]] = mat[[rid, cur_rid]]
        mat[cur_rid] = mat[cur_rid] / mat[cur_rid][i]
        for k in range(n):
            if abs(mat[k][i]) < eps or k == cur_rid:
                continue
            mat[k] = mat[k] - mat[cur_rid] * mat[k][i]
        cur_rid += 1
        if cur_rid == n:
            break

def symbolic_derivative(expression_str, varstrs, varsymbols):
    vardict = dict(zip(varstrs, varsymbols))
    # print("vardict = ", vardict)
    f = parse_expr(expression_str, local_dict=vardict)
    derivatives = {varsymbol: diff(f, varsymbol) for varsymbol in varsymbols}
    return derivatives, vardict

for var_num in [10, 100, 1000, 3000, 10000, 30000, 100000, 300000, 1000000]:
    max_degree = 3
    coeff_range = (-10, 10)
    # expression_str = "x1**2 + 3*x1 + 2*x2 + 4*x3 + 5*x4 + 6*x5"
    varstrs = [f'x{i}' for i in range(var_num)]
    # print("varstrs = ", varstrs)
    varsymbols = [symbols(varstr) for varstr in varstrs]
    expression_str = " + ".join([
        f"{random.randint(*coeff_range)}*{var}**{random.randint(0, max_degree)}"
        for var in varstrs
        for _ in range(random.randint(1, 3))  # Each variable generates 1 to 3 terms.
    ])

    start_time = time()
    derivatives, vardict = symbolic_derivative(expression_str, varstrs, varsymbols)
    end_time = time()
    elapsed_time = end_time - start_time

    print("a random varstr's derivative = ", derivatives[vardict[varstrs[random.randint(1, var_num)]]])
    print("var_num = ", var_num, ", time elapsed = ", elapsed_time)


# x0 = [4.2, 1.421, -2.34, 1.31]
# x1, x2, x3, x4 = symbols("x y z w")
# symbol_list = [x1, x2, x3, x4]
# # g = (x1 * x2 + x4**2 - x3) + (x1**2 - x2 + x4 * x3)
# g = x2**2
# val1 = g.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])
# print(f"g1(x0)={val1}")
# # gg = (x1*x2-x2*x4**2) + (x2*x4-x1*x3**2)
# gg = x1+x3*x4
# val2 = gg.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])
# print(f"g2(x0)={val2}")
# # ggg = x4**2+2*x4+1
# ggg = (x4+x1*x3**2)*(x1+x2)
# val3 = ggg.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])
# print(f"g3(x0)={val3}")

# funcs = Matrix([g, gg, ggg])
# args = Matrix(symbol_list)
# jacob = funcs.jacobian(args).subs([(symbol_list[i], x0[i]) for i in range(4)])

# print(jacob)

# mat = np.array(jacob)
# print(mat)
# # print(type(mat))

# gaussianElimination(mat)
# print(mat)
