import math
import random
from sympy import *

def norm_2(h):
    res = 0
    for x in h:
        res = res + x * x
    return math.sqrt(res)

def gradient_descent(f, grad_f, symbol_list, init_x, alpha, max_iter, tolerance):
    # print(f"grad_f: {grad_f}")
    x = init_x
    for iter in range(max_iter):
        grad = [grad_f[i].subs([(symbol_list[j], x[j]) for j in range(4)]) for i in range(4)]
        # x = x - alpha * grad
        x = [x[i] - alpha * grad[i] for i in range(4)]
        if iter > max_iter * 0.9:
            print(f"iter: {iter}")
            print(f"x: {x}")
            print(f"grad: {grad}")
            print(f"val: {f.subs([(symbol_list[j], x[j]) for j in range(4)])}")
        if norm_2(grad) < tolerance:
            print(f"early stop. iter={iter}, norm = {norm_2(grad)}, tolerance={tolerance}")
            break
    return x, f.subs([(symbol_list[j], x[j]) for j in range(4)])

x0 = [1, -2, 1, -2]

x1, x2, x3, x4 = symbols("x y z w")
symbol_list = [x1, x2, x3, x4]
# g = (x1 * x2 + x4**2 - x3) + (x1**2 - x2 + x4 * x3)
g = x1 * x2
val1 = g.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])
print(f"g1(x0)={val1}")
# gg = (x1*x2-x2*x4**2) + (x2*x4-x1*x3**2)
gg = x1
val2 = gg.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])
print(f"g2(x0)={val2}")
# ggg = x4**2+2*x4+1
ggg = x4
val3 = ggg.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])
print(f"g3(x0)={val3}")

f = (g-val1)**2 + (gg-val2)**2 + (ggg-val3)**2
print(f"f(x0)={f.subs([(x1, x0[0]), (x2, x0[1]), (x3, x0[2]), (x4, x0[3])])}")

grad_f = [diff(f, x1), diff(f, x2), diff(f, x3), diff(f, x4)]
print(f"f={f}")
print(f"grad_f={grad_f}")

# x = [1, 2, -3, 1]
# grad = [grad_f[i].subs([(symbol_list[j], x[j]) for j in range(4)]) for i in range(4)]
# print(grad)

for _ in range(5):
    x_initial = [random.uniform(-10, 10) for _ in range(4)]
    alpha = 0.05
    max_iter = 2000
    tolerance = 1e-4
    x, val = gradient_descent(f, grad_f, symbol_list, x_initial, alpha, max_iter, tolerance)
    grads = [grad_f[i].subs([(symbol_list[j], x[j]) for j in range(4)]) for i in range(4)]
    print(f"x={x}, val={val}, grads={grads}")
