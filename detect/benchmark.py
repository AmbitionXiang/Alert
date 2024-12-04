import os, glob, json, random
from datetime import datetime
import torch

from checker import StrictChecker
from utils import Timer, var_ptn

def random_generate(data: torch.Tensor, n: int):
    # return data.sum()
    t = random.randint(0, 3)
    # t = 1
    if t == 0:
        p = random.randint(0, n-1)
        v = data[p]
    elif t == 1:
        p1 = random.randint(0, n-1)
        p2 = random.randint(0, n-1)
        # p1 = 23
        # p2 = 511
        v = (data[p1]+1) * (data[p2]+1)
    elif t == 2:
        p1 = random.randint(0, n-1)
        p2 = random.randint(0, n-1)
        p3 = random.randint(0, n-1)
        v = (data[p1]+1) * (data[p2]+1) * (data[p3]+1)
    elif t == 3:
        p1 = random.randint(0, n-1)
        p2 = random.randint(0, n-1)
        v = (data[p1]+1) / (data[p2]+1)
    # k = random.randint(-2, 2)
    k = 2
    return v*k

def benchmark(numVars=10000, numFuncs=100):
    checker = StrictChecker(numVars)
    data = torch.rand(numVars, dtype=torch.float64, requires_grad=True)
    # data = [2 for _ in range(numVars)]
    checker.set_initial(data)

    log_item = {"numVars": numVars, "numfuncs": numFuncs, "genTime": 0.0, "addFuncTime": 0.0, "checkTime": 0.0}
    timer = Timer()
    for i in range(numFuncs):
        timer.restart()
        v = None
        for j in range(numVars):
            w = random_generate(data, numVars)
            v = v + w if v else w
        time = timer.end()
        log_item["genTime"] += time
        timer.restart()
        checker.add_function(v)
        time = timer.end()
        log_item["addFuncTime"] += time
    timer.restart()
    result = checker.check_leakage()
    time = timer.end()
    log_item["checkTime"] += time

    print(f"benchmark(numVars={numVars}, numFuncs={numFuncs}):")
    print(f"log: {log_item}\n")
    return log_item

if __name__ == "__main__":
    nf = 100
    l1 = [(1000, nf), (3000, nf), (10000, nf), (30000, nf), (100000, nf), (300000, nf), (1000000, nf)]
    # l1 = [(3000, nf)]
    l1_logs = []
    for config in l1:
        log_item = benchmark(config[0], config[1])
        l1_logs.append(log_item)
        json.dump(l1_logs, open("benchmark_100func.json", "w"))
        print(f"l1_logs:")
        print(l1_logs)
        print()

    nv = 50000
    l2 = [(nv, 10), (nv, 30), (nv, 100), (nv, 300), (nv, 1000)]
    l2_logs = []
    for config in l2:
        log_item = benchmark(config[0], config[1])
        l2_logs.append(log_item)
        json.dump(l2_logs, open("benchmark_50000var.json", "w"))
        print(f"l2_logs:")
        print(l2_logs)
        print()
