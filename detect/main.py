from collections import deque
import os, glob, json
from datetime import datetime
import torch

from checker import StrictChecker
from loader import Variables, Loader
from parser import Parser, Function
from poly_parser import PolynomialParser
from sym_merger import is_poly_symmetric, merge_symmetry, poly_sys_symmetry
from utils import Timer, var_ptn

leakage_found: bool = False

def check_int(func_buffer: list[str], files: list[str], logs):
    print(f"checking int: files={files}, numFuncs={len(func_buffer)}")
    log_item = {"files": files, "numFuncs": len(func_buffer)}
    timer = Timer()
    timer.start()
    isleak = False

    symmetry_q = deque()
    all_vars_q = deque()
    for func_i, poly in enumerate(func_buffer):
        parser = PolynomialParser()
        print("func_i = ", func_i)
        root = parser.parse(poly)
        # parser.print_structure(root)
        symmetry, symmetry_to_expand = parser.gen_poly_sym_all(root)
        if not is_poly_symmetric(symmetry, parser.all_vars):
            print("single poly leaks, symmetry = ", symmetry, ", all vars = ", parser.all_vars)
            isleak = True
            # break
        else:
            print("single poly does not leak")
        symmetry_q.append(symmetry)
        all_vars_q.append(parser.all_vars)
    
    # if len(symmetry_q) > 1 and isleak == False:
    if len(symmetry_q) > 1:
        final_symmetry, isleak = poly_sys_symmetry(symmetry_q, all_vars_q)
    
    if isleak:
        global leakage_found
        leakage_found = True

    time = timer.end()
    log_item["isleak"] = isleak
    log_item["SymmetryCheckTime"] = time
    logs["checkResults"].append(log_item)
    print(f"symmetry check finished, time={time}")

    timer.restart()

def check_real(vars: Variables, func_buffer: list[str], files: list[str], logs):
    print(f"checking real: files={files}, numFuncs={len(func_buffer)}")
    log_item = {"files": files, "numFuncs": len(func_buffer)}
    timer = Timer()
    timer.start()

    # initialize tensor
    used_vars = set()
    for func in func_buffer:
        matches = var_ptn.findall(func)
        used_vars |= {f"x{match[0]}_{match[1]}" for match in matches}
    used_vars = list(used_vars)
    numVars = len(used_vars)
    checker = StrictChecker(numVars)
    init_data = torch.tensor([vars.name2val[used_vars[_]] for _ in range(numVars)], dtype=torch.float64, requires_grad=True)
    checker.set_initial(init_data)
    name2val = {used_vars[_]: init_data[_] for _ in range(numVars)}
    time = timer.end()
    log_item["numVars"] = numVars
    log_item["initializeTime"] = time
    print(f"initializing finished, numVars={numVars}, time={time}")

    timer.restart()
    time_bp = 0.0
    timer_parse = Timer()
    time_parse = 0.0
    for func in func_buffer:
        timer_parse.restart()
        f = Function(func, name2val)
        time_parse += timer_parse.end()
        v = f.stack[0]
        time_bp += checker.add_function(v, False)
    time = timer.end()
    log_item["checkerInitParseTime"] = time_parse
    log_item["checkerInitBPTime"] = time_bp
    log_item["checkerInitConcatTime"] = time - time_parse - time_bp
    print(f"Function initializing finished, time distribution:")
    print(f"parse {time_parse} s, backward {time_bp} s, concat. {time - time_parse - time_bp} s")

    timer.restart()
    result = checker.check_leakage()
    time = timer.end()
    log_item["checkerCheckTime"] = time
    log_item["result"] = result
    logs["checkResults"].append(log_item)
    print(f"Function leakage checking finished, time={time}, Leakage Found={result}")
    if result:
        global leakage_found
        leakage_found = True

def main():
    timer = Timer()

    timer.start()
    loader = Loader()
    vars = Variables()
    loader.load_init_data(vars)
    time = timer.end()
    print(f"load initial value: Done, elapsed time: {time} s")

    output_path = r"../dbgen/output"
    parser = Parser(output_path)

    log_name = datetime.now().strftime("log-%m-%d-%H-%M-%S")+".json"
    logs = {"inputs": [], "checkResults": []}
    file_list = []
    global leakage_found
    # for i in range(1, 23):
    for i in range(2, 23): # 1 (too long), 9,19,20 (leakage, may not leak when data become more)
        for file_path in glob.glob(os.path.join(output_path, "Q%02d-*/part-*.csv" % i)):
            file_name = os.path.join(*file_path.split('/')[-2:])
            print(f"parsing functions in {file_name}")
            file_list.append(file_name)
            timer.restart()
            cnt = parser.collect_funcs(file_path)
            time = timer.end()
            logs["inputs"].append({"file": file_name, "numFunc": cnt, "collectTime": time})
            print(f"Parse: Done, elapsed time: {time} s")

            timer.restart()
            # if len(parser.func_buffer) >= 100:
            #     check(vars, parser.func_buffer, file_list, logs)
            #     parser.func_buffer.clear()
            #     json.dump(logs, open(log_name, "w"))
            #     file_list = []
        # per query
        # check_real(vars, parser.func_buffer, file_list, logs)
        check_int(parser.func_buffer, file_list, logs)
        parser.func_buffer.clear()
        json.dump(logs, open(log_name, "w"))
        file_list = []
    if file_list:
        check_real(vars, parser.func_buffer, file_list, logs)
        parser.func_buffer.clear()
        print("---------------------------------------------v")
        print(f"summering after last small queries:")
        print(f"\nparser max absolute error: {parser.max_abs_err}")
        print(f"\nLeakage Found: {leakage_found}")
        print("log:")
        print(logs)
        print("---------------------------------------------^")
        json.dump(logs, open(log_name, "w"))

if __name__ == "__main__":
    main()