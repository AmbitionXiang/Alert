import os
import torch

from loader import Variables
from utils import var_ptn, num_ptn

class Function:
    """
    Parser for a single function.
    Now it only supports for addition, subtraction, multiplication and division.
    """
    def __init__(self, s: str, name2val: dict) -> None:
        self.s = s
        self.length = len(s)
        self.name2val = name2val
        self.ptr = 0
        self.stack = []
        self.parse()
    
    def consume(self):
        # print(f"JZPDEBUG: in consume: self.ptr is {self.ptr}, current char is {self.s[self.ptr]}, remaining s is {self.s[self.ptr:]}")
        # while (self.s[self.ptr] == ' ') self.ptr += 1
        if self.s[self.ptr] in ['(', ')', '+', '-', '*', '/']:
            self.ptr += 1
            return self.s[self.ptr-1]
        if self.s[self.ptr] == 'x':
            x = var_ptn.match(self.s, self.ptr).group()
            self.ptr += len(x)
            return self.name2val[x]
        if self.s[self.ptr] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.']:
            x = num_ptn.match(self.s, self.ptr).group()
            self.ptr += len(x)
            return float(x)
        print(f"ERROR: in consume: self.s is {self.s}, and self.ptr is {self.ptr}. Don't know what to do next")
        print(f"JZPDEBUG: context: {self.s[self.ptr-10: self.ptr]}  |  {self.s[self.ptr:]}")
        print(f"JZPDEBUG: length is {self.length}")
        raise ValueError

    def operate(self):
        if self.stack[-2] == '+':
            self.stack[-3] = self.stack[-3] + self.stack[-1]
        elif self.stack[-2] == '-':
            self.stack[-3] = self.stack[-3] - self.stack[-1]
        elif self.stack[-2] == '*':
            try:
                self.stack[-3] = self.stack[-3] * self.stack[-1]
            except TypeError:
                print(f"consumed str is {self.s[:self.ptr]}")
                print(f"current stack is {self.stack}")
                raise Exception
        elif self.stack[-2] == '/':
            self.stack[-3] = self.stack[-3] / self.stack[-1]
        self.stack.pop()
        self.stack.pop()

    def parse(self):
        while self.ptr < self.length:
            x = self.consume()
            if x == '(':
                self.stack.append(x)
            elif x == ')':
                while self.stack[-2] != '(':
                    self.operate()
                y = self.stack.pop()
                self.stack.pop()
                self.stack.append(y)
            elif x in ['+', '-']:
                if (len(self.stack) >= 1 and self.stack[-1] == '(') or len(self.stack) == 0:
                    # handle unary '-' like '-24.12'
                    self.stack.append(0)
                if len(self.stack) >= 2 and self.stack[-2] in ['*', '/']:
                    self.operate()
                if len(self.stack) >= 2 and self.stack[-2] in ['+', '-']:
                    self.operate()
                self.stack.append(x)
            elif x in ['*', '/']:
                if len(self.stack) >= 2 and self.stack[-2] in ['*', '/']:
                    self.operate()
                self.stack.append(x)
            else:
                self.stack.append(x)
        while len(self.stack) > 1:
            self.operate()

class Parser:
    """
    A parser for parsing operation expressions like '(x7_111+x7_112)/2'
    """
    def __init__(self, output_dir: str = r"/../dbgen/output") -> None:
        self.output_dir = output_dir
        self.max_abs_err = 0.0
        self.func_buffer = []

    def collect_funcs(self,
                      file_path: str = r"/../dbgen/output/Q03-0/part-00000-92e37250-26b9-4ff6-83c0-d95cc2d4214c-c000.csv"):
        count = 0
        with open(file_path, "r") as f:
            for row in f:
                items = [x.strip() for x in row.split(',')]
                for item in items:
                    if count >= 200:
                        break
                    if var_ptn.search(item):
                        self.func_buffer.append(item)
                        count += 1
        return count

    # def parse_file(self, vars: Variables,
    #                file_path: str = r"/../dbgen/output/Q03-0/part-00000-92e37250-26b9-4ff6-83c0-d95cc2d4214c-c000.csv"):
    #     # expression = "23+45*76*312-(-41*412+51)*412+513*312-51.42+412*42.13/(41-12.3*4)"
    #     # func = Function(expression, vars)
    #     # print(f"result of {expression} is {func.stack[0]}")
    #     # return func.stack[0]
    
    #     val_list = []
    #     with open(file_path, "r") as f:
    #         for row in f:
    #             items = [x.strip() for x in row.split(',')]
    #             last_item = 0
    #             for item in items:
    #                 if var_ptn.search(item):
    #                     if len(item) > 3000000:
    #                         print(f"parsing: Super long expression detected, expression string length: {len(item)}")
    #                     func = Function(item, vars)
    #                     val_list.append(func.stack[0])
    #                     if abs(func.stack[0] - float(last_item)) > 1e-4:
    #                         print(f"ERROR: value of the expression should be {last_item}, but calculating result is {func.stack[0]}")
    #                     self.max_abs_err = max(self.max_abs_err, abs(func.stack[0] - float(last_item)))
    #                     if len(item) > 3000000:
    #                         print(f"parsing: result of super long expression: {func.stack[0]}")
    #                 last_item = item
    #     return val_list
