import time
import re

class Timer:
    def __init__(self):
        self.t = 0
        self.active = True
    def start(self):
        self.t = time.time()
    def end(self):
        self.active = False
        return time.time() - self.t
    def restart(self):
        self.t = time.time()
        self.active = True

var_ptn = re.compile(r'x(\d+)_(\d+)')
num_ptn = re.compile(r'\d+\.\d*|\d*\.\d+|\d+')
