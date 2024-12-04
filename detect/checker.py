import torch
from utils import Timer

eps = 0.0001

class StrictChecker:
    def __init__(self, numVariable: int) -> None:
        self.x_0: torch.Tensor = None
        self.m: int = numVariable
        self.jac: torch.Tensor = torch.empty(0, self.m)
        self.cur_rid: int = 0
        # pivot_rids: {cid: rid}
        self.pivot_rids: dict = {}
        self.have_nan = False

    def set_initial(self, x_0: torch.Tensor) -> None:
        self.x_0 = x_0

    def clear_records(self) -> None:
        self.jac = torch.empty(0, self.m)
        self.cur_rid = 0
        self.pivot_rids = {}

    def check_leakage(self) -> bool:
        n, m = self.jac.shape
        # print(f"Caussian Elimination: m={m}, n={n}")
        # print(f"jac({self.jac.shape}): \n{self.jac}")
        cur_rid = self.cur_rid
        for cid, rid in self.pivot_rids.items():
            for k in range(self.cur_rid, n):
                self.jac[k] = self.jac[k] - self.jac[rid] * self.jac[k][cid]
        # print(f"in the middle: jac({self.jac.shape}): \n{self.jac}")
        for i in range(m):
            if cur_rid >= n:
                break
            if i in self.pivot_rids.keys():
                continue
            rmax, rid = torch.max(self.jac[cur_rid:, i].abs(), dim=0)
            rid = cur_rid + rid.item()
            if abs(rmax) < eps:
                # All zero column
                continue
            self.jac[[cur_rid, rid]] = self.jac[[rid, cur_rid]]
            rid = cur_rid
            cur_rid += 1
            self.pivot_rids[i] = rid
            self.jac[rid] = self.jac[rid] / self.jac[rid][i]
            for k in range(n):
                if abs(self.jac[k][i]) > eps and k != rid:
                    self.jac[k] = self.jac[k] - self.jac[rid] * self.jac[k][i]
        # erase all zero rows
        self.jac = self.jac[:cur_rid]
        self.cur_rid = cur_rid
        counts = ((torch.abs(self.jac) > eps).sum(dim=1) == 1).sum()
        have_leakage = bool(counts > 0)
        # print(f"end: jac({self.jac.shape}: \n{self.jac})")
        # print(f"pivot: {self.pivot_rids}")
        # print("---------------------------------")

        if torch.any(torch.isnan(self.jac)):
            print("Have NaN:")
            print(f"jac({self.jac.shape}): {self.jac}")
            self.have_nan = True
            # raise KeyError
        return have_leakage

    def add_function(self, val: torch.Tensor, check: bool = True):
        timer_debug = Timer()
        timer_debug.start()
        val.backward()
        time_debug = timer_debug.end()
        if torch.any(torch.isnan(self.x_0.grad)):
            print("??? grad have NaN ???")
            self.have_nan = True
            # raise KeyError
        self.jac = torch.cat((self.jac, self.x_0.grad.unsqueeze(0)), dim=0)
        self.x_0.grad.zero_()
        return time_debug
        # if check and self.check_leakage():
        #     # print("Leakage Found!")
        #     # print(f"jac({self.jac.shape}): {self.jac}")
        #     # raise KeyError
        #     return True
        # elif check:
        #     # print("No Leakage Found")
        #     return False
        # else:
        #     # print("skipped")
        #     return False
