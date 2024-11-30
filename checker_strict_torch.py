import torch

eps = 1e-6

class StrictChecker:
    def __init__(self, numVariable: int) -> None:
        self.x_0 = None
        self.m = numVariable
        self.jac = torch.empty(0, self.m)
        self.cur_rid = 0
        self.pivot_rid = torch.zeros(self.m, dtype=torch.int)

    def set_initial(self, x_0: torch.Tensor) -> None:
        self.x_0 = x_0

    def check_leakage(self) -> bool:
        n, m = self.jac.shape
        print(f"Caussian Elimination: m={m}, n={n}")
        cur_rid = self.cur_rid
        for i in range(m):
            if cur_rid >= n:
                break
            if self.pivot_rid[i]:
                rid = self.pivot_rid[i]
            else:
                rid = -1
                for k in range(cur_rid, n):
                    if abs(self.jac[k][i]) > eps:
                        rid = k
                        break
                if rid == -1:
                    # All zero column
                    continue
                if rid != cur_rid:
                    self.jac[[cur_rid, rid]] = self.jac[[rid, cur_rid]]
                rid = cur_rid
                self.pivot_rid[i] = rid
                self.jac[rid] = self.jac[rid] / self.jac[rid][i]
                cur_rid += 1
            for k in range(n):
                if abs(self.jac[k][i]) > eps and k != rid:
                    self.jac[k] = self.jac[k] - self.jac[rid] * self.jac[k][i]
        # erase all zero rows
        self.jac = self.jac[:cur_rid]
        self.cur_rid = cur_rid
        counts = ((torch.abs(self.jac) > eps).sum(dim=1) == 1).sum()
        have_leakage = counts > 0
        return have_leakage

    def add_function(self, f, check: bool = True) -> bool:
        val = f(self.x_0)
        val.backward()
        self.jac = torch.cat((self.jac, self.x_0.grad.unsqueeze(0)), dim=0)
        self.x_0.grad.zero_()
        return self.check_leakage() if check else False
        

if __name__ == "__main__":
    checker = StrictChecker(4)
    x0 = torch.tensor([42.0, 421, -234, 131], requires_grad=True)
    checker.set_initial(x_0=x0)
    g = lambda x: x.sum()
    have_leakage = checker.add_function(g, True)
    print(f"leakage: {have_leakage}")
    g = lambda x: (x**2).sum()
    have_leakage = checker.add_function(g, True)
    print(f"leakage: {have_leakage}")
    g = lambda x: (x**3).sum()
    have_leakage = checker.add_function(g, True)
    print(f"leakage: {have_leakage}")
    g = lambda x: (x**4).sum()
    have_leakage = checker.add_function(g, True)
    print(f"leakage: {have_leakage}")
