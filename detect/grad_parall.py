import numpy as np
import random
import time
from multiprocessing import Pool, cpu_count
import functools
from typing import List, Tuple, Callable
import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)

def generate_equations(n: int, m: int, coeff_range: Tuple[float, float] = (-0.1, 0.1)) -> Tuple[List[dict], List[str]]:
    """
    generate random non-linear equations
    return：
        equations_coeffs
        equations_str
    """
    equations_coeffs = []
    equations_str = []
    
    min_coeff, max_coeff = coeff_range

    for eq_idx in range(n):
        equation_info = {
            'linear_terms': [],
            'cross_terms': [],
            'higher_terms': []
        }
        terms_str = []

        # linear terms
        for j in range(m):
            coeff = round(random.uniform(min_coeff, max_coeff), 3)
            equation_info['linear_terms'].append((j, coeff))
            terms_str.append(f"{coeff:+.3f} * x_{j+1}")

        # cross terms
        num_cross = random.randint(0, m//2)
        for _ in range(num_cross):
            j, k = random.sample(range(m), 2)
            coeff = round(random.uniform(min_coeff/10, max_coeff/10), 3)  # smaller coeffs
            equation_info['cross_terms'].append((j, k, coeff))
            terms_str.append(f"{coeff:+.3f} * x_{j+1} * x_{k+1}")

        # high order terms
        num_higher = random.randint(0, m//2)
        for _ in range(num_higher):
            j = random.randint(0, m-1)
            power = random.randint(2, 3)
            coeff = round(random.uniform(min_coeff/100, max_coeff/100), 3)  # smaller coeffs
            equation_info['higher_terms'].append((j, power, coeff))
            terms_str.append(f"{coeff:+.3f} * x_{j+1}^{power}")

        equations_coeffs.append(equation_info)
        equations_str.append(" + ".join(terms_str) + " = 0")

    return equations_coeffs, equations_str

def compute_equation_value(equation_info: dict, x: np.ndarray) -> float:
    value = 0.0
    
    try:
        for j, coeff in equation_info['linear_terms']:
            term_val = coeff * x[j]
            if not np.isfinite(term_val):
                return np.inf
            value += term_val
        
        for j, k, coeff in equation_info['cross_terms']:
            term_val = coeff * x[j] * x[k]
            if not np.isfinite(term_val):
                return np.inf
            value += term_val
        
        for j, power, coeff in equation_info['higher_terms']:
            # restrict the range for x[j]
            x_val = np.clip(x[j], -100, 100)
            term_val = coeff * (x_val ** power)
            if not np.isfinite(term_val):
                return np.inf
            value += term_val
            
    except (OverflowError, ValueError):
        return np.inf
    
    return value

def compute_equation_gradient(equation_info: dict, x: np.ndarray) -> np.ndarray:
    grad = np.zeros_like(x)
    
    try:
        for j, coeff in equation_info['linear_terms']:
            grad[j] += coeff
        
        for j, k, coeff in equation_info['cross_terms']:
            term1 = coeff * x[k]
            term2 = coeff * x[j]
            if np.isfinite(term1) and np.isfinite(term2):
                grad[j] += term1
                grad[k] += term2
            else:
                grad[j] += 0
                grad[k] += 0
        
        for j, power, coeff in equation_info['higher_terms']:
            # restrict the range for x[j]
            x_val = np.clip(x[j], -100, 100)
            term_val = coeff * power * (x_val ** (power - 1))
            if np.isfinite(term_val):
                grad[j] += term_val
                
    except (OverflowError, ValueError):
        return np.zeros_like(x)
    
    return grad

def parallel_objective_and_gradient(x: np.ndarray, equations_coeffs: List[dict], 
                                   num_processes: int = None) -> Tuple[float, np.ndarray]:
    n = len(equations_coeffs)
    
    if num_processes is None:
        num_processes = min(cpu_count(), n)
    
    with Pool(processes=num_processes) as pool:
        # compute function values
        compute_value_func = functools.partial(compute_equation_value, x=x)
        f_values = pool.map(compute_value_func, equations_coeffs)
        
        if any(not np.isfinite(f) for f in f_values):
            return np.inf, np.zeros_like(x)
        
        # compute object value
        objective_value = np.sum(np.array(f_values) ** 2)
        
        # compute grads
        compute_grad_func = functools.partial(compute_equation_gradient, x=x)
        gradients = pool.map(compute_grad_func, equations_coeffs)
    
    total_gradient = np.zeros_like(x)
    for i, grad in enumerate(gradients):
        total_gradient += 2 * f_values[i] * grad
    
    if not np.all(np.isfinite(total_gradient)):
        return objective_value, np.zeros_like(x)
    
    return objective_value, total_gradient

def gradient_descent(equations_coeffs: List[dict], m: int, 
                    max_iter: int = 1000, tol: float = 1e-6, 
                    learning_rate: float = 0.001,
                    num_processes: int = None,
                    clip_value: float = 1.0) -> Tuple[np.ndarray, float, float]:
    """
    solving equations using GD
    return:
        solution
        final_loss
        duration
    """
    # init guest
    x = np.random.rand(m) * 0.2 - 0.1
    
    # record loss history
    losses = []
    
    start_time = time.time()
    
    for i in range(max_iter):
        loss, grad = parallel_objective_and_gradient(x, equations_coeffs, num_processes)
        
        if not np.isfinite(loss) or loss > 1e10:
            print(f"iter {i}: value unstable, reinit")
            x = np.random.rand(m) * 0.2 - 0.1
            losses.append(1e10)
            continue
            
        losses.append(loss)
        
        grad_norm = np.linalg.norm(grad)
        if grad_norm > clip_value:
            grad = grad * (clip_value / grad_norm)
        
        # update
        x = x - learning_rate * grad
        
        x = np.clip(x, -10, 10)
        
        if loss < tol:
            print(f"converge at iteration {i} with loss: {loss:.6f}")
            break
            
        if i % 100 == 0:
            print(f"iteration {i}, loss: {loss:.6f}, grad norm: {grad_norm:.6f}")
    
    duration = time.time() - start_time
    
    return x, loss, duration

def solve_equations_parallel(n: int, m: int, max_iter: int = 1000, 
                            tol: float = 1e-6, learning_rate: float = 0.001,
                            num_processes: int = None,
                            coeff_range: Tuple[float, float] = (-0.1, 0.1)) -> Tuple[np.ndarray, float, float, List[dict], List[str]]:
    """
    solving in parallel
    return:
        solution
        loss
        duration
        equations_coeffs
        equations_str
    """
    equations_coeffs, equations_str = generate_equations(n, m, coeff_range)
    
    solution, loss, duration = gradient_descent(
        equations_coeffs, m, max_iter, tol, learning_rate, num_processes
    )
    
    return solution, loss, duration, equations_coeffs, equations_str

if __name__ == "__main__":
    np.random.seed(42)
    random.seed(42)
   
    ma = [1000, 3000, 10000, 30000, 100000, 300000]
    for m in ma: # number of equations
        n = 100   # number of variables
    
        num_cores = cpu_count()
        print(f"{num_cores} cores detected")
        
        try:
            solution, loss, duration, equations_coeffs, equations_str = solve_equations_parallel(
                n, m, max_iter=10000000, learning_rate=0.001,
                num_processes=num_cores, coeff_range=(-0.1, 0.1)
            )
            
            print(f"\nnum of equations: {n}, num of variables: {m}")
            print(f"time: {duration:.6f} s")
            print(f"final loss: {loss:.6f}")
            print(f"\nsolution statistics:")
            print(f"  min: {np.min(solution):.6f}")
            print(f"  max: {np.max(solution):.6f}")
            print(f"  avg: {np.mean(solution):.6f}")
            print(f"  std: {np.std(solution):.6f}")
            
            # verify the quality of solutions
            equations_func = lambda x: np.array([
                compute_equation_value(eq, x) for eq in equations_coeffs
            ])
            residuals = equations_func(solution)
            max_residual = np.max(np.abs(residuals))
            print(f"max residual: {max_residual:.6f}")
            
        except Exception as e:
            print(f"error: {e}")
            import traceback
            traceback.print_exc()
