from sympy import *
from sympy.parsing.sympy_parser import parse_expr
import re
import time
from math import factorial

def extract_variable_names(equation_strings):
    """extract variables (x1, x2, x3, ...)"""
    var_set = set()
    for eq in equation_strings:
        matches = re.findall(r'x\d+', eq)
        var_set.update(matches)
    return sorted(var_set, key=lambda x: int(x[1:]))

def string_to_poly(equation_str):
    equation_str = equation_str.replace('^', '**')
    
    if '=' in equation_str:
        parts = equation_str.split('=', 1)
        left = parts[0].strip()
        right = parts[1].strip()
        try:
            left_expr = parse_expr(left)
            right_expr = parse_expr(right)
            return left_expr - right_expr
        except:
            return parse_expr(f"{left} - {right}")
    else:
        return parse_expr(equation_str)

def solve_with_groebner(equations, variables):
    if not equations or not variables:
        return []
    
    try:
        start = time.time()
        G = groebner(equations, *variables, order='lex')
        elapsed = time.time() - start
        print(f"Gröbner base computation (take: {elapsed:.2f} second)")
    except Exception as e:
        print(f"fail to compute Gröbner: {e}")
        return []
    
    solutions = [{}]
    for idx in range(len(variables) - 1, -1, -1):
        var = variables[idx]
        
        candidates = [g for g in G 
                     if var in g.free_symbols and 
                     set(g.free_symbols).issubset(set(variables[idx:]))]
        
        if not candidates:
            continue
            
        eq_to_solve = candidates[0]
        
        new_solutions = []
        for sol in solutions:
            eq_sub = eq_to_solve.subs(sol)
            if not eq_sub.has(var):
                if abs(eq_sub) < 1e-10:
                    new_solutions.append(sol)
                continue
                
            roots = solve(eq_sub, var)
            if not roots:
                continue
                
            for r in roots:
                extended_sol = sol.copy()
                extended_sol[var] = r
                new_solutions.append(extended_sol)
                
        solutions = new_solutions
    
    return solutions

def solve_equations_from_strings(equation_strings):
    var_names = extract_variable_names(equation_strings)
    if not var_names:
        raise ValueError("no variables extracted from equations")
    
    variables = symbols(' '.join(var_names))
    
    equations = []
    for s in equation_strings:
        eq = string_to_poly(s)
        equations.append(eq)
    
    return solve_with_groebner(equations, list(variables))

if __name__ == "__main__":
    eq_strings_2 = [
        "x1 + x2 = 3",
        "3*x1**2 + x2**2 = 13"
    ]
    
    eq_strings_3 = [
        "x1 + x2 + x3 = 6",
        "x1**2 + x2**2 + x3**2 = 14",
        "x1**3 + x2**3 + x3**3 = 36"
    ]
    
    eq_strings_5 = [
        "x1 + x2 + x3 + x4 + x5 = 15",
        "x1**2 + x2**2 + x3**2 + x4**2 + x5**2 = 55",
        "x1**3 + x2**3 + x3**3 + x4**3 + x5**3 = 225",
        "x1**4 + x2**4 + x3**4 + x4**4 + x5**4 = 979",
        "x1**5 + x2**5 + x3**5 + x4**5 + x5**5 = 4425"
    ]
    
    for i, eq_strings in enumerate([eq_strings_2, eq_strings_3, eq_strings_5]):
        print(f"\n{'='*30} test {i+1} (number of variables: {len(extract_variable_names(eq_strings))}) {'='*30}")
        solutions = solve_equations_from_strings(eq_strings)
        
        print(f"\nthe first 3 valid solutions (totally {len(solutions)}):")
        for j in range(min(3, len(solutions))):
            print(f"solution {j+1}: {solutions[j]}")
        
        print("\nverify the solutions:")
        for j in range(min(3, len(solutions))):
            sol = solutions[j]
            for eq in eq_strings:
                eq_expr = string_to_poly(eq)
                val = eq_expr.subs(sol)
                print(f"  {eq} -> {val.evalf()}")