import sympy as sp
from collections import deque
import frozendict
import itertools
import re

class SymbolNode:
    def __init__(self):
        self.parent = None
        self.children = []
        self.symbol = ''
        self.direct_symmetry = {}
        self.pow_symmetry = {}
        self.com_symmetry = frozendict.frozendict()
        self.level = 0

def is_integer(s):
    return re.fullmatch(r"^[-+]?\d+$", s) is not None

def build_expression_tree(expr_str):
    expr = sp.sympify(expr_str)
    root = SymbolNode()

    def traverse(node, depth, cur, parent):
        if node.is_Add:
            symbol = '+'
        elif node.is_Mul:
            symbol = '*'
        # elif node.is_Pow:
        #     symbol = '^'
        elif node.is_Symbol:
            symbol = node.name
        elif node.is_Number:
            symbol = str(node)
        else:
            symbol = str(node.func.__name__)

        if node.is_Number:
            cur.symbol = ''
        else:
            cur.symbol = symbol
        cur.level = depth
        if parent is not None:
            cur.parent = parent
            parent.children.append(cur)

        # recursion
        for arg in node.args:
            traverse(arg, depth + 1, SymbolNode(), cur)

        if len(cur.children) and cur.symbol != 'Pow':
            cur.direct_symmetry = set([child.symbol for child in cur.children if len(child.children) == 0 and child.symbol != ''])

            children_pow = [child for child in cur.children if child.symbol == 'Pow']
            exp_dict = {}
            for child in children_pow:
                exponent = child.children[1].symbol
                if exp_dict.get(exponent) is None:
                    exp_dict[exponent] = [child.children[0].symbol]
                else:
                    exp_dict[exponent].append(child.children[0].symbol)
            cur.pow_symmetry = exp_dict

            children_com = [child for child in cur.children if child.symbol != 'Pow' and len(child.children) > 0]
            com_dict = {}
            for (i, child) in enumerate(children_com):
                id = (child.symbol, len(child.direct_symmetry), frozenset([(key, len(value)) for key, value in child.pow_symmetry.items()]), child.com_symmetry)
                if com_dict.get(id) is None:
                    com_dict[id] = {i}
                else:
                    com_dict[id].add(i)
            cur.com_symmetry = frozendict.frozendict([(key, frozenset(value)) for key, value in com_dict.items()])

    traverse(expr, 1, root, None)
    
    return root

def generate_ordered_pairs(list_a, list_b):
    """Combine the full permutations of two equal-length lists into a list of tuples"""
    assert len(list_a) == len(list_b), "The input lists must have equal lengths"
    
    result = []
    for p in itertools.permutations(list_b):
        paired = list(zip(list_a, p))
        result.append(paired)
    return result

def multi_list_cartesian(lists):
    """Compute the Cartesian product of multiple lists and merge the elements of the sublists."""
    # Generate all possible combinations of sublists.
    combinations = itertools.product(*lists)
    # Merge the sublists within each combination.
    return [ 
        [item for sublist in combo for item in sublist]
        for combo in combinations
    ]

def gen_expr_sym(root):

    symmetry = []
    upper_level_symmetry = []
    
    # First, generate the transpositions that can be directly produced.
    if len(root.direct_symmetry) > 1:
        symmetry.extend([[t] for t in itertools.combinations(root.direct_symmetry, 2)])
    for value in root.pow_symmetry.values():
        if len(value) > 1:
            symmetry.extend([[t] for t in itertools.combinations(value, 2)])
    children_com = [child for child in root.children if child.symbol != 'Pow' and len(child.children) > 0]
    for key, value in root.com_symmetry.items():
        if len(value) > 1:
            children_subset = [children_com[j] for j in value]
            upper_level_symmetry.extend([[t] for t in itertools.combinations(children_subset, 2)])

    queue = deque(upper_level_symmetry) #[[(x_{1,1}, x_{1,2})]]
    has_child = True
    while queue and has_child:
        level_size = len(queue)
        current_level_symmetry = []
        has_child = False
        for _ in range(level_size):
            cur_chained_tuples = queue.popleft()  # [(x_{1,1}, x_{1,2})]

            new_chained_tuples = []
            for cur_tuple in cur_chained_tuples:
                if isinstance(cur_tuple[0], str):
                    new_chained_tuples.append([[cur_tuple]])
                else:
                    has_child = True
                    assert isinstance(cur_tuple[0], SymbolNode)
                    plain_base = [list(cur_tuple[0].direct_symmetry)]
                    plain_base.extend([cur_tuple[0].pow_symmetry[key] for key in sorted(cur_tuple[0].pow_symmetry.keys())])
                    plain_perm = [list(cur_tuple[1].direct_symmetry)]
                    plain_perm.extend([cur_tuple[1].pow_symmetry[key] for key in sorted(cur_tuple[1].pow_symmetry.keys())])
                    assert len(plain_base) == len(plain_perm)
                    to_be_producted = []
                    for i in range(0, len(plain_base)):
                        perm_len = len(plain_base[i])
                        assert perm_len == len(plain_perm[i])
                        to_be_producted.append(generate_ordered_pairs(plain_base[i],plain_perm[i]))

                    children_com_0 = [child for child in cur_tuple[0].children if child.symbol != 'Pow' and len(child.children) > 0] #[x_{2,1}, x_{2,2}]
                    children_com_1 = [child for child in cur_tuple[1].children if child.symbol != 'Pow' and len(child.children) > 0] #[x_{2,3}, x_{2,4}]
                    for key in cur_tuple[0].com_symmetry:
                        assert len(cur_tuple[0].com_symmetry[key]) == len(cur_tuple[1].com_symmetry[key]) 
                        to_be_producted.append(generate_ordered_pairs([children_com_0[j] for j in cur_tuple[0].com_symmetry[key]], [children_com_1[j] for j in cur_tuple[1].com_symmetry[key]])) #[[(x_{2,1}, x_{2,3}), （x_{2,2}, x_{2,4}）], [(x_{2,1}, x_{2,4}), （x_{2,2}, x_{2,3}）]]
        
                    new_chained_tuples.append(multi_list_cartesian(to_be_producted)) # after cartestian: [[(x_{2,1}, x_{2,3}), （x_{2,2}, x_{2,4}）], [(x_{2,1}, x_{2,4}), （x_{2,2}, x_{2,3}）]]
            current_level_symmetry.extend(multi_list_cartesian(new_chained_tuples))

        queue.extend(current_level_symmetry)
    symmetry.extend(queue)
    return symmetry

def gen_expr_sym_all(root):
    if not root or len(root.children) == 0:
        return []
    result = []
    queue = deque([root])
    while queue:
        node = queue.popleft()
        assert len(node.children) > 0
        result.extend(gen_expr_sym(node))
        queue.extend([child for child in node.children if child.symbol != 'Pow' and len(child.children) > 0])
    return result

def level_order(root):
    if not root:
        return []
    result = []
    queue = deque([root])
    while queue:
        level_size = len(queue)
        current_level = []
        for _ in range(level_size):
            node = queue.popleft()
            current_level.append(node.symbol)
            queue.extend(node.children)
        result.append(current_level)
    return result

if __name__ == "__main__":
    # test case
    expr1 = "x_1+x_2*x_3"
    print("expr:", expr1)
    root = build_expression_tree(expr1)
    print(level_order(root))
    print(gen_expr_sym_all(root))

    expr2 = "(x_11 * x_12 * x_13 + x_21 * x_22 * x_23) * (x_31 * x_32 * x_33 + x_41 * x_42 * x_43) * (x_51 * x_52 * x_53 + x_61 * x_62 * x_63)"
    print("\n expr:", expr2)
    root = build_expression_tree(expr2)
    print(level_order(root))
    print(gen_expr_sym_all(root))

    expr3 = "x + (y + z) + a * b * c + d * e * f"
    print("\n expr:", expr3)
    root = build_expression_tree(expr3)
    print(level_order(root))
    print(gen_expr_sym_all(root))

    expr4 = "x^2 + (y^3 + z^3)"
    print("\n expr:", expr4)
    root = build_expression_tree(expr4)
    print(level_order(root))
    print(gen_expr_sym_all(root))

    expr5 = "(x + ((y * z) * w))"
    print("\n expr:", expr5)
    root = build_expression_tree(expr5)
    print(level_order(root))
    print(gen_expr_sym_all(root))

    expr6 = "(x * (1 - y))"
    print("\n expr:", expr6)
    root = build_expression_tree(expr6)
    print(level_order(root))
    print(gen_expr_sym_all(root))

    expr6 = "(((x_1))*(1-((x_2)))-((x_3))*((x_4)))"
    print("\n expr:", expr6)
    root = build_expression_tree(expr6)
    print(level_order(root))
    print(gen_expr_sym_all(root))

