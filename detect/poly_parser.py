import re
import bisect
import sympy as sp
import frozendict
import itertools
from collections import deque
from find_sym import build_expression_tree, gen_expr_sym_all, generate_ordered_pairs, multi_list_cartesian, SymbolNode 

def replace_underscores(s, cnt):
    """Replace underscores in the string with \( x_1, x_2, \ldots \) in order, and return the result and the number of underscores."""
    counter = cnt
    
    def replacer(match):
        nonlocal counter
        counter += 1
        return f"x_{counter}"
    
    new_str = re.sub(r'_', replacer, s)
    return new_str, counter

def generate_all_subsets(tuples_list):
    subsets = set()
    n = len(tuples_list)
    for k in range(n + 1):
        for combo in itertools.combinations(tuples_list, k):
            subsets.add(set(combo))
    return subsets

def remove_elements_inplace(lst, indices):
    for idx in sorted(set(indices), reverse=True):
        if idx < len(lst):
            del lst[idx]
    return lst

def find_in_nested_list(var_idx_groups, var_idx_target):
    for i, var_idx_group in enumerate(var_idx_groups):
        j = bisect.bisect_left(var_idx_group, var_idx_target) # assume var_idx_group is sorted
        if j < len(var_idx_group) and var_idx_group[j] == var_idx_target:
            return (i, j)

class DSU:
    def __init__(self):
        self.parent = {}  # Store the parent node of each node.
        self.rank = {}    # Store the rank (depth of the tree) of each node.

    def find(self, x):
        # Find the root node and perform path compression.
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])  # Path compression
        return self.parent[x]

    def union(self, x, y):
        x_root = self.find(x)
        y_root = self.find(y)
        if x_root == y_root:
            return  # Already in the same set.
        
        # Merge by rank.
        if self.rank[x_root] < self.rank[y_root]:
            self.parent[x_root] = y_root
        else:
            self.parent[y_root] = x_root
            if self.rank[x_root] == self.rank[y_root]:
                self.rank[x_root] += 1

    def add(self, x):
        # Add a new element to the disjoint-set.
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 1

class PolyNode:
    def __init__(self):
        self.parent = None
        self.next = None
        self.prev = None
        self.children = []
        self.expressions = {}
        self.expr_range = [0, 0]
        self.vars = []
        self.companion_dict = {}
        self.join_groups = []
        self.level = 0
        self.child_symmetry = frozendict.frozendict()
        self.expr_node = None

class PolynomialParser:
    def __init__(self):
        self.var_pattern = re.compile(r'([a-z]+\d*_\d+)')
        self.all_vars = set()

    def parse(self, poly_str):
        stack = []
        root = None
        i = 0
        n = len(poly_str)
        num_left_parenthese = 0
        idx_left_parenthese = -1

        # [[(x1)+(x2)+(x3)+(x4)]+[(x5)+(x6)+(x7)+(x8)]+[(x9)+(x10)+(x11)+(x12)]]
        # [[[(x1)+(x2)]+[(x3)+(x4)]]+[[(x5)+(x6)]+[(x7)+(x8)]]+[[(x9)+(x10)]+[(x11)+(x12)]]]
        while i < n:
            if poly_str[i] == '[':
                cur = PolyNode()
                if stack:
                    cur.parent = stack[-1]
                    cur.level += cur.parent.level + 1
                    if stack[-1].children:
                        stack[-1].children[-1].next = cur
                        cur.prev = stack[-1].children[-1]
                    stack[-1].children.append(cur)
                    if len(cur.parent.children) == 1: 
                        cur.expr_range[0] = cur.parent.expr_range[0]
                    else:
                        assert len(cur.parent.children) > 1
                        cur.expr_range[0] = cur.prev.expr_range[1]
                stack.append(cur)
            elif poly_str[i] == ']':
                cur = stack.pop()
                if len(cur.expressions) == 1:
                    print("cur.expressions = ", cur.expressions.keys())
                    self.all_vars.update({var for var_tuple in cur.vars for var in var_tuple})

                    first_var_tuple = cur.vars[0]
                    l = len(first_var_tuple)
                    first_emergence = [{first_var_tuple[j]: 0} for j in range(0, l)]
                    # print("init first_emergence = ", first_emergence)
                    companion_pattern = [[0]*len(cur.vars) for _ in range(0, l)]
                    for k, var_tuple in enumerate(cur.vars[1:]):
                        for j in range(0, l):
                            first_emergence_idx = first_emergence[j].get(var_tuple[j])
                            if first_emergence_idx is not None:      
                                companion_pattern[j][k+1] = first_emergence_idx
                            else:
                                first_emergence[j][var_tuple[j]] = k + 1
                                companion_pattern[j][k+1] = k + 1
                    # print("first_emergence = ", first_emergence)    

                    join_group_dsu = DSU()
                    for k in range(0, len(cur.vars)):
                        join_group_dsu.add(k)
                        for j in range(0, l):
                            if companion_pattern[j][k] != k:
                                join_group_dsu.union(k, companion_pattern[j][k])
                    # print("join_group_dsu = ", join_group_dsu.parent)

                    companion_dict = {}
                    for j, pattern in enumerate(companion_pattern):
                        pattern = tuple(pattern)
                        if pattern not in companion_dict:
                            companion_dict[pattern] = []
                        companion_dict[pattern].append(j)
                    cur.companion_dict = companion_dict

                    join_groups = {}
                    for k in range(0, len(cur.vars)):
                        idx = join_group_dsu.find(k)
                        values = list(companion_dict.values())
                        if join_groups.get(idx) is None:
                            join_groups[idx] = [set() for _ in range(0, len(values))]
                        join_group = join_groups[idx]
                        for m, js in enumerate(values):
                            join_group[m].add(tuple([cur.vars[k][j] for j in js]))
                    cur.join_groups = list(join_groups.values())

                    cur.expr_range[1] = cur.expr_range[0] + len(cur.vars)
                    join_groups_size = {}
                    for join_group in cur.join_groups:
                        jgidx = tuple([(len(s), len(list(s)[0])) for s in join_group])
                        if join_groups_size.get(jgidx) is None:
                            join_groups_size[jgidx] = 1
                        else:
                            join_groups_size[jgidx] += 1
                    cur.child_symmetry = frozendict.frozendict(join_groups_size)
                    print("join_groups_size = ", join_groups_size)
  
                    cur.expr_node = SymbolNode()
                    cur.expr_node.symbol = 'R'
                    cur.expr_node.level = -1 # cur has no children, it gets how many expr node by len(vars)
                    first_expr, first_cnt = replace_underscores(list(cur.expressions.keys())[0], 0)
                    first_repre_node = build_expression_tree(first_expr)
                    if len(cur.vars) == 1:
                        cur.expr_node = first_repre_node
                    else:
                        second_repre_node = build_expression_tree(replace_underscores(list(cur.expressions.keys())[0], first_cnt)[0])
                        cur.expr_node.children = [first_repre_node, second_repre_node] # not only 2 nodes, but do that for convenience, and the variables can be replaced to generate other symmetry
                        if len(first_repre_node.children) == 0:
                            cur.expr_node.direct_symmetry = set([child.symbol for child in cur.expr_node.children])
                        elif first_repre_node.symbol == 'Pow':
                            exponent = first_repre_node.children[1].symbol  # the exponent is the same
                            cur.expr_node.pow_symmetry[exponent] = [first_repre_node.children[0].symbol, second_repre_node.children[1].symbol]
                        else:
                            id = (first_repre_node.symbol, len(first_repre_node.direct_symmetry), frozenset([(key, len(value)) for key, value in first_repre_node.pow_symmetry.items()]), first_repre_node.com_symmetry)
                            cur.expr_node.com_symmetry = frozendict.frozendict({id: frozenset({0, 1})})

                elif len(cur.expressions) > 1: # TODO: If the expression is not unique, what should we do? The union operation may introduce such a situation.
                    None
                else: # TODO: If the parent node has its own expression, what should we do? This situation may be introduced by the union operation.
                    cur.expr_range[1] = cur.children[-1].expr_range[1]
                    
                    com_dict = {}
                    for (idx, child) in enumerate(cur.children):
                        l = len(child.children)
                        id = (l, child.child_symmetry)
                        v = com_dict.get(id)
                        if v is None:
                            com_dict[id] = {idx}
                        else:
                            com_dict[id].add(idx)
                    cur.child_symmetry = frozendict.frozendict({key: frozenset(value) for key, value in com_dict.items()})
                if not cur.parent:
                    root = cur
            # After reduceByKey, there will be no map operation, which ensures that () will not contain []
            elif poly_str[i] == '(':
                if num_left_parenthese == 0:
                    idx_left_parenthese = i
                num_left_parenthese += 1
            elif poly_str[i] == ')':
                num_left_parenthese -= 1
                if num_left_parenthese == 0 and idx_left_parenthese != -1 and len(stack) > 0:
                    # Extract the complete expression.
                    expr = poly_str[idx_left_parenthese+1:i]
                    vars = self.var_pattern.findall(expr)
                    # Remove the variable symbols.
                    expr = self.var_pattern.sub('_', expr)
                    stack_var_idx = len(stack[-1].vars)
                    stack_var_set = stack[-1].expressions.get(expr)
                    if stack_var_set:
                        stack_var_set.add(stack_var_idx)
                    else:
                        stack[-1].expressions[expr] = {stack_var_idx}
                    stack[-1].vars.append(tuple(vars))
                    idx_left_parenthese = -1
            i += 1

        return root
    
    def gen_poly_sym(self, root):
        symmetry = []
        symmetry_to_expand = []
        # First, generate the transpositions that can be directly generated.
        if len(root.expressions) == 1:
            print("print last-level symmetry")
            if len(root.join_groups) == len(root.vars):
                print("does not have join group: ")
                symmetry_tmp = gen_expr_sym_all(root.expr_node)
                assert len(root.companion_dict) == 1
                if len(root.vars) == 1:
                    var_dict = {f"x_{i+1}": var for i, var in enumerate(list(root.vars[0]))} 
                    symmetry.extend([[tuple(sorted([var_dict[cur_tuple[0]], var_dict[cur_tuple[1]]])) for cur_tuple in chained_tuples] for chained_tuples in symmetry_tmp])
                for t in itertools.combinations(root.vars, 2):
                    var_dict = {f"x_{i+1}": var for i, var in enumerate(list(t[0]) + list(t[1]))}                
                    symmetry.extend([[tuple(sorted([var_dict[cur_tuple[0]], var_dict[cur_tuple[1]]])) for cur_tuple in chained_tuples] for chained_tuples in symmetry_tmp])
            else: # TODO: what about join group
                print("has join group: ")
                symmetry_expr_node_placeholder = [[tuple([int(re.fullmatch(r'^x_(\d+)$', symbol).group(1)) - 1
 for symbol in cur_tuple]) for cur_tuple in chained_tuples] for chained_tuples in gen_expr_sym_all(root.expr_node.children[0])]
                in_comp_chained_tuples_placeholder = []
                indice_to_remove = []
                has_non_join_column = False
                for pattern, companion in root.companion_dict.items():
                    if len(set(pattern)) == len(root.vars) and len(root.companion_dict) > 2:
                        has_non_join_column = True
                    companion_set = set(companion)
                    companion_chained_tuples = []
                    for idx, chained_tuples in enumerate(symmetry_expr_node_placeholder):
                        is_in_companion = True
                        for cur_tuple in chained_tuples:
                            for symbol in cur_tuple:
                                if symbol not in companion_set:
                                    is_in_companion = False
                                    break
                            if not is_in_companion:
                                break
                        if is_in_companion:
                            indice_to_remove.append(idx)
                            companion_chained_tuples.append(chained_tuples)
                    in_comp_chained_tuples_placeholder.append(companion_chained_tuples)
                
                remove_elements_inplace(symmetry_expr_node_placeholder, indice_to_remove) # avoid repetitive output
                
                companion_dict_values = list(root.companion_dict.values())
                var_idx_to_companion_idx = {idx: i for i, indice in enumerate(companion_dict_values) for idx in indice}
                companion_idx_to_var_idx_group = {i: indice for i, indice in enumerate(companion_dict_values)}
                for join_group in root.join_groups:
                    for (companion_chained_tuples, companion_idx, companion_set) in zip(in_comp_chained_tuples_placeholder, root.companion_dict.values(), join_group):
                        for companion in companion_set:  # Within the same join group, the same companion set, and the same tuple, perform the interchange.
                            symbol_dict =  {symbol_idx: symbol for symbol_idx, symbol in zip(companion_idx, companion)}
                            symmetry.extend([[tuple(sorted([symbol_dict[symbol_idx] for symbol_idx in cur_tuple])) for cur_tuple in chained_tuples] for chained_tuples in companion_chained_tuples])
                    # print("for join group = ", join_group, ", before companion exchange, current symmetry = ", symmetry)    
                    
                    if not has_non_join_column:
                        for companion_set in join_group:  # Within the same join group and the same companion set, perform the interchange.
                            for t in itertools.combinations(companion_set, 2):
                                symmetry.append([tuple(sorted(cur_tuple)) for cur_tuple in list(zip(*t))]) # Here, some symmetries arising from partial permutations are omitted and require further investigation.
                        # print("for join group = ", join_group, ", before companion set exchange, current symmetry = ", symmetry) 

                        join_group_size = [(len(s), len(list(s)[0])) for s in join_group]
                        for chained_tuples in symmetry_expr_node_placeholder: # Perform interchanges between different companion sets within the same join group.
                            # print("chained_tuples = ", chained_tuples)
                            bind_dict = {}
                            is_comp_consist = True                          
                            for cur_tuple in chained_tuples:
                                idx_0 = var_idx_to_companion_idx[cur_tuple[0]]
                                idx_1 = var_idx_to_companion_idx[cur_tuple[1]]
                                if bind_dict.get(idx_0) is None and bind_dict.get(idx_1) is None:
                                    assert idx_0 != idx_1
                                    bind_dict[idx_0] = idx_1
                                    bind_dict[idx_1] = idx_0
                                else:
                                    if bind_dict.get(idx_0) != idx_1 or bind_dict.get(idx_1) != idx_0:
                                        is_comp_consist = False
                                        break
                            if is_comp_consist:
                                is_group_size_match = True
                                for key, value in bind_dict.items():
                                    if join_group_size[key] != join_group_size[value]:
                                        is_group_size_match = False
                                        break
                                if is_group_size_match:
                                    bind_groups = [sorted(s) for s in {frozenset({key, value}) for key, value in bind_dict.items()}] # bind companion idx
                                    bind_group_to_real_tuples = {frozenset(bind_group): itertools.chain.from_iterable(generate_ordered_pairs(list(join_group[bind_group[0]]), list(join_group[bind_group[1]]))) for bind_group in bind_groups}
                                    bind_group_to_dummy_tuples = {}
                                    for cur_tuple in chained_tuples:
                                        bind_group = frozenset({var_idx_to_companion_idx[var_idx] for var_idx in cur_tuple})
                                        if bind_group_to_dummy_tuples.get(bind_group) is None:
                                            bind_group_to_dummy_tuples[bind_group] = [cur_tuple]
                                        else:
                                            bind_group_to_dummy_tuples[bind_group].append(cur_tuple)

                                    real_chained_tuples = []
                                    for bind_group, part_chained_tuples in bind_group_to_dummy_tuples.items():
                                        real_tuples = bind_group_to_real_tuples[bind_group]
                                        # print("bind_group = ", bind_group, ", real_tuples = ", real_tuples)
                                        bind_group = sorted(bind_group)
                                        var_idx_groups = [companion_idx_to_var_idx_group[companion_idx] for companion_idx in bind_group] # var_idx_group[i] is sorted by var idx, var_idx_group is sorted by companion_idx
                                        real_part_symmetry = []
                                        for real_tuple in real_tuples: # real_tuples: [(join_group[bind_group[0]][i], join_group[bind_group[1]][i]) for i in ...], real tuple is sorted by companion_idx
                                            real_part_chained_tuples = []
                                            # print("bind_group = ", bind_group, ", real_tuple = ", real_tuple)
                                            for dummy_tuple in part_chained_tuples:
                                                real_part_chained_tuples.append(tuple(sorted([real_tuple[i][j] for i, j in [find_in_nested_list(var_idx_groups, var_idx) for var_idx in dummy_tuple]])))
 
                                            real_part_symmetry.append(real_part_chained_tuples)
                                            # print("real_part_symmetry = ", real_part_symmetry)
                                        real_chained_tuples.append(real_part_symmetry)
                                    real_chained_tuples = multi_list_cartesian(real_chained_tuples)
                                    for chained_tuples in real_chained_tuples:
                                        chained_tuples.sort()
                                    symmetry.extend(real_chained_tuples)

                # print("before inter-join-group symmetrym, cur symmetry = ", symmetry)

                # inter-join-group symmetry. Here, only a subset of the symmetry results is generated.
                join_groups_size = {}
                for join_group_idx, join_group in enumerate(root.join_groups):
                    jgidx = tuple([(len(s), len(list(s)[0])) for s in join_group])
                    if join_groups_size.get(jgidx) is None:
                        join_groups_size[jgidx] = {join_group_idx}
                    else:
                        join_groups_size[jgidx].add(join_group_idx)
                for join_group_indice in join_groups_size.values():
                    for join_group in itertools.combinations([root.join_groups[i] for i in join_group_indice], 2):
                        chained_tuples = []
                        for companion_set in zip(*join_group):
                            for companion in zip(*companion_set):
                                chained_tuples.extend([tuple(sorted(cur_tuple)) for cur_tuple in zip(*companion)])
                        chained_tuples.sort()
                        symmetry.append(chained_tuples)

        elif len(root.expressions) > 1:
            None # TODO
        else:
            # print("print upper-level symmetry")
            upper_level_symmetry = []
            # print("root.child_symmetry = ", root.child_symmetry)
            for key, value in root.child_symmetry.items():
                if len(value) > 1:
                    children_subset = [root.children[j] for j in value]
                    upper_level_symmetry.extend([[t] for t in itertools.combinations(children_subset, 2)])

            # print("upper_level_symmetry = ", upper_level_symmetry)
            queue = deque(upper_level_symmetry)
            has_child = True
            while queue and has_child:
                level_size = len(queue)
                current_level_symmetry = []
                has_child = False
                for _ in range(level_size):
                    cur_chained_tuples = queue.popleft()

                    new_chained_tuples = []
                    for cur_tuple in cur_chained_tuples:
                        has_child = has_child or len(cur_tuple[0].children) > 0
                        to_be_producted = []
                        if len(cur_tuple[0].children) > 0:
                            children_com_0 = [child for child in cur_tuple[0].children]
                            children_com_1 = [child for child in cur_tuple[1].children]
                            for key in cur_tuple[0].child_symmetry:
                                assert len(cur_tuple[0].child_symmetry[key]) == len(cur_tuple[1].child_symmetry[key]) 
                                to_be_producted.append(generate_ordered_pairs([children_com_0[j] for j in cur_tuple[0].child_symmetry[key]], [children_com_1[j] for j in cur_tuple[1].child_symmetry[key]]))
                        else:
                            if len(cur_tuple[0].join_groups) == len(cur_tuple[0].vars) and len(cur_tuple[1].join_groups) == len(cur_tuple[1].vars):
                                plain_base = range(cur_tuple[0].expr_range[0], cur_tuple[0].expr_range[1])
                                plain_perm = range(cur_tuple[1].expr_range[0], cur_tuple[1].expr_range[1])
                                perm_len = len(plain_base)
                                assert perm_len == len(plain_perm)
                                to_be_producted.append(generate_ordered_pairs(plain_base,plain_perm))
                            else:
                                None # TODO: join group

                        new_chained_tuples.append(multi_list_cartesian(to_be_producted))
                    current_level_symmetry.extend(multi_list_cartesian(new_chained_tuples))

                queue.extend(current_level_symmetry)  # Directly add all child nodes without expansion.
            symmetry_to_expand.extend(queue)

        return symmetry, symmetry_to_expand
    
    def gen_poly_sym_all(self, root):
        if not root:
            return []
        result = []
        result_to_expand = []
        queue = deque([root])
        while queue:
            node = queue.popleft()
            node_symmetry, node_symmetry_to_expand = self.gen_poly_sym(node)
            # print(node_symmetry)
            result.extend(node_symmetry)
            result_to_expand.extend(node_symmetry_to_expand)
            queue.extend([child for child in node.children])
        return result, result_to_expand
    
    def print_structure(self, node, indent=0):
        print('  ' * indent + f'Level {node.level}:')
        if node.expressions:
            print('  ' * (indent+1) + 'Exprs:', node.expressions)
        if node.vars:
            print('  ' * (indent+1) + 'Vars:', node.vars)
        if node.join_groups:
            print('  ' * (indent+1) + 'Join Groups:', node.join_groups)
        if node.companion_dict:
            print('  ' * (indent+1) + 'Companion Dict:', node.companion_dict)
        if node.expr_range:
            print('  ' * (indent+1) + 'Expr Range:', node.expr_range) 
        for child in node.children:
            self.print_structure(child, indent+1)


if __name__ == "__main__":
    # test case
    parser = PolynomialParser()
    # test_case = "[[[(x_1)+(x_2)]+[(x_3)+(x_4)]]+[[(x_5)+(x_6)]+[(x_7)+(x_8)]]+[[(x_9)+(x_10)]+[(x_11)+(x_12)]]]"
    # test_case = "[[((x_1+y_1)*z_1)+((x_1+y_1)*z_2)+((x_2+y_2)*z_1)+((x_2+y_2)*z_2)]+[((x_3+y_3)*z_3)+((x_4+y_4)*z_4)]]"
    # test_case = "[[((x_1+y_1)*(z_1+w_1))+((x_1+y_1)*(z_2+w_2))+((x_2+y_2)*(z_1+w_1))+((x_2+y_2)*(z_2+w_2))]+[((x_3+y_3)*(z_3+w_3))+((x_4+y_4)*(z_4+w_4))]]"
    test_case = "[[((x_1+y_1)*(z_1+w_1)*q_1)+((x_1+y_1)*(z_2+w_2)*q_2)+((x_2+y_2)*(z_1+w_1)*q_3)+((x_2+y_2)*(z_2+w_2)*q_4)+((x_3+y_3)*(z_3+w_3)*q_5)+((x_3+y_3)*(z_4+w_4)*q_6)+((x_4+y_4)*(z_3+w_3)*q_7)+((x_4+y_4)*(z_4+w_4)*q_8)]+[((x_5+y_5)*(z_5+w_5)*q_9)+((x_6+y_6)*(z_6+w_6)*q_10)]]"
    # test_case = "[[(x1_1)+(x1_2)]+[(x1_3)+(x1_4)]]"
    root = parser.parse(test_case)
    parser.print_structure(root)
    symmetry, symmetry_to_expand = parser.gen_poly_sym_all(root)
    print("symmetry = ", symmetry)
    
