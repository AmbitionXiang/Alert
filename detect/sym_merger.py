import bisect
from collections import deque

def is_poly_symmetric(symmetry, all_vars):
    all_sym_vars = {var for chained_tuples in symmetry for var_tuple in chained_tuples for var in var_tuple}
    if len(all_sym_vars) != len(all_vars):
        print("diff vars = ", all_vars.difference(all_sym_vars))
    return len(all_sym_vars) == len(all_vars)

def is_chained_tuples_shared(symmetry, chained_tuples, peer_chained_tuples, all_vars_diff):
    # print("input chain = ", chained_tuples, ", peer chain = ", peer_chained_tuples)
    chain_diff = chained_tuples.difference(peer_chained_tuples)
    symmetry = [s for s in symmetry if len(s) <= len(chain_diff)]
    chain_diff_possible_subset = [chain_diff_possible_subset for chain_diff_possible_subset in symmetry if chain_diff_possible_subset.issubset(chain_diff)]
    # print("chain_diff = ", chain_diff, ", chain_diff_possible_subset = ", chain_diff_possible_subset)
    if len(chain_diff_possible_subset) == 0:  # The subset search is complete, and the remaining variables must be those that are disjointed.
        for tpl in chain_diff:
            if not set(tpl).issubset(all_vars_diff):
                return False
        return True
    else:
        for chain in chain_diff_possible_subset:
            if is_chained_tuples_shared(symmetry, chain_diff, chain, all_vars_diff):
                return True
        return False 

def merge_symmetry(symmetry_pair, all_vars_pair): # each tuple chain in symmetry is sorted
    # mats = []
    # for symmetry in symmetry_pair:
    #     mat = {}
    #     for chained_tuples in symmetry:
    #         key = chained_tuples[0][0]
    #         if mat.get(key) is None:
    #             mat[key] = {tuple(chained_tuples)}
    #         else:
    #             mat[key].add(tuple(chained_tuples))
    #     mats.append(mat)
    
    # keys = sorted(all_vars_pair[0] | all_vars_pair[1])

    # for key in keys:
    #     share = set()
    #     unshare = set()

    #     for i in [0, 1]:
    #         if key not in all_vars_pair[i] and key in mats[1-i]:
    #             while len(mats[1-i][key]) > 0:
    #                 chained_tuples = mats[1-i][key].pop()
    #                 key_1 = chained_tuples[0][1] # the second item in the first tuple in the chain
    #                 if key_1 not in all_vars_pair[i]:
    #                     if len(chained_tuples) == 1:
    #                         share.add(chained_tuples) # disjoint
    #                     else:
    #                         new_chained_tuples = chained_tuples[1:] + chained_tuples[0:1] # share/share tuple is rotated to the end of the tuple chain
    #                         new_key = new_chained_tuples[0][0]
    #                         if new_key < key: # if the second tuple key is less than the first tuple key in the old chain, it means that all tuples in the chain are rotated, which means they are share/share tuples
    #                             share.add(chained_tuples)
    #                         else:
    #                             mats[1-i][new_key].add(new_chained_tuples)
    #                 else:
    #                     unshare.add(chained_tuples)

    #         if key in all_vars_pair[i] and key not in mats[i] and key in mats[1-i]:
    #             # when key not in mats[1], key may be the second item of a tuple (such tuple must be discarded), or the first item of non-first tuple:
    #             # for the case of the first item of non-first tuple, there must be a tuple with first item smaller than that, that has been processed
    #             unshare.update(mats[1-i][key])
    #             mats[1-i][key] = {}

    #     if key in mats[0] and key in mats[1]:
    #         s = [mats[0][key], mats[1][key]] 
    #         share.update(s[0].intersection(s[1]))
    #         mats[0][key].difference_update(share)
    #         mats[1][key].difference_update(share)

    #         check_list = [sorted(s[0]), sorted(s[1])]
    #         while len(mats[0][key]) > 0:
    #             chained_tuples = mats[0][key].pop()
    #             key_1 = chained_tuples[0][1]
    #             if key_1 not in all_vars_pair[i]:
    #                 unshare.add(chained_tuples)
    #             else:
    #                 idx = bisect.bisect_left(check_list[1], chained_tuples)
                    # check_list[1][idx-1][0][1] ==
                    # check_list[1][idx][0][1] ==

    symmetry_pair = [{frozenset(chained_tuples) for chained_tuples in symmetry} for symmetry in symmetry_pair]
    var_diffs = [all_vars_pair[0].difference(all_vars_pair[1]), all_vars_pair[1].difference(all_vars_pair[0])]
    share = set()
    share.update(symmetry_pair[0].intersection(symmetry_pair[1]))
    # print("after intersection, share = ", share, ", symmetry_pair[0] = ", symmetry_pair[0], ", symmetry_pair[1] = ", symmetry_pair[1], ", intersection = ", symmetry_pair[0].intersection(symmetry_pair[1]))
    for i, symmetry in enumerate(symmetry_pair):
        for chained_tuples in symmetry:
            chained_tuple_vars = {var for t in chained_tuples for var in t}
            if len(chained_tuple_vars.intersection(all_vars_pair[1-i])) == 0:
                share.add(chained_tuples)
    
    for chained_tuples in symmetry_pair[0]:
        for peer_chained_tuples in symmetry_pair[1]:
            if len(chained_tuples) > len(peer_chained_tuples) and peer_chained_tuples.issubset(chained_tuples) and is_chained_tuples_shared(symmetry_pair[1], chained_tuples, peer_chained_tuples, var_diffs[0]):
                share.add(chained_tuples)
            elif len(chained_tuples) < len(peer_chained_tuples) and chained_tuples.issubset(peer_chained_tuples) and is_chained_tuples_shared(symmetry_pair[0], peer_chained_tuples, chained_tuples, var_diffs[1]):
                share.add(peer_chained_tuples)
            elif chained_tuples == peer_chained_tuples:
                continue
    unshare = symmetry_pair[0].union(symmetry_pair[1]).difference(share)
    return share, unshare

def poly_sys_symmetry(symmetry_q, all_vars_q):
    assert isinstance(symmetry_q, deque)
    assert isinstance(all_vars_q, deque)
    first_symmetry = symmetry_q.popleft()
    first_all_vars = all_vars_q.popleft()
    isleak = False
    print("begin merge")
    merged_cnt = 0
    while len(symmetry_q):     
        second_symmetry = symmetry_q.popleft()
        second_all_vars = all_vars_q.popleft()
        share, unshare = merge_symmetry([first_symmetry, second_symmetry], [first_all_vars, second_all_vars])
        first_symmetry = share
        first_all_vars.update(second_all_vars)
        if not is_poly_symmetric(first_symmetry, first_all_vars):
            print("merge poly leak, symmetry = ", first_symmetry, ", all vars = ", first_all_vars)
            isleak = True # corner case not solved
            break
        merged_cnt += 1
        print("merged_cnt = ", merged_cnt)
    return first_symmetry, isleak

if __name__ == "__main__":
    # The following case is hard to identify
    # symmetry_0 = [[(1, 3), (2, 4)],[(1, 2)], [(3, 4), (5, 6)], [(1, 2), (3, 4)]]
    # symmetry_1 = [[(3, 4)],[(1, 2)], [(1, 2), (5, 6)]]
    # all_vars_pair = [{1, 2, 3, 4, 5, 6}, {1, 2, 3, 4, 5, 6}]
    
    # symmetry_0 = [[(1, 3), (2, 4)],[(1, 2)], [(1, 2), (3, 4)]]
    # symmetry_1 = [[(3, 4)],[(1, 2)], [(1, 2), (5, 6)]]
    # all_vars_pair = [{1, 2, 3, 4}, {1, 2, 3, 4, 5, 6}]

    symmetry_0 = [[(1, 3), (2, 4)],[(1, 2)], [(1, 2), (3, 4)]]
    symmetry_1 = [[(5, 6)],[(7, 8)], [(9, 10), (7, 8)]]
    all_vars_pair = [{1, 2, 3, 4}, {5, 6, 7, 8, 9, 10}]
    symmetry_pair = [symmetry_0, symmetry_1]
    share, unshare = merge_symmetry(symmetry_pair, all_vars_pair)
    print("share = ", share)
    print("unshare = ", unshare)             


        
            
