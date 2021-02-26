import xml.dom.minidom as xml
import numpy as np

import sys
sys.path.append("..")
import common.tree_tools as tt
from common.settings import cfg

GAP_ELEMENT_NAME = "gap"
GAP_PENALTY = 10
IGNORE = {"xml:id", "label", "startid", "endid", "facs"}

def create_gap_element():
    return tt.create_element_node(GAP_ELEMENT_NAME)

# Needleman Wunsch algorithm implementation for sequences of XML nodes
# Algorithm based on psuedocode with modifications at: https://en.wikipedia.org/wiki/Needleman%E2%80%93Wunsch_algorithm
def align_xml(A, B, distance_function, gap_penalty):
    # padding and re-referencing

    A = A + [create_gap_element() for x in range(max(0, len(B)-len(A)))]
    B = B + [create_gap_element() for x in range(max(0, len(A)-len(B)))]

    # Computation of alignment scores
    # Matrix G is intended for varying gap_penalty values if the need ever arises (e.g. to punish small gaps)
    F = np.zeros((len(A) + 1, len(B) + 1))
    G = np.zeros((len(A) + 1, len(B) + 1))
    for i in range(len(A) + 1):
        F[i, 0] = gap_penalty * i
        G[i, 0] = gap_penalty
    for j in range(len(B) + 1):
        F[0, j] = gap_penalty * j
        G[0, j] = gap_penalty
    for i in range(1, len(A) + 1):
        for j in range(1, len(B) + 1):
            match = F[i - 1, j - 1] + distance_function(A[i - 1], B[j - 1])
            delete = F[i - 1, j] + gap_penalty
            insert = F[i, j - 1] + gap_penalty
            minimum = min([match, insert, delete])
            G[i, j] = gap_penalty
            F[i, j] = minimum

    # Picking the best alignment
    A_aligned = []
    B_aligned = []
    i = len(A)
    j = len(B)
    while i > 0 or j > 0:
        if i > 0 and j > 0 and F[i, j] == F[i - 1, j - 1] + distance_function(A[i - 1], B[j - 1]):
            A_aligned = [A[i - 1]] + A_aligned
            B_aligned = [B[j - 1]] + B_aligned
            i -= 1
            j -= 1
        elif i > 0 and F[i, j] == F[i - 1, j] + G[i, j]:
            A_aligned = [A[i - 1]] + A_aligned
            B_aligned = [create_gap_element()] + B_aligned
            i -= 1
        elif j > 0 and F[i, j] == F[i, j - 1] + G[i, j]:
            A_aligned = [create_gap_element()] + A_aligned
            B_aligned = [B[j - 1]] + B_aligned
            j -= 1
    
    # Trim redundant gaps from result and correct the costs
    trimmed_gaps = 0
    old_len = len(A_aligned)
    aligned = [(a, b) for a, b in zip(A_aligned, B_aligned) if not a.tagName == b.tagName == GAP_ELEMENT_NAME]
    if aligned:
        A_aligned, B_aligned = zip(*aligned)
        trimmed_gaps += old_len - len(A_aligned)
    distance = F[len(A), len(B)] - trimmed_gaps * gap_penalty
    
    return A_aligned, B_aligned, distance


# TODO: Get rid of magic numbers
def node_distance(a, b, ignored = IGNORE):
    penalty = 0
    
    # Different tag
    if a.tagName != b.tagName:
        penalty += 20
    
    # Missing attributes
    matches = a.attributes.keys() & b.attributes.keys()
    missing = (a.attributes.keys() | b.attributes.keys()) - matches
    penalty += 4 * len(missing)
    
    # Mismatching values
    for match in matches - ignored:
        if a.attributes[match].value != b.attributes[match].value:
            penalty += 2
    
    return penalty

# Make it beneficial to match on index if present
# Punish mismatches in crmp_id if present
# Should work as long as there aren't nodes in the same list of children with the same index
def node_distance_anchored(a, b, ignored = IGNORE):
    distance = node_distance(a, b, ignored)
    a_n = a.getAttribute("n")
    b_n = b.getAttribute("n")
    if a_n != "" and b_n != "":
        if a_n==b_n and a.tagName==b.tagName:
            distance = 0

    a_crmp = a.getAttribute("crmp_id")
    b_crmp = b.getAttribute("crmp_id")
    if a_crmp != "" and b_crmp != "":
        if a_crmp != b_crmp:
            distance += 1000

    return distance

# Tree alignment: traverses all the nodes in the tree while keeping track of the total distance and node count
def align_trees_pairwise(tree1, tree2, distance_function=node_distance, gap_penalty=GAP_PENALTY):
    nodes1 = [node for node in tree1.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    nodes2 = [node for node in tree2.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    
    score = 0
    
    if nodes1 or nodes2:
        tree1_new_children, tree2_new_children, distance = align_xml(nodes1, nodes2, distance_function, gap_penalty)
        tt.replace_child_nodes(tree1, tree1_new_children)
        tt.replace_child_nodes(tree2, tree2_new_children)

        aligned_nodes = list(zip(tree1.childNodes, tree2.childNodes))
        
        score += distance
        for child1, child2 in aligned_nodes:
            new_score = align_trees_pairwise(child1, child2, distance_function, gap_penalty=gap_penalty)
            score += new_score
            
    return score


def align_trees_multiple(trees, distance_function=node_distance, gap_penalty=GAP_PENALTY):
    # If we just have one tree, don't do anything fancy and just return that one
    if len(trees)==1:
        return [tt.purge_non_element_nodes(xml.parseString(trees[0]))]

    # Create all distinct xml pairs
    pairs = {}
    for i in range(len(trees)):
        for j in range(i + 1, len(trees)):
            a = tt.purge_non_element_nodes(xml.parseString(trees[i]))
            b = tt.purge_non_element_nodes(xml.parseString(trees[j]))
            pairs[(i, j)] = (a, b)

    # Perform pairwise alignments
    distances = np.full((len(trees), len(trees)), np.inf)
    for i, j in pairs:
        a, b = pairs[i, j]
        distances[i, j] = align_trees_pairwise(a, b, distance_function=distance_function, gap_penalty=gap_penalty)
    
    distance_bins = [0] * len(trees)
    for i, j in pairs:
        distance_bins[i] += distances[i, j]
        distance_bins[j] += distances[i, j]

    # Tree with the smallest overall distance will be a good candidate
    main_tree_index = np.argmin(distance_bins)

    # Use the pairwise alignments with a heuristic to come up with a multiple alignment solution
    mas = []
    candidate_pairs = sorted([(i, j) for i, j in pairs if main_tree_index in (i, j)], key = lambda t : distances[t[0], t[1]])
    closest_pair_index = candidate_pairs[0]
    closest_pair = pairs[closest_pair_index]
    mas.append(closest_pair[closest_pair_index.index(main_tree_index)]) # The closest tree
    mas.append(closest_pair[not closest_pair_index.index(main_tree_index)]) # The "other" tree
    for pair_index in candidate_pairs[1:]:
        pair = pairs[pair_index]
        main_tree = pair[pair_index.index(main_tree_index)]
        cand_tree = pair[not pair_index.index(main_tree_index)]

        print(f"ITERATION {candidate_pairs.index(pair_index)}:")
        print("===MAIN:")
        print(main_tree.toprettyxml())
        print("===CAND:")
        print(cand_tree.toprettyxml())
        print()

        # Copy the gaps
        for tree in mas:
            copy_gaps(main_tree, tree)

        mas.append(cand_tree)

        print("===RESULT SO FAR:")
        for tree in mas:
            print(tree.toprettyxml())
            print("============")

        print()
        print()
        print()
        print("------------------------------------------------")

    # Too spammy, since we use the aggregator in the score rebuilder: the above prints do not trigger for 2-tree alignments

    # print("===FINAL RESULTS:")
    # for index, tree in enumerate(mas):
    #     print(f"TREE AT INDEX {index}:")
    #     print(tree.toprettyxml())
    #     print("============")
    return mas


def copy_gaps(tree1, tree2):
    for path in tt.get_matching_paths(tree1.documentElement, lambda node : node.tagName == GAP_ELEMENT_NAME):
        tt.insert_node_with_extension(tree2.documentElement, path, create_gap_element(), create_gap_element())


def consensus_best_node_distance(nodes, distance_function=node_distance):
    # print("performing consensus check for nodes: ")
    # [print("-", x.tagName) for x in nodes]
    node_distances = np.full((len(nodes), len(nodes)), np.inf)
    for i, a in enumerate(nodes):
        for j, b in enumerate(nodes): 
            node_distances[i, j] = distance_function(a, b)

    # print("distances: ")
    # print(node_distances)
    # Get the cumulative distances of all the nodes to one another
    node_distance_bins = [0] * len(nodes)
    for i in range(len(nodes)):
        for j in range(len(nodes)): 
            node_distance_bins[i] += node_distances[i, j]
            node_distance_bins[j] += node_distances[i, j]

    # print("bins: ", node_distance_bins)
    # Idea for threshold check: see how many nodes are within 1 mad of best node regarding cumulative distance
    # Then get the ratio between this and the total amount of candidates
    # This number will get close to 1 if many of the nodes agree with the best node
    mcd = min(node_distance_bins)
    mcd_index = node_distance_bins.index(mcd)
    mad = np.median(np.absolute(np.array(node_distance_bins) - np.median(node_distance_bins)))
    candidates_that_agree = np.zeros(len(node_distance_bins)) > 0
    candidates_that_agree[mcd_index] = True
    for index, val in enumerate(candidates_that_agree):
        if val==False:
            # Is the node within std range?
            candidates_that_agree[index] = node_distances[mcd_index, index] <= cfg.aggregator_consensus_tolerance * mad

    # print("mad and mcd: ", mad, mcd)
    # print(candidates_that_agree)
    ratio = sum(candidates_that_agree) / len(nodes)
    consensus = False
    if ratio >= cfg.aggregator_xml_threshold:
        consensus = True
    # print("consensus: ", nodes[mcd_index].tagName, consensus)
    return nodes[mcd_index], consensus, False

# Should probably deprecate/redesign this one
def consensus_bnd_enrich_skeleton(nodes, distance_function=node_distance):
    head = nodes[0]
    tail = nodes[1:]

    if head.tagName == "measure":
        return head, True, False
    if head.tagName == GAP_ELEMENT_NAME:
        return consensus_best_node_distance(tail, distance_function=distance_function)
    else:
        return consensus_best_node_distance(nodes, distance_function=distance_function)


# This one will preserve measures/layers/staffs from the first node, but override anything that is different within a layer
# by the best node determined by the best_node_distance method
def consensus_bnd_override_inner(nodes, distance_function=node_distance):
    head = nodes[0]
    tail = nodes[1:]

    if head.tagName in ["measure", "layer", "staff"]:
        for node in tail:
            if node.tagName == GAP_ELEMENT_NAME:
                return head, True, True
    return consensus_best_node_distance(tail, distance_function=distance_function)


def build_consensus_tree(trees, consensus_method = consensus_best_node_distance, exclude = [GAP_ELEMENT_NAME]):
    consensus_per_node = {}
    return _build_consensus_tree(trees, create_gap_element(), 0, consensus_method, exclude, consensus_per_node, -1).childNodes[0], consensus_per_node


def _build_consensus_tree(trees, new_tree, n, consensus_method, exclude, node_consensus_dict, prune_index):
    group = zip(*[c.childNodes for c in trees])
    for nodes in group:
        if prune_index >= 0:
            best, consensus, prune = nodes[prune_index], True, True
        else:
            best, consensus, prune = consensus_method(nodes)
        node_consensus_dict[best] = consensus
        if best.tagName in exclude:
            continue

        new_tree.childNodes.append(best.cloneNode(False))

        _build_consensus_tree(nodes, new_tree.childNodes[-1], n + 1, consensus_method, exclude, node_consensus_dict, nodes.index(best) if prune else prune_index)
    return new_tree