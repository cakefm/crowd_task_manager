import xml.dom.minidom as xml
import numpy as np

import sys
sys.path.append("..")
import common.tree_tools as tt

GAP_ELEMENT_NAME = "gap"

def create_gap_element():
    return xml.parseString("<t/>").createElement(GAP_ELEMENT_NAME)

# Needleman Wunsch algorithm implementation for sequences of XML nodes
# Algorithm based on psuedocode with modifications at: https://en.wikipedia.org/wiki/Needleman%E2%80%93Wunsch_algorithm
def align_xml(A, B, similarity_function, gap_penalty = 10):
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
            match = F[i - 1, j - 1] + similarity_function(A[i - 1], B[j - 1])
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
        if i > 0 and j > 0 and F[i, j] == F[i - 1, j - 1] + similarity_function(A[i - 1], B[j - 1]):
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


def node_distance(a, b, ignored = {"xml:id", "n", "label", "startid", "endid"}):
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
    
# Tree alignment: traverses all the nodes in the tree while keeping track of the total distance and node count
def align_trees_pairwise(tree1, tree2):
    nodes1 = [node for node in tree1.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    nodes2 = [node for node in tree2.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    
    score = 0
    
    if nodes1 or nodes2:
        tree1.childNodes, tree2.childNodes, distance = align_xml(nodes1, nodes2, node_distance)
        aligned_nodes = list(zip(tree1.childNodes, tree2.childNodes))
        
        score += distance
        for child1, child2 in aligned_nodes:
            new_score = align_trees_pairwise(child1, child2)
            score += new_score
            
    return score


def align_trees_multiple(trees):
    # Create all distinct xml pairs
    pairs = {}
    for i in range(len(trees)):
        for j in range(i + 1, len(trees)):
            a = trees[i]
            b = trees[j]
            pairs[(i, j)] = (xml.parseString(a), xml.parseString(b))

    # Perform pairwise alignmnents
    distances = np.full((len(trees), len(trees)), np.inf)
    for i, j in pairs:
        a, b = pairs[i, j]
        distances[i, j] = align_trees_pairwise(a, b)
    
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
    return mas


def copy_gaps(tree1, tree2):
    for path in tt.get_matching_paths(tree1.documentElement, lambda node : node.tagName == GAP_ELEMENT_NAME):
        tt.insert_node_with_extension(tree2.documentElement, path, create_gap_element(), create_gap_element())