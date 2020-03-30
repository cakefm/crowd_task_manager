import xml.dom.minidom as xml
import numpy as np

# Needleman Wunsch algorithm implementation for sequences of XML nodes
# Algorithm based on psuedocode with modifications at: https://en.wikipedia.org/wiki/Needleman%E2%80%93Wunsch_algorithm
def align_xml(A, B, similarity_function, gap_penalty = 10):
    # padding and re-referencing
    def create_gap_element():
        return xml.parseString("<t/>").createElement("gap")

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
    aligned = [(a, b) for a, b in zip(A_aligned, B_aligned) if not a.tagName == b.tagName == 'gap']
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
# Recursive function
def align_trees_rec(tree1, tree2):
    nodes1 = [node for node in tree1.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    nodes2 = [node for node in tree2.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    
    score = 0
    n = 0
    
    if nodes1 or nodes2:
        tree1.childNodes, tree2.childNodes, distance = align_xml(nodes1, nodes2, node_distance)
        aligned_nodes = list(zip(tree1.childNodes, tree2.childNodes))
        
        score += distance
        n += 1
        for child1, child2 in aligned_nodes:
            new_score, new_n = align_trees_rec(child1, child2)
            score += new_score
            n += new_n
            
    return score, n

# API method
# For now, nothing is done with the count, but it could be used to calculate a mean node distance
def align_trees(tree1, tree2):
    score, n = align_trees_rec(tree1, tree2)
    return score