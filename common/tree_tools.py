import xml.dom.minidom as xml
from collections import namedtuple
import numpy as np

# def tree_traversal(tree, depth_first=True):
#     nodes = [tree]
#     node_index = 0

#     while L:
#     	if depth_first:
#         	child_index, node = nodes.pop()
#         else:
#         	child_index, node = nodes.pop(0)
#         node_index += 1
#         nodes += [child for child in node.childNodes if child.nodeType == xml.Node.ELEMENT_NODE]
#         yield node


# def insert_node(tree, path, node):
#     nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
#     if nodes:
#         if path[0] > len(tree.childNodes):
#         	raise IndexError(f"Cannot insert at index {path[0]}, sequence only has {len(tree.childNodes)} nodes.")
#         if len(path) == 1:
#             new_nodes = list(tree.childNodes)
#             new_nodes.insert(path[0], node)
#             tree.childNodes = new_nodes
#         insert_node(tree.childNodes[path[0]], path[1:], node)


# def trees_equivalent(tree1, tree2, node_distance_function):
# 	paths1 = get_node_paths(tree1, lambda x : True)
# 	paths2 = get_node_paths(tree2, lambda x : True)

# 	if len(paths1) != len(paths2):
# 		return False

# 	for p1, p2 in zip(paths1, paths2):
# 		if node_distance_function(get_node_at_path(tree1, p1), get_node_at_path(tree2, p2)) != 0:
# 			return False

# 	return True


# def validate_criteria_per_node(tree, criteria_function):
# 	return validate_criteria(tree, [], criteria_function)

# def _validate_criteria_per_node(tree, booleans, criteria_function):
# 	nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
#     if nodes:
#         for i, node in enumerate(tree.childNodes):
#             if node.nodeType == xml.Node.ELEMENT_NODE:
#                 booleans.append(criteria_function(node))
#             _validate_criteria_per_node(node, booleans, criteria_function)
#     return booleans

# def get_node_paths(tree, criteria_function):
#     return _node_paths(tree, [], [], criteria_function)

# def _get_node_paths(tree, path, paths, criteria_function):
#     nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
#     if nodes:
#         for i, node in enumerate(tree.childNodes):
#             if node.nodeType == xml.Node.ELEMENT_NODE and criteria_function(node):
#                 paths.append(path + [i])
#             _get_node_paths(node, path + [i], paths, criteria_function)
#     return paths

# def get_node_at_path(tree, path):
# 	nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
#     if nodes:
#         if path[0] > len(tree.childNodes):
#         	raise IndexError(f"Cannot get node at index {path[0]} of remaining path {path}, sequence only has {len(tree.childNodes)} nodes.")
#         if len(path) == 1:
#         	return list(tree.childNodes)[path[0]]
#         return get_node_at_path(tree.childNodes[path[0]], path[1:])

# def insert_node(tree, path, node):
#     nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
#     if nodes:
#         if path[0] > len(tree.childNodes):
#         	raise IndexError(f"Cannot insert at index {path[0]}, sequence only has {len(tree.childNodes)} nodes.")
#         if len(path) == 1:
#             new_nodes = list(tree.childNodes)
#             new_nodes.insert(path[0], node)
#             tree.childNodes = new_nodes
#         insert_node(tree.childNodes[path[0]], path[1:], node)


# # Automatically extend the sequence with "empty" nodes to make insertion possible
# def insert_node_extend(tree, path, node, create_empty_node):
#     nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
#     if nodes:
#         while path[0] >= len(tree.childNodes):
#             tree.childNodes.append(create_empty_node())
#         if len(path) == 1:
#             new_nodes = list(tree.childNodes)
#             new_nodes.insert(path[0], node)
#             tree.childNodes = new_nodes
#         insert_node_extend(tree.childNodes[path[0]], path[1:], node, create_empty_node)

IndexedNode = namedtuple("IndexedNode", ["path", "node"])

def create_element_node(name, attributes = dict()):
    node = xml.parseString(f"<{name}/>").documentElement
    for key in attributes:
        node.setAttribute(key, attributes[key])
    return node

def filter_element_nodes(sequence):
    return [x for x in sequence if x.nodeType == xml.Node.ELEMENT_NODE]

def traverse_tree(tree, depth_first):
    indexed_nodes = [IndexedNode([], tree)]
    while indexed_nodes:
        indexed_node = indexed_nodes.pop() if depth_first else indexed_nodes.pop(0)
        filtered_child_nodes = enumerate(filter_element_nodes(indexed_node.node.childNodes))
        indexed_child_nodes = [IndexedNode(indexed_node.path[:] + [index],  child) for index, child in filtered_child_nodes]
        indexed_nodes += reversed(indexed_child_nodes) if depth_first else indexed_child_nodes
        yield indexed_node
        
def traverse_tree_bf(tree):
    return traverse_tree(tree, False)

def traverse_tree_df(tree):
    return traverse_tree(tree, True)

def insert_node(tree, path, node):
    root = tree.cloneNode(True)
    sub_tree = root
    for depth, index in enumerate(path):
        if len(filter_element_nodes(sub_tree.childNodes)) <= index:
            raise IndexError(f"Cannot insert node at depth {depth} of path {path}, child node sequence only has {len(filter_element_nodes(sub_tree.childNodes))} element nodes while attempting to insert at {index}.")
        sub_tree = filter_element_nodes(sub_tree.childNodes)[index]
    sub_tree.parentNode.insertBefore(node, sub_tree)
    return root
        
def insert_node_with_extension(tree, path, node, extension_node):
    root = tree.cloneNode(True)
    sub_tree = root
    last = None
    for depth, index in enumerate(path):
        while len(filter_element_nodes(sub_tree.childNodes)) <= index:
            last = sub_tree.appendChild(extension_node.cloneNode(True))
        sub_tree = filter_element_nodes(sub_tree.childNodes)[index]
    sub_tree.parentNode.insertBefore(node, sub_tree)
    if last:
        sub_tree.parentNode.removeChild(last)
    return root

def get_node_at_path(root, path):
    node = root
    while path:
        node = filter_element_nodes(node.childNodes)[path[0]]
        path = path[1:]
    return node

def delete_node_at_path(root, path):
    node = root
    while path:
        node = filter_element_nodes(node.childNodes)[path[0]]
        path = path[1:]
    return node.parentNode.deletechild(node)

def delete_node(node):
    return node.parentNode.removeChild(node)

def get_matching_paths(tree, criteria_function):
    return [indexed_node.path for indexed_node in traverse_tree_bf(tree) if criteria_function(indexed_node.node)]

def get_matching_nodes(tree, criteria_function):
    return [indexed_node.node for indexed_node in traverse_tree_bf(tree) if criteria_function(indexed_node.node)]

def trees_equivalent(tree_1, tree_2, node_distance_function):
    indexed_nodes_1 = [n for n in traverse_tree_df(tree_1)]
    indexed_nodes_2 = [n for n in traverse_tree_df(tree_2)]
    
    if len(indexed_nodes_1) != len(indexed_nodes_2):
        return False
    
    for indexed_node_1, indexed_node_2 in zip(indexed_nodes_1, indexed_nodes_2):
        if indexed_node_1.path != indexed_node_2.path:
            return False
        if node_distance_function(indexed_node_1.node, indexed_node_2.node) != 0:
            return False
    
    return True