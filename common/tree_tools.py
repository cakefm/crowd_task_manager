import xml.dom.minidom as xml
from collections import namedtuple
import numpy as np

IndexedNode = namedtuple("IndexedNode", ["path", "node"])

def create_element_node(name, attributes = dict()):
    node = xml.parseString(f"<{name}/>").documentElement
    for key in attributes:
        node.setAttribute(key, attributes[key])
    return node

def filter_element_nodes(sequence):
    return [x for x in sequence if x.nodeType == xml.Node.ELEMENT_NODE]

def purge_non_element_nodes(tree):
	root = tree.cloneNode(True)
	for node in traverse_tree(root):
		for child in node.childNodes:
			if child.nodeType != xml.Node.ELEMENT_NODE:
				node.removeChild(child)
	return root

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