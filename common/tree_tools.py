import xml.dom.minidom as xml
import numpy as np

def get_node_paths(tree, criteria_function):
    return _node_paths(tree, [], [], criteria_function)

def _node_paths(tree, path, paths, criteria_function):
    nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    if nodes:
        for i, node in enumerate(tree.childNodes):
            if node.nodeType == xml.Node.ELEMENT_NODE and criteria_function(node):
                paths.append(path + [i])
            _node_paths(node, path + [i], paths, criteria_function)
    return paths


def insert_node(tree, path, node):
    nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    if nodes:
        if path[0] > len(tree.childNodes):
        	raise IndexError(f"Cannot insert at index {path[0]}, sequence only has {len(tree.childNodes)} nodes.")
        if len(path) == 1:
            new_nodes = list(tree.childNodes)
            new_nodes.insert(path[0], node)
            tree.childNodes = new_nodes
        insert_node(tree.childNodes[path[0]], path[1:], node)


# Automatically extend the sequence with gaps to make insertion possible
def insert_node_extend(tree, path, node, create_empty_node):
    nodes = [node for node in tree.childNodes if node.nodeType == xml.Node.ELEMENT_NODE]
    if nodes:
        while path[0] >= len(tree.childNodes):
            tree.childNodes.append(create_empty_node())
        if len(path) == 1:
            new_nodes = list(tree.childNodes)
            new_nodes.insert(path[0], node)
            tree.childNodes = new_nodes
        insert_node_extend(tree.childNodes[path[0]], path[1:], node, create_empty_node)