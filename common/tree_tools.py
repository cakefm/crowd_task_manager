import xml.dom.minidom as xml
from collections import namedtuple
import numpy as np

IndexedNode = namedtuple("IndexedNode", ["path", "node"])

def create_element_node(name, attributes = dict()):
    '''
    Creates a new XML element node with given tag name and attributes.

    Arguments:
        name (str)                          -- The tag name of the element node
        attributes (dict<str:str>)          -- The attributes of the element node

    Returns:
        node (Node)                         -- The XML element node
    '''
    node = xml.parseString(f"<{name}/>").documentElement
    for key in attributes:
        node.setAttribute(str(key), str(attributes[key]))
    return node

def matching_pairs(sequence_1, sequence_2, attribute_names):
    '''
    Match two sequences on attributes in order to create a list with matching pairs.

    Arguments:
        sequence_1 (list<Node>)               -- A list with nodes
        sequence_2 (list<Node>)               -- A list with nodes
        attribute_names (list<str>)           -- A list with attribute names

    Returns:
        list (List<Node, Node>)               -- List for matching pairs
    '''
    pairs = []
    for a in sequence_1:
        for b in sequence_2:
            if all([a.getAttribute(attr) == b.getAttribute(attr) for attr in attribute_names]):
                pairs.append((a, b))
    return pairs

def filter_element_nodes(sequence):
    '''
    Filters element nodes from a sequence of nodes.

    Arguments:
        sequence (list<Node>)               -- A list with nodes

    Returns:
        filtered (list<Node>)               -- A list with only element nodes
    '''
    return [x for x in sequence if x.nodeType == xml.Node.ELEMENT_NODE]

def node_type_filter(node_type):
    '''
    Creates a lambda that can act as a node filter for the tree traversal function.
    This filter will filter on the given node type.

    Arguments:
        node_type (int)                    -- Type of the xml node to filter on

    Returns:
        filter (<Node> -> <bool>)          -- A function that returns True if Node.nodeType==node_type
    '''
    return lambda node : node.nodeType == node_type

def purge_non_element_nodes(tree):
    '''
    Purges all non-element nodes from the given (sub) tree. It does not edit the tree
    in place, instead it returns a modified copy. Mainly useful for consistent printing.

    Arguments:
        tree (Node)                         -- The root node of an XML tree

    Returns:
        purged (Node)                       -- A copy of the tree that only contains element nodes
    '''
    root = tree.cloneNode(True)
    for indexed_node in traverse_tree(root, filter_function=lambda x : x):
        if indexed_node.node.nodeType != xml.Node.ELEMENT_NODE and indexed_node.node.parentNode:
            indexed_node.node.parentNode.removeChild(indexed_node.node)
    return root

def traverse_tree(tree, depth_first=True, filter_function=node_type_filter(xml.Node.ELEMENT_NODE)):
    '''
    Provides a generator that traverses the tree in either a depth-first or a breadth-first fashion,
    depending on the given argument. Ignores non-element nodes during traversal.

    Arguments:
        tree (Node)                         -- The root node of an XML tree
        depth_first (bool)                  -- Whether to use depth-first traversal, otherwise will use breadth-first

    Returns:
        next (Node)                         -- The next node of the given sub_tree in the chosen traversal order
    '''
    indexed_nodes = [IndexedNode([], tree)]
    while indexed_nodes:
        indexed_node = indexed_nodes.pop() if depth_first else indexed_nodes.pop(0)
        filtered_child_nodes = enumerate(filter(filter_function, indexed_node.node.childNodes))
        indexed_child_nodes = [IndexedNode(indexed_node.path[:] + [index],  child) for index, child in filtered_child_nodes]
        indexed_nodes += reversed(indexed_child_nodes) if depth_first else indexed_child_nodes
        yield indexed_node


def has_ancestors_by_name(node, ancestor_names):
    ancestor = node.parentNode
    while ancestor != None:
        try:
            if ancestor.tagName in ancestor_names:
                return True
            ancestor = ancestor.parentNode
        except:
            return False
    return False

def insert_node(tree, path, node, append=False):
    '''
    Inserts a given node into the given (sub) tree at the given path. The path indicates
    the next child node to take while traversing down the tree. The last element of the path
    indicates where to insert the given node; as such the path must be non-empty.
    Will throw an exception if it cannot traverse the given path at any point.

    If `append` is `True`, it will instead append to the children of the last node
    specified by the path.

    Arguments:
        tree (Node)                         -- The root node of an XML tree
        path (list<int>)                    -- The path to insert the node at (non-empty)
        node (Node)                         -- The element node to insert
        append (bool)                       -- Whether to append to children instead

    Returns:
        modified_tree (Node)                -- A copy of the tree with the inserted node
    '''
    root = tree.cloneNode(True)
    sub_tree = root
    for depth, index in enumerate(path):
        if len(filter_element_nodes(sub_tree.childNodes)) <= index:
            raise IndexError(f"Cannot insert node at depth {depth} of path {path}, child node sequence only has {len(filter_element_nodes(sub_tree.childNodes))} element nodes while attempting to insert at {index}.\n Tree:\n {root.toprettyxml()}")
        sub_tree = filter_element_nodes(sub_tree.childNodes)[index]

    if append:
        sub_tree.appendChild(node)
    else:
        sub_tree.parentNode.insertBefore(node, sub_tree)

    return root

def insert_node_with_extension(tree, path, node, extension_node):
    '''
    Inserts a given node into the given (sub) tree at the given path. The path indicates
    the next child node to take while traversing down the tree. The last element of the path
    indicates where to insert the given node; as such the path must be non-empty. If the path
    cannot be followed due to missing child nodes in any of the nodes, the child node sequence
    will be automatically extended with copies of the `extensions_node` to make traversal possible.

    Arguments:
        tree (Node)                             -- The root node of an XML tree
        path (list<int>)                        -- The path to insert the node at (non-empty)
        node (Node)                             -- The element node to insert
        extension_node (Node)                   -- The element node to use for extension

    Returns:
        modified_tree (Node)                    -- A copy of the tree with the inserted node(s)
    '''
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

def get_node_at_path(tree, path):
    '''
    Gets a node from the given tree at the given path.

    Arguments:
        tree (Node)                             -- The root node of an XML tree
        path (list<int>)                        -- The path to get the node from

    Returns:
        node (Node)                             -- The node found at the path
    '''
    node = tree
    while path:
        node = filter_element_nodes(node.childNodes)[path[0]]
        path = path[1:]
    return node

def delete_node_at_path(root, path):
    '''
    Deletes a node from the given tree at the given path. Modifies the tree in-place.

    Arguments:
        tree (Node)                             -- The root node of an XML tree
        path (list<int>)                        -- The path to delete the node at

    Returns:
        node (Node)                             -- The deleted node
    '''
    node = root
    while path:
        node = filter_element_nodes(node.childNodes)[path[0]]
        path = path[1:]
    return node.parentNode.deletechild(node)

def delete_node(node):
    '''
    Deletes a node from its own tree. Modifies the tree in-place.

    Arguments:
        node (Node)                             -- The node to delete

    Returns:
        node (Node)                             -- The deleted node
    '''
    return node.parentNode.removeChild(node)

def get_matching_paths(tree, criteria_function):
    '''
    Gets the paths to all nodes that match the criteria_function in the given tree.

    Arguments:
        tree (Node)                             -- The root node of an XML tree
        criteria_function (<Node> --> <bool>)   -- The criteria function that decides whether to include the node or not

    Returns:
        paths (list<list<int>>)                 -- A list of matching paths
    '''
    return [indexed_node.path for indexed_node in traverse_tree(tree, depth_first=False) if criteria_function(indexed_node.node)]

def get_matching_nodes(tree, criteria_function):
    '''
    Gets all nodes that match the criteria_function in the given tree.

    Arguments:
        tree (Node)                             -- The root node of an XML tree
        criteria_function (<Node> --> <bool>)   -- The criteria function that decides whether to include the node or not

    Returns:
        nodes (list<Node>)                      -- A list of matching nodes
    '''
    return [indexed_node.node for indexed_node in traverse_tree(tree, depth_first=False) if criteria_function(indexed_node.node)]

def trees_equivalent(tree_1, tree_2, node_distance_function):
    '''
    Compares tree_1 and tree_2 by checking whether the structure is the same and whether the distance between every
    matching node is 0 according to the given node distance function. Ignores non-element nodes.

    Arguments:
        tree1 (Node)                                        -- The root node of the first XML tree
        tree2 (Node)                                        -- The root node of the second XML tree
        node_distance_function (<Node, Node> --> <float>)   -- The distance function to use for the nodes

    Returns:
        trees_equivalent (bool)                             -- Whether the trees are equivalent
    '''
    indexed_nodes_1 = [n for n in traverse_tree(tree_1)]
    indexed_nodes_2 = [n for n in traverse_tree(tree_2)]
    
    if len(indexed_nodes_1) != len(indexed_nodes_2):
        return False
    
    for indexed_node_1, indexed_node_2 in zip(indexed_nodes_1, indexed_nodes_2):
        if indexed_node_1.path != indexed_node_2.path:
            return False
        if node_distance_function(indexed_node_1.node, indexed_node_2.node) != 0:
            return False
    
    return True


def replace_child_nodes(tree, new_child_nodes):
    '''
    Replaces the child nodes of the given root with the ones in the given list.
    Edits the tree in place.

    Arguments:
        tree (Node)                             -- The root node of an XML tree
        new_child_nodes (list<Node>)            -- A list with nodes to use as the new children for the root

    Returns:
        tree (Node)                             -- The modified tree (not a copy, same ref!)
    '''
    for child in [c for c in tree.childNodes]:
        tree.removeChild(child)
    for child in list(new_child_nodes):
        tree.appendChild(child)
    return tree
