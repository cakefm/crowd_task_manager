import sys
sys.path.append("..")
import common.tree_alignment as ta
import common.tree_tools as tt

import xml.dom.minidom as xml
import unittest


class NodeDistanceTestCase(unittest.TestCase):
    def check(self, expected, actual):
        self.assertTrue(actual == expected, f"Distance was {actual} instead of {expected}")

    def test_nodes_equivalent(self):
        node1 = create_xml_node("test", {"val1" : 1, "val2" : 2})
        node2 = create_xml_node("test", {"val1" : 1, "val2" : 2})
        self.check(0, ta.node_distance(node1, node2))

    def test_nodes_different_name(self):
        node1 = create_xml_node("test1", {"val1" : 1, "val2" : 2})
        node2 = create_xml_node("test2", {"val1" : 1, "val2" : 2})
        self.check(20, ta.node_distance(node1, node2))

    def test_nodes_different_attributes(self):
        node1 = create_xml_node("test", {"val1" : 1, "val2" : 2, "val3" : 3})
        node2 = create_xml_node("test", {"val1" : 1, "val2" : 2})
        self.check(4, ta.node_distance(node1, node2))

    def test_nodes_different_attribute_values(self):
        node1 = create_xml_node("test", {"val1" : 1, "val2" : 2})
        node2 = create_xml_node("test", {"val1" : 1, "val2" : 4})
        self.check(2, ta.node_distance(node1, node2))

    def test_nodes_total_mismatch(self):
        node1 = create_xml_node("test", {"val1" : 1, "val2" : 2})
        node2 = create_xml_node("test1", {"val3" : 5, "val4" : 22})
        self.check(20 + 4 * 4, ta.node_distance(node1, node2))

    def test_nodes_overlapping_and_value_mismatch(self):
        node1 = create_xml_node("test", {"val1" : 1, "val2" : 2})
        node2 = create_xml_node("test1", {"val1" : 1, "val2" : 4, "val3" : 5, "val4" : 22})
        self.check(20 + 2 * 4 + 1 * 2, ta.node_distance(node1, node2))


if __name__ == '__main__':
    unittest.main()
