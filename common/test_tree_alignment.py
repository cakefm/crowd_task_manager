import sys
sys.path.append("..")
import common.tree_alignment as ta
import common.tree_tools as tt

import xml.dom.minidom as xml
import unittest

# TODO: add tests for ignored attributes
class NodeDistanceTestCase(unittest.TestCase):
    def check(self, expected, actual):
        self.assertTrue(actual == expected, f"Distance was {actual} instead of {expected}")

    def test_nodes_equivalent(self):
        node1 = tt.create_element_node("test", {"val1" : 1, "val2" : 2})
        node2 = tt.create_element_node("test", {"val1" : 1, "val2" : 2})
        self.check(0, ta.node_distance(node1, node2))

    def test_nodes_different_name(self):
        node1 = tt.create_element_node("test1", {"val1" : 1, "val2" : 2})
        node2 = tt.create_element_node("test2", {"val1" : 1, "val2" : 2})
        self.check(20, ta.node_distance(node1, node2))

    def test_nodes_different_attributes(self):
        node1 = tt.create_element_node("test", {"val1" : 1, "val2" : 2, "val3" : 3})
        node2 = tt.create_element_node("test", {"val1" : 1, "val2" : 2})
        self.check(4, ta.node_distance(node1, node2))

    def test_nodes_different_attribute_values(self):
        node1 = tt.create_element_node("test", {"val1" : 1, "val2" : 2})
        node2 = tt.create_element_node("test", {"val1" : 1, "val2" : 4})
        self.check(2, ta.node_distance(node1, node2))

    def test_nodes_total_mismatch(self):
        node1 = tt.create_element_node("test", {"val1" : 1, "val2" : 2})
        node2 = tt.create_element_node("test1", {"val3" : 5, "val4" : 22})
        self.check(20 + 4 * 4, ta.node_distance(node1, node2))

    def test_nodes_overlapping_and_value_mismatch(self):
        node1 = tt.create_element_node("test", {"val1" : 1, "val2" : 2})
        node2 = tt.create_element_node("test1", {"val1" : 1, "val2" : 4, "val3" : 5, "val4" : 22})
        self.check(20 + 2 * 4 + 1 * 2, ta.node_distance(node1, node2))


class TreeAlignmentPairwiseTestCase(unittest.TestCase):
    # Gives a penalty of 10 if there is any difference, otherwise returns 0
    def simple_distance_function(self, a, b):
        different_tag = a.tagName != b.tagName
        different_attributes = a.attributes.keys() != b.attributes.keys()
        different_attribute_values = {x.value for x in a.attributes.values()} != {x.value for x in b.attributes.values()}
        return any([different_tag, different_attributes, different_attribute_values]) * 10

    def check_tree_transformation(self, tree1, tree2, tree1_expected):
        tree1 = xml.parseString(tree1).documentElement
        tree2 = xml.parseString(tree2).documentElement
        tree1_expected = xml.parseString(tree1_expected).documentElement

        ta.align_trees_pairwise(tree1, tree2, distance_function=self.simple_distance_function, gap_penalty=1)
        self.assertTrue(tt.trees_equivalent(tree1, tree1_expected, self.simple_distance_function), f"""
            Tree 1 did not transform as expected: 
              EXPECTED:
              {tree1_expected.toprettyxml()}

              ACTUAL:
              {tree1.toprettyxml()}
            """)


    def test_alignment_equivalent(self):
        tree = '''
                <root>
                    <measure m_attr1="1" m_attr2="test">
                        <layer l_attr1="1"/>
                        <layer l_attr1="2"/>
                    </measure>
                </root>
                '''
        self.check_tree_transformation(tree, tree, tree)


    def test_alignment_node_missing(self):
        tree1 = '''
        <root>
            <measure m_attr1="1" m_attr2="test">
                <layer l_attr1="2"/>
                <layer l_attr1="3"/>
            </measure>
        </root>
        '''
        tree2 = '''
        <root>
            <measure m_attr1="1" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
            </measure>
        </root>
        '''
        tree1_expected = f'''
        <root>
            <measure m_attr1="1" m_attr2="test">
                <{ta.GAP_ELEMENT_NAME}/>
                <layer l_attr1="2"/>
                <layer l_attr1="3"/>
            </measure>
        </root>
        '''
        tree2_expected = f'''
        <root>
            <measure m_attr1="1" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
                <{ta.GAP_ELEMENT_NAME}/>
            </measure>
        </root>
        '''

        self.check_tree_transformation(tree1, tree2, tree1_expected)
        self.check_tree_transformation(tree2, tree1, tree2_expected)


    def test_alignment_empty_root(self):
        tree1 = "<root/>"
        tree2 = '''
        <root>
            <measure m_attr1="1" m_attr2="test">
                <layer l_attr1="2"/>
                <layer l_attr1="3"/>
            </measure>
        </root>
        '''
        tree1_expected = f'''
        <root>
            <{ta.GAP_ELEMENT_NAME}>
                <{ta.GAP_ELEMENT_NAME}/>
                <{ta.GAP_ELEMENT_NAME}/>
            </{ta.GAP_ELEMENT_NAME}>
        </root>
        '''
        self.check_tree_transformation(tree1, tree2, tree1_expected)
        self.check_tree_transformation(tree2, tree1, tree2)


    def test_alignment_one_onto_many(self):
        tree1 = '''
        <root>
            <measure m_attr1="3" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
            </measure>
        </root>
        '''
        tree2 = '''
        <root>
            <measure m_attr1="1" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
            </measure>
            <measure m_attr1="2" m_attr2="test">
                <layer l_attr1="1"/>
            </measure>
            <measure m_attr1="3" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
                <layer l_attr1="3"/>
            </measure>
            <measure m_attr1="4" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
            </measure>
            <measure m_attr1="5" m_attr2="test">
                <layer l_attr1="1"/>
            </measure>
        </root>
        '''
        tree1_expected = f'''
        <root>
            <{ta.GAP_ELEMENT_NAME}>
                <{ta.GAP_ELEMENT_NAME}/>
                <{ta.GAP_ELEMENT_NAME}/>
            </{ta.GAP_ELEMENT_NAME}>
            <{ta.GAP_ELEMENT_NAME}>
                <{ta.GAP_ELEMENT_NAME}/>
            </{ta.GAP_ELEMENT_NAME}>
            <measure m_attr1="3" m_attr2="test">
                <layer l_attr1="1"/>
                <layer l_attr1="2"/>
                <{ta.GAP_ELEMENT_NAME}/>
            </measure>
            <{ta.GAP_ELEMENT_NAME}>
                <{ta.GAP_ELEMENT_NAME}/>
                <{ta.GAP_ELEMENT_NAME}/>
            </{ta.GAP_ELEMENT_NAME}>
            <{ta.GAP_ELEMENT_NAME}>
                <{ta.GAP_ELEMENT_NAME}/>
            </{ta.GAP_ELEMENT_NAME}>
        </root>
        '''

        self.check_tree_transformation(tree1, tree2, tree1_expected)
        self.check_tree_transformation(tree2, tree1, tree2)


if __name__ == '__main__':
    unittest.main()
