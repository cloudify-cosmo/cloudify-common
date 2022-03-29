import unittest

from dsl_parser.functions import _merge_function_args


class TestGetAttributeHelperFunctions(unittest.TestCase):
    def test_merge_function_args_simple(self):
        assert _merge_function_args([]) == []
        assert _merge_function_args(['a', 'b', 'c']) == ['a', 'b', 'c']

    def test_merge_function_args_one_function(self):
        assert _merge_function_args(
            [{'get_input': ['test_tmp', 'a']}, 'b', 'c']
        ) == {'get_input': ['test_tmp', 'a', 'b', 'c']}
        assert _merge_function_args(
            ['a', {'get_attribute': ['test_tmp', 'b']}, 'c']
        ) == ['a', {'get_attribute': ['test_tmp', 'b', 'c']}]
        assert _merge_function_args(
            ['a', 'b', {'get_capability': ['test_tmp', 'c']}]
        ) == ['a', 'b', {'get_capability': ['test_tmp', 'c']}]

    def test_merge_function_args_two_functions(self):
        assert _merge_function_args(
            [{'get_attribute': ['test_tmp', 'a']}, 'b',
             {'get_attribute': ['test_tmp', 'c']}]
        ) == {'get_attribute': [
            'test_tmp', 'a', 'b', {'get_attribute': ['test_tmp', 'c']}
        ]}
        assert _merge_function_args(
            [{'get_input': ['test_tmp', 'a']}, 'b', 'c',
             {'get_attribute': ['test_tmp', 'd']}, 'e', 'f', 'g']
        ) == {'get_input': [
            'test_tmp', 'a', 'b', 'c',
            {'get_attribute': ['test_tmp', 'd']}, 'e', 'f', 'g'
        ]}
