########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import testtools
from mock import patch, MagicMock

from dsl_parser import exceptions
from dsl_parser import constraints


class TestGeneralUtilFunctions(testtools.TestCase):
    def test_validate_args_raises(self):
        validation_funcs = {'some_type': lambda _: False}
        with patch('dsl_parser.constraints.VALIDATION_FUNCTIONS',
                   validation_funcs):
            constraint_cls_mock = MagicMock()
            constraint_cls_mock.constraint_data_type = 'some_type'
            self.assertRaises(
                exceptions.DSLParsingLogicException,
                constraints.validate_args,
                constraint_cls_mock,
                None,
                None)

    def test_validate_args_raises_with_func_as_input(self):
        constraint_cls_mock = MagicMock()
        constraint_cls_mock.constraint_data_type = 'some_type'
        self.assertRaises(
            exceptions.DSLParsingLogicException,
            constraints.validate_args,
            constraint_cls_mock,
            {'get_input': 'shouldnt matter'},
            None)

    def test_validate_args_successful(self):
        validation_funcs_mock = {'some_type': lambda _: True}
        with patch.dict('dsl_parser.constraints.VALIDATION_FUNCTIONS',
                        validation_funcs_mock):
            constraint_cls_mock = MagicMock()
            constraint_cls_mock.constraint_data_type = 'some_type'
            constraints.validate_args(constraint_cls_mock, None, None)

    def test_is_valid_regex_successful(self):
        self.assertTrue(constraints.is_valid_regex('^valid_regex$'))

    def test_is_valid_regex_bad_bad_regex(self):
        self.assertFalse(constraints.is_valid_regex('\\'))

    def test_is_valid_regex_not_string(self):
        self.assertFalse(constraints.is_valid_regex(0.0))

    def test_is_valid_list_successful(self):
        self.assertTrue(constraints.is_valid_sequence([]))

    def test_is_valid_list_not_successful(self):
        self.assertFalse(constraints.is_valid_sequence('string'))

    def test_is_valid_dual_scalar_successful(self):
        self.assertTrue(constraints.is_valid_dual_scalar([0, 0.0]))

    def test_is_valid_dual_scalar_bad_first_arg(self):
        self.assertFalse(constraints.is_valid_dual_scalar(['not_a_scalar', 0]))

    def test_is_valid_dual_scalar_bad_second_arg(self):
        self.assertFalse(constraints.is_valid_dual_scalar([0, 'not_a_scalar']))

    def test_is_valid_scalar_successful(self):
        self.assertTrue(constraints.is_valid_scalar(0))

    def test_is_valid_scalar_not_successful(self):
        self.assertFalse(constraints.is_valid_scalar('not_a_scalar'))
        self.assertFalse(constraints.is_valid_scalar(True))

    def test_register_constraint_successful(self):
        constraints_dict = {}
        with patch('dsl_parser.constraints.CONSTRAINTS', constraints_dict):
            @constraints.register_constraint(
                name='name1', constraint_data_type=-2)
            class A(constraints.Constraint):
                name = 'wrong'
                constraint_data_type = -1

                def predicate(self, _):
                    return False

            self.assertEqual(A.name, 'name1')
            self.assertEqual(A.constraint_data_type, -2)
            self.assertTrue('name1' in constraints_dict)

            constraints.register_constraint(
                name='name2', constraint_data_type=-3, cls=A)
            self.assertEqual(A.name, 'name2')
            self.assertEqual(A.constraint_data_type, -3)
            self.assertTrue('name2' in constraints_dict)

    def test_register_validation_func(self):
        validation_funcs = {}
        with patch('dsl_parser.constraints.VALIDATION_FUNCTIONS',
                   validation_funcs):
            @constraints.register_validation_func('type1')
            def f():
                return 'f'

            self.assertTrue('type1' in validation_funcs)
            self.assertEqual(f(), validation_funcs['type1']())

            constraints.register_validation_func('type2', f=f)
            self.assertTrue('type2' in validation_funcs)
            self.assertEqual(f(), validation_funcs['type2']())


class TestConstraint(testtools.TestCase):
    def test_str(self):
        class DummyConstraint(constraints.Constraint):
            name = 'name'

            def predicate(self, value):
                return True

        constraint = DummyConstraint('arg')
        constraint.name = 'name'
        self.assertEqual('name(arg) operator', str(constraint))
        constraint = DummyConstraint(['arg1', 123])
        constraint.name = 'name'
        self.assertEqual('name(arg1, 123) operator', str(constraint))


class TestEqual(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.Equal(3)
        self.assertTrue(dummy.predicate(3))
        self.assertFalse(dummy.predicate(2))

    def test_raises_error(self):
        class A(object):
            def __eq__(self, other):
                raise TypeError()

        dummy = constraints.Equal(3)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestGreaterThan(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.GreaterThan(3)
        self.assertTrue(dummy.predicate(5))
        self.assertFalse(dummy.predicate(3))

    def test_raises_error(self):
        class A(object):
            def __gt__(self, other):
                raise TypeError()

        dummy = constraints.GreaterThan(3)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestGreaterOrEqual(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.GreaterOrEqual(3)
        self.assertTrue(dummy.predicate(5))
        self.assertFalse(dummy.predicate(2))

    def test_raises_error(self):
        class A(object):
            def __ge__(self, other):
                raise TypeError()

        dummy = constraints.GreaterOrEqual(3)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestLessThan(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.LessThan(3)
        self.assertTrue(dummy.predicate(2))
        self.assertFalse(dummy.predicate(3))

    def test_raises_error(self):
        class A(object):
            def __lt__(self, other):
                raise TypeError()

        dummy = constraints.LessThan(3)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestLessOrEqual(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.LessOrEqual(3)
        self.assertTrue(dummy.predicate(2))
        self.assertFalse(dummy.predicate(4))

    def test_raises_error(self):
        class A(object):
            def __le__(self, other):
                raise TypeError()

        dummy = constraints.LessOrEqual(3)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestInRange(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.InRange([0, 5])
        self.assertTrue(dummy.predicate(2))
        self.assertFalse(dummy.predicate(-4))

    def test_raises_error(self):
        class A(object):
            def __ge__(self, item):
                raise TypeError()

            def __le__(self, item):
                raise TypeError()

        dummy = constraints.InRange([0, 5])
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestValidValues(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.ValidValues([0, 2])
        self.assertTrue(dummy.predicate(0))
        self.assertFalse(dummy.predicate(1))

    def test_raises_error(self):
        class A(object):
            def __eq__(self, item):
                raise TypeError()

        dummy = constraints.ValidValues([0, 2])
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestLength(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.Length(1)
        self.assertTrue(dummy.predicate([0]))
        self.assertFalse(dummy.predicate([]))

    def test_raises_error(self):
        class A(object):
            def __len__(self):
                raise TypeError()

        dummy = constraints.Length(2)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestMinLength(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.MinLength(1)
        self.assertTrue(dummy.predicate([0, 1]))
        self.assertFalse(dummy.predicate([]))

    def test_raises_error(self):
        class A(object):
            def __len__(self):
                raise TypeError()

        dummy = constraints.MinLength(2)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestMaxLength(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.MaxLength(1)
        self.assertTrue(dummy.predicate([0]))
        self.assertFalse(dummy.predicate([0, 1]))

    def test_raises_error(self):
        class A(object):
            def __len__(self):
                raise TypeError()

        dummy = constraints.MaxLength(1)
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, A())


class TestPattern(testtools.TestCase):

    def test_successful(self):
        dummy = constraints.Pattern('a{3}b{1}c+')
        self.assertTrue(dummy.predicate('aaabc'))
        self.assertTrue(dummy.predicate('aaabcce'))
        self.assertFalse(dummy.predicate('cba'))
        self.assertFalse(dummy.predicate('aaabe'))

    def test_raises_error(self):
        dummy = constraints.Pattern('')
        self.assertRaises(exceptions.ConstraintException, dummy.predicate, 123)
