########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
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


class Holder(object):

    def __init__(self,
                 value,
                 start_line=None,
                 start_column=None,
                 end_line=None,
                 end_column=None,
                 filename=None,
                 namespace=None,
                 is_cloudify_type=False,
                 only_children_namespace=False):
        self.value = value
        self.start_line = start_line
        self.start_column = start_column
        self.end_line = end_line
        self.end_column = end_column
        self.filename = filename
        self.namespace = namespace
        self.is_cloudify_type = is_cloudify_type

        # This flag will mark that the namespace scope is only
        # applied on the holder (/DSL element) children.
        self.only_children_namespace = only_children_namespace

    def __str__(self):
        return '{0}<{1}.{2}-{3}.{4} [{5}]>'.format(
            self.value,
            self.start_line,
            self.start_column,
            self.end_line,
            self.end_column,
            self.filename)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.value)

    def __eq__(self, other):
        return isinstance(other, Holder) and self.value == other.value

    def __contains__(self, key):
        key_holder, value_holder = self.get_item(key)
        return value_holder is not None

    def __getitem__(self, key):
        key_holder, value = self.get_item(key)
        if not value:
            raise KeyError("The expected key {0} does not exists"
                           .format(key_holder.value))
        return value

    def get_item(self, key):
        if not isinstance(self.value, dict):
            raise ValueError('Value is expected to be a dictionary while it '
                             'is of type {0}'
                             .format(type(self.value).__name__))
        for key_holder, value_holder in self.value.items():
            if key_holder.value == key:
                return key_holder, value_holder
        return None, None

    def set_item(self, key, value):
        if not isinstance(self.value, dict):
            raise ValueError('Value is expected to be a dictionary while it '
                             'is of type {0}'
                             .format(type(self.value).__name__))
        self.value.update({
            Holder.of(key): Holder.of(value)
        })

    def restore(self):
        if isinstance(self.value, dict):
            return dict((key_holder.restore(), value_holder.restore())
                        for key_holder, value_holder in self.value.items())
        elif isinstance(self.value, list):
            return [value_holder.restore() for value_holder in self.value]
        elif isinstance(self.value, set):
            return set((value_holder.restore() for value_holder in self.value))
        else:
            return self.value

    @staticmethod
    def of(obj, filename=None):
        if isinstance(obj, Holder):
            return obj
        if isinstance(obj, dict):
            result = dict((Holder.of(key, filename=filename),
                           Holder.of(value, filename=filename))
                          for key, value in obj.items())
        elif isinstance(obj, list):
            result = [Holder.of(item, filename=filename) for item in obj]
        elif isinstance(obj, set):
            result = set((Holder.of(item, filename=filename) for item in obj))
        else:
            result = obj
        return Holder(result, filename=filename)

    def copy(self):
        return Holder(value=self.value,
                      start_line=self.start_line,
                      start_column=self.start_column,
                      end_line=self.end_line,
                      end_column=self.end_column,
                      filename=self.filename,
                      namespace=self.namespace,
                      is_cloudify_type=self.is_cloudify_type,
                      only_children_namespace=self.only_children_namespace)

    def keys(self):
        if not isinstance(self.value, dict):
            raise ValueError('Value is expected to be a dictionary while it '
                             'is of type {0}'
                             .format(type(self.value).__name__))
        return [k.value for k in self.value.keys()]
