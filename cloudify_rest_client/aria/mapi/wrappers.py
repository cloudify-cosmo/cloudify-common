########
# Copyright (c) 2017 GigaSpaces Technologies Ltd. All rights reserved
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


class WrapperBase(object):

    def __init__(self, _attribute_path, _query, **_):
        self.__dict__['_attribute_path'] = _attribute_path
        self.__dict__['_query'] = _query

    @property
    def _attribute_path(self):
        return self.__dict__['_attribute_path']

    @property
    def _query(self):
        return self.__dict__['_query']


class ModelWrapper(WrapperBase):
    def __init__(self, obj, **kwargs):
        WrapperBase.__init__(self, **kwargs)
        self.__dict__['_obj'] = obj

    def __getattr__(self, item):
        try:
            return getattr(self._obj, item)
        except AttributeError:
            return self._query('/'.join([self._attribute_path, item]))

    def __setattr__(self, key, value):
        setattr(self._obj, key, value)

    def __eq__(self, other):
        return (isinstance(other, ModelWrapper) and
                other._obj.id == self._obj.id and
                other._obj.__class__ == self._obj.__class__)


class DictWrapper(dict, WrapperBase):

    def __init__(self, _attribute_path, _query, **kwargs):
        WrapperBase.__init__(self,
                             _attribute_path=_attribute_path,
                             _query=_query)
        dict.__init__(self, **kwargs)

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            return self._query('/'.join([self._attribute_path, str(item)]))

    def values(self):
        return list(
            v
            for k, v in dict.items(self)
            if k not in ('_attribute_path', '_query')
        )

    def itervalues(self):
        return iter(self.values())

    def items(self):
        return [
            (k, v)
            for k, v in dict.items(self)
            if k not in ('_attribute_path', '_query')
        ]

    def iteritems(self):
        return iter(self.items())

    def keys(self):
        return [
            k
            for k in dict.keys(self)
            if k not in ('_attribute_path', '_query')
        ]

    def iterkeys(self):
        return iter(self.keys())

    def __iter__(self):
        return self.iterkeys()


class ListWrapper(list, WrapperBase):

    def __init__(self, seq=None, **kwargs):
        WrapperBase.__init__(self, **kwargs)
        list.__init__(self, seq)

    def __getitem__(self, item):
        try:
            return list.__getitem__(self, item)
        except KeyError:
            return self._query('/'.join([self._attribute_path, str(item)]))
