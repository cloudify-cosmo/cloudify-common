########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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


class Metadata(dict):
    """
    Metadata dict returned by various list operations.
    """

    def __init__(self, metadata):
        self.update(metadata)
        self['pagination'] = Pagination(metadata.get('pagination', {}))

    @property
    def pagination(self):
        """
        :return: The pagination properties
        """
        return self.get('pagination')


class Pagination(dict):
    """
    Pagination properties.
    """
    @property
    def offset(self):
        """
        :return: The list offset.
        """
        return int(self.get('offset'))

    @property
    def size(self):
        """
        :return: The returned page size.
        """
        return int(self.get('size'))

    @property
    def total(self):
        """
        :return: The total number of finds.
        """
        return int(self.get('total'))


class ListResponse(object):

    def __init__(self, items, metadata):
        self.items = items
        self.metadata = Metadata(metadata)

    def __iter__(self):
        return iter(self.items)

    def __getitem__(self, index):
        return self.items[index]

    def __len__(self):
        return len(self.items)

    def sort(self, cmp=None, key=None, reverse=False):
        if cmp is not None:
            raise TypeError('cmp is not supported. Use key instead.')
        return self.items.sort(key=key, reverse=reverse)

    def one(self):
        """Return the only item in this list; error if there isn't just one.

        It is often the case that the caller knows there's only one item
        in the results; instead of doing result[0], use result.one().
        Then, in case there's more results, an error will be thrown for
        the failing assumption, rather than silently continuing, which
        could lead to subtle bugs.
        """
        if len(self) != 1:
            raise ValueError(
                'Called one() on a list with {0} items'.format(len(self)))
        return self[0]


class DeletedResponse(object):
    """
    Describes outcome of a deletion: number of 'deleted' records.
    """
    def __init__(self, deleted):
        self.deleted = deleted
