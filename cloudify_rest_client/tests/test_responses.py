import pytest

from cloudify_rest_client.responses import ListResponse


def test_listresponse_interface():
    items = [1, 2, 3]
    lr = ListResponse(items, metadata={})

    assert list(lr) == items

    for ix, value in enumerate(items):
        assert lr[ix] == value

    assert len(lr) == len(items)


def test_listresponse_sort():
    items = [3, 1, 2]
    lr = ListResponse(items, metadata={})

    with pytest.raises(TypeError):
        lr.sort(cmp=lambda x, y: x < y)

    lr.sort()
    assert list(lr) == sorted(items)


def test_listresponse_one():
    with pytest.raises(ValueError):
        ListResponse([], metadata={}).one()

    with pytest.raises(ValueError):
        ListResponse([1, 2, 3], metadata={}).one()

    assert ListResponse([42], metadata={}).one() == 42
