import copy


class VisibilityState(object):
    PRIVATE = 'private'
    TENANT = 'tenant'
    GLOBAL = 'global'

    STATES = [PRIVATE, TENANT, GLOBAL]


states_except_private = copy.deepcopy(VisibilityState.STATES)
states_except_private.remove(VisibilityState.PRIVATE)
VISIBILITY_EXCEPT_PRIVATE = states_except_private

states_except_global = copy.deepcopy(VisibilityState.STATES)
states_except_global.remove(VisibilityState.GLOBAL)
