from cloudify import ctx
from cloudify.state import ctx_parameters as p

ctx.returns(p.key)
