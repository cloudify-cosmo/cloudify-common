from cloudify.workflows import ctx
from cloudify.workflows import parameters as p

node = next(ctx.nodes)
instance = next(node.instances)
ctx.returns(instance.execute_operation('test.script', kwargs={
    'script_path': 'script.py',
    'key': p.key
}, allow_kwargs_override=True).get())
