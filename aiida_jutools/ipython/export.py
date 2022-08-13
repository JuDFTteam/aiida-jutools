"""
Magics class for exporting aiida nodes from a jupyer notebook environment
"""
from IPython.core.magic import magics_class, line_magic, needs_local_scope
from aiida import orm
from collections import defaultdict
import re
import yaml

IPYTHON_VARIABLE = re.compile('_+[0-9]*')

@magics_class
class NotebookAiidaExport:

    @line_magic
    @needs_local_scope
    def export_nodes(self, line, local_ns):

        args = line.split(' ', maxsplit=1)
        group_name = args[0]

        if len(args) == 2:
            if args[1].lower() not in ('pk', 'uuid'):
                raise ValueError('Invalid value for export format (Either pk or uuid)')
            export_format = args[1].lower()
        else:
            export_format = 'uuid'
        
        #Get all AiiDA nodes in the local namespace
        aiida_nodes = defaultdict(set)
        for name, node in local_ns.items():
            if isinstance(node, orm.Node):
                aiida_nodes[node].add(name)
        
        aiida_nodes_one_name = {}
        #Now choose the names and prefer proper variable names over _12, etc. (ipython utilities)
        export_dict = defaultdict(list)
        for node, names in aiida_nodes.items():
            class_name = getattr(node, 'process_class', node.__class__).__name__
            identifier = node.pk if export_format == 'pk' else node.uuid
            if len(names) == 1:
                name = names.pop()
                export_dict[class_name].append([name, identifier])
                aiida_nodes_one_name[name] = node
            elif len({name for name in names if not re.fullmatch(IPYTHON_VARIABLE, name)}) != 0:
                name = {name for name in names if not re.fullmatch(IPYTHON_VARIABLE, name)}.pop()
                export_dict[class_name].append([name, identifier])
                aiida_nodes_one_name[name] = node
            else:
                name = names.pop()
                export_dict[class_name].append([name, identifier])
                aiida_nodes_one_name[name] = node
                
        
        group, _ = orm.Group.objects.get_or_create(f'notebook-exports/{group_name}')
        for name, node in aiida_nodes_one_name.items():
            node.extras['notebook-variable'] = name
            node.extras['notebook-origin'] = group_name
            group.add_nodes(node)
        
        with open(f'{group_name}-nodes.yml', 'w') as file:
            yaml.dump(dict(export_dict), file)
