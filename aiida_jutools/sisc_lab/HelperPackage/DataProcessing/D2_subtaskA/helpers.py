# In here you can implement all functions you need within the notebooks
# you can import them via `from .helpers.import <function_namw>` 

# helpers function for subtask d2.a


def print_bold(text: str):
    """Print text in bold.

    :param text: text to print in bold.
    """
    bold_text = f"\033[1m{text}\033[1m"
    print(bold_text)


import numpy as np
import pandas as pd

# aiida imports
from aiida.orm import QueryBuilder as QB
from aiida.orm import WorkFunctionNode, WorkChainNode
from aiida.orm import Dict
from aiida.plugins import DataFactory #, WorkflowFactory
StructureData = DataFactory('structure')


def get_structure_workflow_dict(
            structure_project=['extras.formula', 'uuid'], structure_filters=None,
            workflow_project=['attributes.process_label', 'uuid'], workflow_filters=None,
            dict_project=['uuid'], dict_filters=None):
    '''
    Input the demanding project information and filters information.
    Return all output dict nodes returned by workflows, which had StructureData nodes as inputs are there in the database 
    with the projections and filters given.
    '''
    qb_wfunc = QB() # qb for WorkFunctionNode
    qb_wfunc.append(StructureData, project=structure_project, filters=structure_filters, tag='structure')
    qb_wfunc.append(WorkFunctionNode, project=workflow_project, filters=workflow_filters, tag='work_function',
            with_incoming='structure')
    qb_wfunc.append(Dict, project=dict_project, filters=dict_filters, tag='results', with_incoming='work_function')

    qb_wchain = QB() # qb for WorkChainNode
    qb_wchain.append(StructureData, project=structure_project, filters=structure_filters, tag='structure')
    qb_wchain.append(WorkChainNode, project=workflow_project, filters=workflow_filters, tag='work_chain', 
            with_incoming='structure')
    qb_wchain.append(Dict, project=dict_project, filters=dict_filters, tag='results', with_incoming='work_chain')

    workflowlst = qb_wchain.all() + qb_wchain.all() # Combine into a workflow list
    stlen, wflen, dclen = len(structure_project), len(workflow_project), len(dict_project)
    workflowdictlst = [{'structure': wf[0:stlen], 
            'workflow': wf[stlen:(stlen+wflen)], 
            'dict': wf[(stlen+wflen):(stlen+wflen+dclen)]} for wf in workflowlst] # Transform to a list of dicts

    return workflowdictlst 


def generate_dict_property_pandas_source(
            workflow_name=None, 
            dict_project=['attributes.energy', 'attributes.total_energy', 'attributes.distance_charge']):
    '''
    Given a workflow, generate the dict_project information (which is the output of the workflow) as a pandas object.
    e.g. workflow_name = 'fleur_scf_wc'
    '''
    if not workflow_name:
        workflowdictlst = get_structure_workflow_dict(dict_project=dict_project)
    else:
        workflowdictlst = get_structure_workflow_dict(
                    dict_project=dict_project,
                    workflow_filters={'attributes.process_label':workflow_name})       

    dictlst = [wf['dict'] for wf in workflowdictlst] # Generate a list for Dict nodes
    cleaned_col = [att.split('.')[-1] for att in dict_project]
    dictpd = pd.DataFrame(dictlst, columns=cleaned_col) # Transform into pd DataFrame

    return dictpd


def generate_structure_property_pandas_source(workflow_name=None, structure_project=['uuid', 'extras.formula']):
    '''
    Given a workflow, generate the structure_project information (which is the input of the workflow) as a pandas object.
    e.g. workflow_name = 'fleur_scf_wc'
    '''
    if not workflow_name:
           workflowdictlst = get_structure_workflow_dict(structure_project=structure_project)
    else:
           workflowdictlst = get_structure_workflow_dict(
                structure_project=structure_project,
                workflow_filters={'attributes.process_label':workflow_name})
 
    structurelst = [wf['structure'] for wf in workflowdictlst] # Generate a list for Structure nodes
    cleaned_col = [att.split('.')[-1] for att in structure_project]
    structurepd = pd.DataFrame(structurelst, columns=cleaned_col) # Transform into pd DataFrame

    return structurepd