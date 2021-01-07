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


    
# Interactive visualize by Bokeh
from bokeh.io import output_file
from bokeh.layouts import gridplot
from bokeh.models import ColumnDataSource
from bokeh.models.tools import HoverTool, BoxSelectTool
from bokeh.plotting import figure, show


def bokeh_struc_prop_vis(xdata, ydata, filename='bokeh_visualization.html'):
    filename = filename
    output_file(filename)
    TOOLS="pan, wheel_zoom, box_select, reset"

    # Scatter plot
    p = figure(plot_width=600, plot_height=600,
            toolbar_location="above", x_axis_location=None, y_axis_location=None,
            title="Linked Histograms", tools=TOOLS)
    r = p.scatter(xdata, ydata, color="blue", alpha=0.6)

    # Horizontal histogram
    hhist, hedges = np.histogram(xdata, bins=20)
    hzeros = np.zeros(len(hedges)-1)
    hmax = max(hhist)*1.1
    hsource = ColumnDataSource(dict(left=hedges[:-1], right=hedges[1:], top=hhist, bottom=np.zeros(hhist.shape)))
    # Settings
    ph = figure(toolbar_location=None, plot_width=p.plot_width, plot_height=200, x_range=p.x_range,
                y_range=(-1, hmax), min_border=10, min_border_left=50, y_axis_location="right")
    ph.xgrid.grid_line_color = None
    ph.yaxis.major_label_orientation = np.pi/4
    # Render
    ph.quad(bottom="bottom", left="left", right="right", top="top", color="blue", alpha=0.4, source=hsource)

    # Vertical histogram
    vhist, vedges = np.histogram(ydata, bins=20)
    vzeros = np.zeros(len(vedges)-1)
    vmax = max(vhist)*1.1
    vsource = ColumnDataSource(dict(left=np.zeros(vhist.shape), right=vhist, top=vedges[1:], bottom=vedges[:-1]))
    # Settings
    pv = figure(toolbar_location=None, plot_width=200, plot_height=p.plot_height, x_range=(-1, vmax),
                y_range=p.y_range, min_border=10, y_axis_location="right")
    pv.ygrid.grid_line_color = None
    pv.xaxis.major_label_orientation = np.pi/4
    # Render
    pv.quad(left="left", bottom="bottom", top="top", right="right", color="blue", alpha=0.4, source=vsource)


    # TODO: Add hover tools (should add structure info in the output dict file of d1 first)




    # Show plots
    layout = gridplot([[p, pv], [ph, None]], merge_tools=False)
    show(layout)

