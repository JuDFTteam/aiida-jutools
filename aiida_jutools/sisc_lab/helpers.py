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

from collections import Counter
from math import pi
import pandas as pd
from pandas import DataFrame
import numpy as np
from bokeh.io import output_file,output_notebook, show
from bokeh.layouts import column
from bokeh.palettes import Category20,Category20c
from bokeh.plotting import figure,ColumnDataSource
from bokeh.transform import cumsum
from bokeh.models import Legend,LegendItem,HoverTool

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
    
    
#D1.b

def print_Count(types,res):
    if types=='user':
        dict_type = Counter([r[4] for r in res])
    elif types=='types':
        dict_type = Counter([r[3] for r in res])
    for count, name in sorted((v, k) for k, v in dict_type.items())[::-1]:
        print("- {} created {} nodes".format(name, count))
        
                
#D1.c

#split data nodes and process nodes
def get_data_node_count(types,node_type):
    labelst,sizest=[],[]
    for k,v in types.items():
        if k.split('.')[0]==node_type:
            labelst.append(k.split('.')[-2])
            sizest.append(v)
    x = dict(zip(labelst,sizest))
    return x

def get_process_node_count(types,node_type):
    q = QB()
    q.append(ProcessNode)
    pro = q.iterall()
    nodetypes = Counter([p[0].process_label for p in pro if '_' not in p[0].process_label])
    workchain={k:v for k,v in nodetypes.items() if k.endswith('WorkChain')}
    calculation={k:v for k,v in nodetypes.items() if k.endswith('Calculation')}
    workfunction={k:v for k,v in nodetypes.items() if k.endswith('WorkFunction')}

    x1 = get_node_count(types,node_type)
    for k,v in nodetypes.items():
        if k.endswith('WorkChain'):
            x1.pop('WorkChainNode',None)
            x1.update(**workchain)
        elif k.endswith('Calculation'):
            x1.pop('CalcJobNode',None)
            x1.update(**calculation)
        elif k.endswith('WorkFunction'):
            x1.pop('WorkfunctionNode',None)
            x1.update(**workfuction)
    return x1

def draw_pie_chart(x,title):
    data=pd.DataFrame.from_dict(dict(x),orient='index').reset_index().rename(index=str,columns={0:'value','index':'data_nodes'})
    data['angle'] = data['value']/sum(list(x.values())) * 2*pi
    data['color'] = Category20[len(x)]
    data['percent']=data["value"]/sum(x.values())
    p = figure(title=title, toolbar_location=None,
           tools="hover", tooltips=[('Data','@data_nodes'),('Percent','@percent{0.00%}'),('Count','@value')])
    p.wedge(x=0, y=1, radius=0.4,
        start_angle=cumsum('angle', include_zero=True), end_angle=cumsum('angle'),
        line_color="white", fill_color='color', legend_field='data_nodes', source=data)
    p.axis.axis_label=None
    p.axis.visible=False
    p.grid.grid_line_color = None 
    return p

def get_dict_link_types():
    link_labels={}
    xl = []
    q = QB()
    q.append(Dict)
    dicts = q.iterall()

    for node in dicts:
        if len(node[0].get_incoming().all_link_labels())>1 :
            link_labels = Counter(node[0].get_incoming().all_link_labels())
            for key,value in link_labels.items():
                if value>1:
                    xl.append(key)
    return xl


#D1.d

# line plot by ctime & mtime
def draw_line_plot(users,res):
    #ctime & mtime for total
    ctimes = sorted(r[1] for r in res)
    mtimes = sorted(r[2] for r in res)
    num_nodes_integrated = range(len(ctimes))
    df = pd.DataFrame({'A':ctimes,"B":mtimes})

    p = figure(x_axis_type='datetime',y_axis_type='log')
    r=p.multi_line([df['A'], df['B']],  
                   [df.index, df.index],   
                   color=['red','blue'],      
                   alpha=[0.8, 0.6],     
                   line_width=[2,2],     
                   )

    legend=Legend(items=[
            LegendItem(label='ctime',renderers=[r],index=0),
            LegendItem(label='mtime',renderers=[r],index=1),
            ])

    p.add_layout(legend)
    p.xaxis.axis_label = 'Date'
    p.yaxis.axis_label = 'Number of nodes'
    p.yaxis.axis_label_text_font_size = "15pt"
    show(p)

    #ctime & mtime for each user
    for count, email in sorted((v, k) for k, v in users.items())[::-1]:
        ctimes = sorted(r[1] for r in res if r[4] in email)
        mtimes = sorted(r[2] for r in res if r[4] in email)
        num_nodes_integrated = range(len(ctimes))
        df = pd.DataFrame({'A':ctimes,"B":mtimes})

        p = figure(x_axis_type='datetime',y_axis_type='log')
        r=p.multi_line([df['A'], df['B']],  
                   [df.index, df.index],   
                   color=['red','blue'],      
                   alpha=[0.8, 0.6],     
                   line_width=[2,2],     
                   )

        legend=Legend(items=[
            LegendItem(label=email+':ctime',renderers=[r],index=0),
            LegendItem(label=email+':mtime',renderers=[r],index=1),
            ])

        p.add_layout(legend)
        p.xaxis.axis_label = 'Date'
        p.yaxis.axis_label = 'Number of nodes'
        p.yaxis.axis_label_text_font_size = "15pt"
        show(p)

