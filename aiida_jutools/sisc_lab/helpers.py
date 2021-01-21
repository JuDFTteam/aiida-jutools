# In here you can implement all functions you need within the notebooks
# you can import them via `from .helpers.import <function_namw>` 

# helpers function for subtask d2.a


def print_bold(text: str):
    """Print text in bold.

    :param text: text to print in bold.
    """
    bold_text = f"\033[1m{text}\033[1m"
    print(bold_text)


# python imports
import numpy as np
import pandas as pd
from collections import Counter
from math import pi
from pandas import DataFrame


# aiida imports
from aiida.orm import QueryBuilder as QB
from aiida.orm import WorkFunctionNode, WorkChainNode
from aiida.orm import Dict
from aiida.plugins import DataFactory #, WorkflowFactory
StructureData = DataFactory('structure')


def get_structure_workflow_dict(
            structure_project=['uuid', 'extras.formula'], structure_filters=None,
            workflow_project=['uuid', 'attributes.process_label'], workflow_filters=None,
            dict_project=['uuid'], dict_filters=None):
    '''
    Input the demanding project information and filters information.
    Return all output dict nodes returned by workflows, which had StructureData nodes as inputs are there in the database 
    with the projections and filters given.
    The output is a list of dicts
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
            dict_project=['attributes.energy', 'attributes.total_energy', 'attributes.distance_charge'],
            filename=None):
    '''
    Given a workflow, generate the dict_project property (which is the output of the workflow) as a pandas object, 
    and write it into a json file.
    e.g. workflow_name='fleur_scf_wc', filename='dict_property.json'
    '''
    if not workflow_name:
        workflowdictlst = get_structure_workflow_dict(dict_project=dict_project)
    else:
        workflowdictlst = get_structure_workflow_dict(
                    dict_project=dict_project,
                    workflow_filters={'attributes.process_label':workflow_name})       

    dictlst = [wf['dict'] for wf in workflowdictlst] # Generate a list for Dict nodes
    cleaned_col = [att.split('.')[-1] for att in dict_project] # Re-define the column name
    try: # Check if uuid information exists
        idx = cleaned_col.index('uuid')
    except ValueError:
        pass
    else:
        cleaned_col[idx] = 'dict_uuid' # Change uuid column name
        for info in dictlst: # Shorten the uuid to its first part
            info[0] = info[0].split('-')[0]
    dictpd = pd.DataFrame(dictlst, columns=cleaned_col) # Transform list into pd DataFrame
    
    if filename:
        dictpd.to_json(filename, orient='records')

    return dictpd


def generate_structure_property_pandas_source(
            workflow_name=None, 
            structure_project=['uuid', 'extras.formula'],
            filename=None):
    '''
    Given a workflow, generate the structure_project property (which is the input of the workflow) as a pandas object
    and write it into a json file.
    e.g. workflow_name='fleur_scf_wc', filename='structure_property.json'
    '''
    if not workflow_name:
           workflowdictlst = get_structure_workflow_dict(structure_project=structure_project)
    else:
           workflowdictlst = get_structure_workflow_dict(
                structure_project=structure_project,
                workflow_filters={'attributes.process_label':workflow_name})
 
    structurelst = [wf['structure'] for wf in workflowdictlst] # Generate a list for Structure nodes
    cleaned_col = [att.split('.')[-1] for att in structure_project] # Re-define the column name
    try: # Check if uuid exists
        idx = cleaned_col.index('uuid')
    except ValueError:
        pass
    else:
        cleaned_col[idx] = 'structure_uuid' # Change the uuid column name
        for info in structurelst: # Shorten the uuid to its first part
            info[0] = info[0].split('-')[0]
    structurepd = pd.DataFrame(structurelst, columns=cleaned_col) # Transform into pd DataFrame

    if filename:
        structurepd.to_json(filename, orient='records')

    return structurepd


def generate_combination_property_pandas_source(
            workflow_name=None, 
            dict_project=['attributes.energy', 'attributes.total_energy', 'attributes.distance_charge'],
            structure_project=['uuid', 'extras.formula'],
            filename=None):
    '''
    Given a workflow, generate the combination of dict_project and structure_project property as a pandas object, 
    and write it into a json file.
    e.g. workflow_name='fleur_scf_wc', filename='combination_property.json'
    '''
    dictpd = generate_dict_property_pandas_source(workflow_name,dict_project=dict_project)
    structurepd = generate_structure_property_pandas_source(workflow_name, structure_project=structure_project)
    combinepd = pd.concat([dictpd, structurepd], axis=1)
    
    if filename:
        combinepd.to_json(filename, orient='records')

    return combinepd


    
# D2 interactive visualize by Bokeh imports
from bokeh.io import output_file
from bokeh.layouts import gridplot
from bokeh.models import ColumnDataSource
from bokeh.models.tools import HoverTool, BoxSelectTool
from bokeh.plotting import figure, show


def read_json_file(filename, xcol, ycol):
    '''
    Read the dataset from the file.
    Use xcol, ycol to specify the columns you want. The function will filter out the missing values of 
    these columns.
    Return df(the entire dataframe of the file after filtering), and the data of each column.
    '''
    try: # Check if the file could successfully opened
        df = pd.read_json(filename, orient='records')
        try: # Check if xcol exists
            df.dropna(axis=0, how='any', subset=[xcol], inplace=True)
        except KeyError:
            print("Column '{}' not found.".format(xcol))
            xcol, xdata = None, None
        try: # Check if ycol exists
            df.dropna(axis=0, how='any', subset=[ycol], inplace=True)
        except KeyError:
            print("Column '{}' not found.".format(ycol))
            ycol, ydata = None, None
    except ValueError:
        print("Invalid file '{}'.".format(filename))
        df, xdata, ydata, xcol, ycol = None, None, None, None, None
        #raise
    else:
        df.reset_index(drop=True, inplace=True)
    
    if (xcol != None):
        xdata = df[xcol]
    if (ycol != None):
        ydata = df[ycol]

    return df, xdata, ydata


def bokeh_struc_prop_vis(input_filename, xcol, ycol, output_filename='bokeh_visualization.html'):
    '''
        Create Bokeh Interactive scatter-histogram graphs for the xcol and ycol data from the input file.
        Hover tools included.
        The return plot file is saved in html format by default.
    '''
    # IO and other settings
    df, xdata, ydata = read_json_file(input_filename, xcol, ycol)
    output_file(output_filename)
    TOOLS="pan, wheel_zoom, box_select, reset"
    
    # Create the scatter plot
    TOOLTIPS=[("Index", "$index"), ("Sturture_node uuid:", "@structure_uuid"), ("Input formula:", "@formula"),
            ("Dict_node uuid:", "@dict_uuid")] # Hover tools
    source = ColumnDataSource(df)
    # Settings
    p = figure(plot_width=600, plot_height=600,
            toolbar_location="above", x_axis_location=None, y_axis_location=None,
            title="Linked Histograms", tools=TOOLS, tooltips=TOOLTIPS)
    # Render
    r = p.circle(xcol, ycol, color="blue", alpha=0.6, selection_color="red", selection_fill_alpha=0.8,
               nonselection_fill_color="grey", nonselection_fill_alpha=0.2, source=source)

    # Create the horizontal histogram
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
    ph.quad(bottom="bottom", left="left", right="right", top="top", color="blue", alpha=0.6, source=hsource)

    # Create the vertical histogram
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
    pv.quad(left="left", bottom="bottom", top="top", right="right", color="blue", alpha=0.6, source=vsource)

    # Show plots
    layout = gridplot([[p, pv], [ph, None]], merge_tools=False)
    show(layout)
  


# D1 imports
from bokeh.io import output_file,output_notebook, show
from bokeh.layouts import column
from bokeh.palettes import Category20,Category20c
from bokeh.plotting import figure,ColumnDataSource
from bokeh.transform import cumsum
from bokeh.models import Legend,LegendItem,HoverTool


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

