# -*- coding: utf-8 -*-
'''
In here you can implement all functions you need within the notebooks
you can import them via `from .helpers.import <function_namw>`

helpers function for subtask d2.a
'''

# python imports
import numpy as np
import pandas as pd
from collections import Counter
from math import pi
from pandas import DataFrame
import time

# D1 imports
from bokeh.io import output_file, output_notebook, show
from bokeh.layouts import column
from bokeh.palettes import Category20, Category20c,Spectral11
from bokeh.plotting import figure, ColumnDataSource
from bokeh.transform import cumsum
from bokeh.models import Legend, LegendItem, HoverTool,ColumnDataSource

# D2 interactive visualize by Bokeh imports
from bokeh.io import output_file, show, curdoc
from bokeh.layouts import gridplot, column, row
from bokeh.models import ColumnDataSource, Select, Legend
from bokeh.models.tools import HoverTool, BoxSelectTool
from bokeh.plotting import figure, show


# # aiida imports
# from aiida.orm import QueryBuilder as QB
# from aiida.orm import WorkFunctionNode, WorkChainNode
# from aiida.orm import Dict, ProcessNode
# from aiida.plugins import DataFactory  #, WorkflowFactory
# StructureData = DataFactory('structure')


# 16:9
FIGURE_HEIGHT = 540
FIGURE_WIDTH = 960

def print_bold(text: str):
    """Print text in bold.

    :param text: text to print in bold.
    """
    bold_text = f'\033[1m{text}\033[1m'
    print(bold_text)


MAP = {'workflow_0.2.2':'wf_0_2_2', 
      'workflow_0.3.0':'wf_0_3_0',
      'workflow_0.4.2':'wf_0_4_2',
      'workflow_0.8.0':'wf_0_8_0',
      'workflow_0.9.4':'wf_0_9_4',
      'workflow_0.10.4':'wf_0_10_4',
      'workflow_0.12.0':'wf_0_12_0',
      'parser_AiiDA Fleur Parser v0.3.0':'ps_0_3_0',
      'parser_AiiDA Fleur Parser v0.3.1':'ps_0_3_1',
      'parser_AiiDA Fleur Parser v0.3.2':'ps_0_3_2',
      'parser_0.4.2':'ps_0_4_2',
      'parser_0.6.6':'ps_0_6_6'}
INVMAP = {value:key for key, value in MAP.items()}


class StandardWorkflow():

    def __init__(self):
        self.workflow_list = {}

    def show_workflow_list(self):
        return list(self.workflow_list.keys())

    def add_workflow(self, *workflows):
        for workflow in workflows:
            self.workflow_list[workflow.workflow_version_name] = workflow

    def get_workflow(self, workflow_version_name):
        return self.workflow_list[workflow_version_name]
    
    def del_workflow(self, *workflows):
        for workflow in workflows:
            del self.workflow_list[str(workflow)]

class WorkflowProjections():
    '''Standard projections for each workflow/parse version'''
    def __init__(self, proj, workflow_version_name):
        '''
        Format  of self.proj: ["uuid", "attributes.workflow_version", "attrbutes.attr1","attrbutes.attr1_units"
        "attrbutes.attr2","attrbutes.attr2_units'",...]
        Have to have a pair of [attr, attr_units] for each atribute.      
        '''
        self.projections = proj
        self.workflow_version_name = workflow_version_name

    def set_projections(self, proj):
        self.projections = proj
        
    def add_projections(self, new_proj):
        if (isinstance(new_proj) & (len(new_proj)==22)):
            self.projections = self.projections + new_proj

    def get_projections(self):
        print(self.projections)

# Workflow
wf_0_2_2 = WorkflowProjections(['uuid', 'attributes.workflow_version','attributes.force',
                            'attributes.force_units', 'attributes.energy', 'attributes.energy_units'],
                            'wf_0_2_2')
wf_0_3_0 = WorkflowProjections(['uuid', 'attributes.workflow_version', 'attributes.energy', 'attributes.energy_units',
                             'attributes.total_magnetic_moment_cell', 'attributes.total_magnetic_moment_cell_units'],
                             'wf_0_3_0')
wf_0_4_2 = WorkflowProjections(['uuid', 'attributes.workflow_version', 'attributes.total_energy', 
                            'attributes.total_energy_units', 'attributes.distance_charge',
                            'attributes.distance_charge_units', 'attributes.total_wall_time',
                            'attributes.total_wall_time_units'],
                            'wf_0_4_2')
wf_0_8_0 = WorkflowProjections(['uuid', 'attributes.workflow_version', 'attributes.number_of_rms_steps', 
                            'attributes.number_of_rms_steps_units', 'attributes.convergence_values_all_step',
                            'attributes.convergence_values_all_step_units'],
                            'wf_0_8_0')
wf_0_9_4 = WorkflowProjections(['uuid', 'attributes.workflow_version', 'attributes.loop_count', 
                            'attributes.loop_count_units', 'attributes.convergence_values_all_step',
                            'attributes.convergence_values_all_step_units'],
                            'wf_0_9_4')
wf_0_10_4 = WorkflowProjections(['uuid', 'attributes.workflow_version', 'attributes.charge_neutrality', 
                             'attributes.charge_neutrality_unints','attributes.convergence_value',
                             'attributes.convergence_value_units'],
                             'wf_0_10_4')
wf_0_12_0 = WorkflowProjections(['uuid', 'attributes.workflow_version', 'attributes.starting_fermi_energy',
                               'attributes.starting_fermi_energy_units', 'attributes.last_rclustz',
                               'attributes.last_rclustz_units', 'attributes.max_wallclock_seconds',
                               'attributes.max_wallclock_seconds_units'],
                               'wf_0_12_0')
# Parser
ps_0_3_0 = WorkflowProjections(['uuid','attributes.parser_info', 'attributes.energy', 'attributes.energy_units', 
                            'attributes.fermi_energy', 'attributes.fermi_energy_units', 'attributes.energy_hartree',
                            'attributes.energy_hartree_units', 'attributes.bandgap', 'attributes.bandgap_units',
                            'attributes.walltime', 'attributes.walltime_units'],
                            'ps_0_3_0')
ps_0_3_1 =  WorkflowProjections(['uuid','attributes.parser_info', 'attributes.energy', 'attributes.energy_units', 
                            'attributes.fermi_energy', 'attributes.fermi_energy_units', 'attributes.energy_hartree',
                            'attributes.energy_hartree_units', 'attributes.bandgap', 'attributes.bandgap_units',
                            'attributes.walltime', 'attributes.walltime_units'],
                            'ps_0_3_1')
ps_0_3_2 =  WorkflowProjections(['uuid','attributes.parser_info', 'attributes.energy', 'attributes.energy_units', 
                            'attributes.fermi_energy', 'attributes.fermi_energy_units', 'attributes.energy_hartree',
                            'attributes.energy_hartree_units', 'attributes.bandgap', 'attributes.bandgap_units',
                            'attributes.walltime', 'attributes.walltime_units'],
                            'ps_0_3_2')
ps_0_4_2 = WorkflowProjections(['uuid','attributes.parser_version', 'attributes.energy', 'attributes.energy_unit',
                              'attributes.fermi_energy', 'attributes.fermi_energy_units', 'attributes.total_energy_Ry',
                              'attributes.total_energy_Ry_unit', 'attributes.total_energies_atom',
                              'attributes.total_energy_energies_atom_unit', 'attributes.single_particle_energies', 
                              'attributes.single_particle_energies_unit','attributes.total_charge_per_atom',
                              'attributes.total_charge_per_atom_unit', 'attributes.charge_core_states_per_atom',
                              'attributes.charge_core_states_per_atom_unit','attributes.charge_valence_states_per_atom',
                              'attributes.charge_valence_states_per_atom', 'attributes.timings', 'attributes.timings_unit'],
                              'ps_0_4_2')
ps_0_6_6 = WorkflowProjections(['uuid','attributes.parser_version', 'attributes.energy', 'attributes.energy_unit',
                              'attributes.fermi_energy', 'attributes.fermi_energy_units', 'attributes.total_energy_Ry',
                              'attributes.total_energy_Ry_unit', 'attributes.single_particle_energies', 
                              'attributes.single_particle_energies_unit', 'attributes.alat_internal',
                              'attributes.alat_internal_unit', 'attributes.two_pi_over_alat_internal',
                              'attributes.two_pi_over_alat_internal_unit', 'attributes.dos_at_fermi_energy',
                              'attributes.dos_at_fermi_energy_units', 'attributes.total_charge_per_atom',
                              'attributes.total_charge_per_atom_unit', 'attributes.charge_core_states_per_atom',
                              'attributes.charge_core_states_per_atom_unit','attributes.charge_valence_states_per_atom',
                              'attributes.charge_valence_states_per_atom', 'attributes.timings', 'attributes.timings_unit'],
                              'ps_0_6_6')

predifined_workflow = StandardWorkflow()
predifined_workflow.add_workflow(
            wf_0_2_2, wf_0_3_0, wf_0_4_2, wf_0_8_0, wf_0_9_4, wf_0_10_4, wf_0_12_0, 
            ps_0_3_0, ps_0_3_1, ps_0_3_2, ps_0_4_2, ps_0_6_6)


def set_structure_formula():
    '''
    Proprocessing.
    Set extras.formula attributes for structure nodes
    '''
    qb = QB()
    qb.append(StructureData)
    strucs = qb.all()
    for struc in strucs:
        struc = struc[0]
        if 'formula' in struc.extras: # Could be projected in the query
            continue
        formula = struc.get_formula()
        struc.set_extra('formula', formula)


def get_structure_workflow_dict(
        structure_project=['uuid', 'extras.formula'],
        structure_filters=None,
        workflow_project=['uuid', 'attributes.process_label'],
        workflow_filters=None,
        dict_project=['uuid'],
        dict_filters=None,
        timing=False,
        check_version=False):
    '''
    Input the required project information and filters information.
    Return all output dict nodes returned by workflows, which had StructureData nodes as inputs are there in the database
    with the projections and filters given.
    The output is a list of dicts.
    '''
    if check_version:
        dict_project.extend(['attributes.workflow_version', 'attributes.parser_info', 'attributes.parser_version'])
        tmp = dict_project
        dict_project = list(set(dict_project))
        dict_project.sort(key=tmp.index)
    if timing:
        time_start = time.time()

    qb_wfunc = QB()  # qb for WorkFunctionNode
    qb_wfunc.append(StructureData,
                    project=structure_project,
                    filters=structure_filters,
                    tag='structure')
    qb_wfunc.append(WorkFunctionNode,
                    project=workflow_project,
                    filters=workflow_filters,
                    tag='work_function',
                    with_incoming='structure')
    qb_wfunc.append(Dict,
                    project=dict_project,
                    filters=dict_filters,
                    tag='results',
                    with_incoming='work_function')

    qb_wchain = QB()  # qb for WorkChainNode
    qb_wchain.append(StructureData,
                     project=structure_project,
                     filters=structure_filters,
                     tag='structure')
    qb_wchain.append(WorkChainNode,
                     project=workflow_project,
                     filters=workflow_filters,
                     tag='work_chain',
                     with_incoming='structure')
    qb_wchain.append(Dict,
                     project=dict_project,
                     filters=dict_filters,
                     tag='results',
                     with_incoming='work_chain')

    workflowlst = qb_wfunc.all() + qb_wchain.all(
    )  # Combine into a workflow list

    if timing:
        time_end = time.time()
        time_elapsed = time_end - time_start
        print("Elapsed time: ", time_elapsed, 's\n')


    stlen, wflen, dclen = len(structure_project), len(workflow_project), len(
        dict_project)
    workflowdictlst = [{
        'structure': wf[0:stlen],
        'workflow': wf[stlen:(stlen + wflen)],
        'dict': wf[(stlen + wflen):(stlen + wflen + dclen)]
    } for wf in workflowlst]  # Transform workflowlst to a list of dicts

    if check_version:
        idx1 = dict_project.index('attributes.workflow_version')
        idx2 = dict_project.index('attributes.parser_info')
        idx3 = dict_project.index('attributes.parser_version')
        versions_wf = [item['dict'][idx1] for item in workflowdictlst] # Generate versions for workflow
        versions_ps = [[item['dict'][idx2], item['dict'][idx3]
                       ] for item in workflowdictlst] # Generate versions for parser
        filtered_wf = filter(None, versions_wf)
        flattened_ps = [val for version in versions_ps for val in version]
        flattened_ps = filter(None, flattened_ps)
        final_wf = ['workflow_' + vs for vs in filtered_wf]
        final_ps = ['parser_' + vs for vs in flattened_ps]
        final_versions = Counter(final_wf + final_ps).most_common() # Count versions
        print("Versions and frequency:\n", final_versions, '\n')   

    if check_version:
        return workflowdictlst, final_versions
    else:
        return workflowdictlst


def generate_dict_property_pandas_source(workflow_name=None,
                                         version=None,
                                         dict_project=[
                                             'attributes.energy',
                                             'attributes.energy_units',
                                             'attributes.total_energy',
                                             'attributes.total_energy_units'
                                         ],
                                         filename=None):
    '''
    Given a workflow and version, generate the dict_project property (which is the output of the workflow) as a pandas object,
    and write it into a json file.
    e.g. workflow_name='fleur_scf_wc', filename='dict_property.json'
    '''
    if version:
        type_name, version_name = version.split('_')
        if type_name == 'workflow':
            dict_filters = {'attributes.workflow_version': {'==': version_name}}
        elif type_name == 'parser':
            dict_filters = {'or': [{'attributes.parser_info': {'==': version_name}},
                                  {'attributes.parser_version': {'==': version_name}}]}
        else:
            print("Invalid version!")
            dict_filters = None
    else:
        dict_filters = None
        
    if workflow_name:
        workflow_filters = {'attributes.process_label': {'==': workflow_name}}
    else:
        workflow_filters = None

    workflowdictlst = get_structure_workflow_dict(dict_project=dict_project,
                                                  workflow_filters=workflow_filters,
                                                  dict_filters=dict_filters)
    dictlst = [wf['dict']
               for wf in workflowdictlst]  # Generate a list for Dict nodes
    cleaned_col = [att.split('.')[-1]
                   for att in dict_project]  # Re-define the column name
    try:  # Check if uuid information exists
        idx = cleaned_col.index('uuid')
    except ValueError:
        pass
    else:
        cleaned_col[idx] = 'dict_uuid'  # Change uuid column name
        for info in dictlst:  # Shorten the uuid to its first part
            info[0] = info[0].split('-')[0]
    dictpd = pd.DataFrame(
        dictlst, columns=cleaned_col)  # Transform list into pd DataFrame

    if filename:
        dictpd.to_json(filename, orient='records')

    return dictpd


def generate_structure_property_pandas_source(
        workflow_name=None,
        version=None,
        structure_project=['uuid', 'extras.formula'],
        filename=None):
    '''
    Given a workflow, generate the structure_project property (which is the input of the workflow) as a pandas object
    and write it into a json file.
    e.g. workflow_name='fleur_scf_wc', filename='structure_property.json'
    '''
    if version:
        type_name, version_name = version.split('_')
        if type_name == 'workflow':
            dict_filters = {'attributes.workflow_version': {'==': version_name}}
        elif type_name == 'parser':
            dict_filters = {'or': [{'attributes.parser_info': {'==': version_name}},
                                  {'attributes.parser_version': {'==': version_name}}]}
        else:
            print("Invalid version!")
            dict_filters = None
    else:
        dict_filters = None
        
    if workflow_name:
        workflow_filters = {'attributes.process_label': {'==': workflow_name}}
    else:
        workflow_filters = None

    workflowdictlst = get_structure_workflow_dict(structure_project=structure_project,
                                                  workflow_filters=workflow_filters,
                                                  dict_filters=dict_filters)
    structurelst = [wf['structure'] for wf in workflowdictlst
                    ]  # Generate a list for Structure nodes
    cleaned_col = [att.split('.')[-1]
                   for att in structure_project]  # Re-define the column name
    try:  # Check if uuid exists
        idx = cleaned_col.index('uuid')
    except ValueError:
        pass
    else:
        cleaned_col[idx] = 'structure_uuid'  # Change the uuid column name
        for info in structurelst:  # Shorten the uuid to its first part
            info[0] = info[0].split('-')[0]
    structurepd = pd.DataFrame(
        structurelst, columns=cleaned_col)  # Transform into pd DataFrame

    if filename:
        structurepd.to_json(filename, orient='records')

    return structurepd


def generate_combined_property_pandas_source(
        workflow_name=None,
        version=None,
        structure_project=['uuid', 'extras.formula'],
        dict_project=[
            'attributes.energy',
            'attributes.energy_units', 
            'attributes.total_energy',
            'attributes.total_energy_units'],
        filename=None):
    '''
    Given a workflow and a version, generate the combination of dict_project and structure_project property as a pandas object,
    and write it into a json file.
    e.g. workflow_name='fleur_scf_wc', filename='combination_property.json'
    '''
    dictpd = generate_dict_property_pandas_source(workflow_name=workflow_name,
                                                  version=version,
                                                  dict_project=dict_project)
    structurepd = generate_structure_property_pandas_source(workflow_name=workflow_name, 
                                                            version=version, 
                                                            structure_project=structure_project)
    combinepd = pd.concat([dictpd, structurepd], axis=1)

    if filename:
        combinepd.to_json(filename, orient='records')

    return combinepd


# D2 part b

def filter_missing_value(df, xcol=None, ycol=None):
    '''
    Given xcol, ycol specifying the columns in need. The function will filter out the missing values of
    these columns in the DataFrame. If both xcol and ycol are None, just return the original DataFrame.
    Return filtered_df(the entire DataFrame of the file after filtering), and the data of each column.
    '''
    filtered_df = df.copy()
    '''
    if cut_lists:
        # lists do not work, here we hard iterate them out and take the last element before we remove the nodes
        # greedy
        xdata = list(filtered_df[xcol])
        ydata = list(filtered_df[ycol])
        for i, data in enumerate(xdata):
            if isinstance(data, list):
                xdata[i] = data[-1]
        for i, data in enumerate(ydata):
            if isinstance(data, list):
                ydata[i] = data[-1]
        xdata = pd.Series(xdata)
        ydata = pd.Series(ydata)
        filtered_df[xcol] = xdata
        filtered_df[ycol] = ydata
    '''
    try:  # Check if xcol exists
        filtered_df.dropna(axis=0, how='any', subset=[xcol], inplace=True)
    except KeyError:
        if xcol!=None:
            print("Column '{}' not found.".format(xcol))
        xcol = None
    try:  # Check if ycol exists
        filtered_df.dropna(axis=0, how='any', subset=[ycol], inplace=True)
    except KeyError:
        if ycol!=None:
            print("Column '{}' not found.".format(ycol))
        ycol = None

    filtered_df.reset_index(drop=True, inplace=True)
    if xcol:
        xdata = filtered_df[xcol]
    else:
        xdata = None
    if ycol:
        ydata = filtered_df[ycol]
    else:
        ydata = None
 
    # Deal with lists
    if xcol:
        if not all([not isinstance(val, list) for val in xdata]):       
            xdata = list(filtered_df[xcol])
            for i, data in enumerate(xdata):
                if isinstance(data, list):
                    xdata[i] = data[-1]
            xdata = pd.Series(xdata)
            filtered_df[xcol] = xdata
    if ycol:
        if not all([not isinstance(val, list) for val in ydata]):    
            ydata = list(filtered_df[ycol])
            for i, data in enumerate(ydata):
                if isinstance(data, list):
                    ydata[i] = data[-1]
            ydata = pd.Series(ydata)
            filtered_df[ycol] = ydata

    return filtered_df, xdata, ydata


def filter_unavailable_df(df, node_num_thres=20, attr_num_thres=2):
    '''
    Check if the input dataframe is capable of plotting.
    If the dataframe cannot pass the number of nodes check or the number of attributes check,
    return df = None.
    If the attributes of the dataframe cannot pass the checks, filter out that attributes and the
    corresponding units from the dataframe.
    '''
    node_num_flag, attr_num_flag, del_attr_flag = 0, 0, 0
    # Number of nodes check
    if df.shape[0] < node_num_thres:
        node_num_flag = 1
    # Number of attributess check
    if df.shape[1] < 4 + 2 * attr_num_thres:
        attr_num_flag = 1
    
    if (node_num_flag==0) and (attr_num_flag==0):
        # Values check
        attrs, _, units_cols = get_attrs_and_units(df, get_units_cols=True)
        for idx, attr in enumerate(attrs):
            _, attr_data, _ = filter_missing_value(df, attr)
            # Number of filtered attribute data check
            if attr_data.shape[0] < node_num_thres:
                df.drop([attr, units_cols[idx]], axis=1, inplace=True)
                del_attr_flag = 1
                continue
            # Type of attribute data check
            if not all([isinstance(val, (int, float)) for val in attr_data]):
                df.drop([attr, units_cols[idx]], axis=1, inplace=True)
                del_attr_flag = 1

    if (del_attr_flag == 1):
        # Number of attributess check
        if df.shape[1] < 4 + 2 * attr_num_thres:
            attr_num_flag = 1  

    # set final df
    if (node_num_flag==1) or (attr_num_flag==1):
        df = pd.DataFrame()

    return df


def read_json_file(filename, chunksize=None):
    '''
    Read the dataset from the json file and return a DataFrame object.
    Use chuncksize for large json file
    '''
    try:  # Check if the file could successfully opened
        df = pd.read_json(filename, orient='records', lines=True, chunksize=chunksize)
    except ValueError:
        df = None
        print("Invalid file '{}'.".format(filename))
        #raise  

    return df


def read_excel_file(filename):
    '''
    Read the dataset from the excel file and return a dict of DataFrame object.
    '''
    try:  # Check if the file could successfully opened
        dfs = pd.read_excel(filename, sheet_name=None)
    except FileNotFoundError:
        dfs = None
        print("Invalid file '{}'.".format(filename))
        #raise  

    return dfs


def get_attrs_and_units(df, get_units_cols=False):
    '''
    Given the DataFrame, return the available attributes in the columns as options and their 
    corresponding units.
    '''
    cols = list(df.columns)
    cleaned_cols = cols[2:-2]
    attrs = [col for idx, col in enumerate(cleaned_cols) if idx % 2 == 0 ]
    units_cols = [col for idx, col in enumerate(cleaned_cols) if idx % 2 != 0 ]
    units = list(df[units_cols].iloc[0])
    
    if get_units_cols:
        return attrs, units, units_cols
    else:
        return attrs, units


def bokeh_struc_prop_vis(input_filename,
                         xcol,
                         ycol,
                         chunksize=None,
                         output_filename='Interactive_visualization.html', 
                         nbins=20, axis_type=['linear', 'linear'], maker_size=10):
    '''
        Create a single Bokeh Interactive scatter-histogram graphs for the xcol and ycol data from 
        the input json file.
        Hover tools included.
        The return plot file is saved in html format by default.
    '''

    # TODO: choose auto axis scale

    # IO and other settings
    original_df = read_json_file(input_filename, chunksize=chunksize)
    df, xdata, ydata = filter_missing_value(original_df, xcol, ycol)
    #print(list(xdata))
    #print(list(ydata))
    OPTIONS, UNITS = get_attrs_and_units(df)
    output_file(output_filename)
    TOOLS = 'box_zoom, pan, wheel_zoom, box_select, reset, save'

    # Create the scatter plot
    TOOLTIPS = [('Index', '$index'),
                ('Sturture_node uuid', '@structure_uuid'),
                ('Input formula', '@formula'),
                ('Dict_node uuid', '@dict_uuid'),
                ('Value', '($x, $y)')]  # Hover tools
    source = ColumnDataSource(df)
    # Settings
    p = figure(plot_width=FIGURE_HEIGHT+100,
               plot_height=FIGURE_HEIGHT+100,
               toolbar_location='above',
               x_axis_location=None,
               y_axis_location=None,
               y_axis_type=axis_type[1],
               x_axis_type=axis_type[0],
               title='Linked Histograms',
               tools=TOOLS,
               tooltips=TOOLTIPS)
    p.title.text = "Properties visualization"
    # Render
    r = p.circle(xcol,
                 ycol,
                 color='#3a5785',
                 alpha=0.6,
                 size=maker_size,
                 selection_color='red',
                 selection_fill_alpha=0.8,
                 nonselection_fill_color='grey',
                 nonselection_fill_alpha=0.2,
                 source=source)

    # Create the horizontal histogram
    if axis_type[0] == 'log':
        # assumes positive values
        minb = np.log10(abs(min(xdata)))
        maxb = np.log10(abs(max(xdata)))
        bins = [10**i for i in np.linspace(minb, maxb, num=nbins)]
    else:
        bins = np.linspace(min(xdata), max(xdata), num=nbins)

    hhist, hedges = np.histogram(xdata, bins=bins)
    hmax = max(hhist) * 1.1
    hsource = ColumnDataSource(
        dict(left=hedges[:-1],
             right=hedges[1:],
             top=hhist,
             bottom=np.zeros(hhist.shape)))
    # Settings
    ph = figure(toolbar_location=None,
                plot_width=p.plot_width,
                plot_height=200,
                x_axis_type=axis_type[0],
                x_range=p.x_range,
                y_range=(-1, hmax),
                min_border=10,
                min_border_left=50,
                y_axis_location='right',
                tools='hover',
                tooltips=[('Count', '@top')])
    ph.xgrid.grid_line_color = None
    ph.yaxis.major_label_orientation = np.pi / 4
    # Render
    ph.quad(bottom='bottom',
            left='left',
            right='right',
            top='top',
            color='#3a5785',
            alpha=0.6,
            source=hsource)
    xunit = UNITS[OPTIONS.index(xcol)]
    if xunit:
        ph.xaxis.axis_label = f"{xcol} ({xunit})"
    else:
        ph.xaxis.axis_label = xcol

    # Create the vertical histogram
    if axis_type[1] == 'log':
        # assumes positive values
        # assumes positive values
        minb = np.log10(abs(min(ydata)))
        maxb = np.log10(abs(max(ydata)))
        bins = [10**i for i in np.linspace(minb, maxb, num=nbins)]
    else:
        bins = np.linspace(min(ydata), max(ydata), num=nbins)

    vhist, vedges = np.histogram(ydata, bins=bins)
    vmax = max(vhist) * 1.1
    vsource = ColumnDataSource(
        dict(left=np.zeros(vhist.shape),
             right=vhist,
             top=vedges[1:],
             bottom=vedges[:-1]))
    # Settings
    pv = figure(toolbar_location=None,
                plot_width=200,
                plot_height=p.plot_height,
                x_range=(-1, vmax),
                y_range=p.y_range,
                y_axis_type=axis_type[1],
                min_border=10,
                y_axis_location='right',
                tools='hover',
                tooltips=[('Count', '@right')])
    pv.ygrid.grid_line_color = None
    pv.xaxis.major_label_orientation = np.pi / 4
    # Render
    pv.quad(left='left',
            bottom='bottom',
            top='top',
            right='right',
            color='#3a5785',
            alpha=0.6,
            source=vsource)
    yunit = UNITS[OPTIONS.index(ycol)]
    if yunit:
        pv.yaxis.axis_label = ycol + f"{ycol} ({yunit})"
    else:
        pv.yaxis.axis_label = ycol

    # Show plots
    layout = gridplot([[p, pv], [ph, None]], merge_tools=False)
    curdoc().add_root(layout)
    curdoc().title = "Properties visualization"
    show(layout)


#D1.b


def print_Count(types, res):
    if types == 'user':
        dict_type = Counter([r[4] for r in res])
    elif types == 'types':
        dict_type = Counter([r[3] for r in res])
    for count, name in sorted((v, k) for k, v in dict_type.items())[::-1]:
        print('- {} created {} nodes'.format(name, count))


#D1.c


#split data nodes and process nodes
def get_data_node_count(types, node_type):
    labelst, sizest = [], []
    for k, v in types.items():
        if k.split('.')[0] == node_type:
            labelst.append(k.split('.')[-2])
            sizest.append(v)
    x = dict(zip(labelst, sizest))
    nodes=sum(list(x.values()))
    return x


def get_process_node_count(types, node_type):
    q = QB()
    q.append(ProcessNode)
    pro = q.iterall()
    nodetypes = Counter(
        [p[0].process_label for p in pro if '_' not in p[0].process_label])
    workchain = {k: v for k, v in nodetypes.items() if k.endswith('WorkChain')}
    calculation = {
        k: v
        for k, v in nodetypes.items() if k.endswith('Calculation')
    }
    workfunction = {
        k: v
        for k, v in nodetypes.items() if k.endswith('WorkFunction')
    }

    x1 = get_data_node_count(types, node_type)
    for k, v in nodetypes.items():
        if k.endswith('WorkChain'):
            x1.pop('WorkChainNode', None)
            x1.update(**workchain)
        elif k.endswith('Calculation'):
            x1.pop('CalcJobNode', None)
            x1.update(**calculation)
        elif k.endswith('WorkFunction'):
            x1.pop('WorkfunctionNode', None)
            x1.update(**workfuction)
    return x1


def draw_pie_chart(x, title):
    data = pd.DataFrame.from_dict(
        dict(x), orient='index').reset_index().rename(index=str,
                                                      columns={
                                                          0: 'value',
                                                          'index': 'data_nodes'
                                                      })
    data['angle'] = data['value'] / sum(list(x.values())) * 2 * pi
    data['color'] = Category20[len(x)]
    data['percent'] = data['value'] / sum(x.values())
    nodes=sum(list(x.values()))
    p = figure(plot_height=FIGURE_HEIGHT,plot_width=FIGURE_WIDTH,
               title=title%nodes,
               toolbar_location=None,
               tools='hover',
               tooltips=[('Data', '@data_nodes'),
                         ('Percent', '@percent{0.00%}'), ('Count', '@value')])
    p.add_layout(Legend(), 'right')
    p.wedge(x=0,
            y=1,
            radius=0.6,
            start_angle=cumsum('angle', include_zero=True),
            end_angle=cumsum('angle'),
            line_color='white',
            fill_color='color',
            legend_field='data_nodes',
            source=data)
    p.axis.axis_label = None
    p.axis.visible = False
    p.grid.grid_line_color = None
    return p


def get_dict_link_types():
    link_labels = {}
    xl = []
    q = QB()
    q.append(Dict)
    dicts = q.iterall()

    for node in dicts:
        if len(node[0].get_incoming().all_link_labels()) > 1:
            link_labels = Counter(node[0].get_incoming().all_link_labels())
            for key, value in link_labels.items():
                if value > 1:
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

    #plot multiline
    p = figure(x_axis_type='datetime',y_axis_type='log')
    r=p.multi_line([df['A'], df['B']],  
                   [df.index, df.index],   
                   color=["blue", "red"],   
                   alpha=[0.8, 0.6],     
                   line_width=[2,2],     
                   )

    legend=Legend(items=[
        LegendItem(label="ctime",renderers=[r],index=0),
        LegendItem(label="mtime",renderers=[r],index=1),
    ])
    #ctime & mtime for each user
    ctimes = sorted(r[1] for r in res)
    mtimes = sorted(r[2] for r in res)
    num_nodes_integrated = range(len(ctimes))
    df = pd.DataFrame({'ctimes':ctimes,"mtimes":mtimes})
    userss = Counter([r[4] for r in res])
    #p = figure(x_axis_type='datetime')

    numlines=2*len(userss)
    mypalettes=Spectral11[0:numlines]

    for count, email in sorted((v, k) for k, v in userss.items())[::-1]:
        ctimes = sorted(r[1] for r in res if r[4] in email)
        mtimes = sorted(r[2] for r in res if r[4] in email)
        num_nodes_integrated = range(len(ctimes))
        df_user = pd.DataFrame({email+':ctimes':ctimes,email+':mtimes':mtimes})
        df = pd.concat([df,df_user],axis=1)
    
    
    p = figure(plot_width=600,plot_height=800,x_axis_type='datetime',y_axis_type='log')
    df_list = df.columns
    for i in range(len(df_list)):
        source = ColumnDataSource(
            data={'x':df[df_list[i]],
                  'y':df.index})
        p.line(x='x',
               y='y',
               source=source,
               legend_label = df_list[i],
               color = (Category20[8])[i])#add tool tips
    #show(p)

    #p.add_layout(legend)
    p.xaxis.axis_label = 'Date'
    p.yaxis.axis_label = 'Number of nodes'
    p.yaxis.axis_label_text_font_size = "11pt"
    show(p)
