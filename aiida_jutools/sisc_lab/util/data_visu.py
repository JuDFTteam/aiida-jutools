# -*- coding: utf-8 -*-
import re
import time
import numpy as np
import pandas as pd
from itertools import groupby
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource, HoverTool, Legend
from bokeh.plotting import figure
from bokeh.io import output_notebook
from bokeh.palettes import Category20
from bokeh.palettes import inferno
from aiida.orm import WorkflowNode
from aiida.orm import QueryBuilder
from aiida.common.constants import elements
from aiida_jutools.sisc_lab.helpers import FIGURE_HEIGHT, FIGURE_WIDTH
from math import pi
from bokeh.transform import cumsum

AllElements = elements



####### function to analyse structure data elements number and nodes number but not implemented yet! ###############################
## class StrucData to analyse the elements
class StrucFormulaHelper:
    '''
    To get the StructureData elements from the formula

    '''
    def __init__(self, formula):
        self.Formula = formula

    def FormAnalyse(self):
        #a = self.Formula
        a = self.COUNTELEMENT(self.Formula)
        return a

    def seperate_String_Num(self, s):

        groups = []
        uniquekeys = []
        for k, g in groupby(s, lambda x: x.isdigit()):
            groups.append(list(g))
            uniquekeys.append(k)
        if (uniquekeys[-1] == False):
            groups.append(['1'])
            uniquekeys.append(True)
        for i in range(len(groups)):
            g = ''.join(groups[i])
            #if(uniquekeys[i]):
            #    g = int(g)
            groups[i] = g
        #print(groups)
        return groups

    def COUNTELEMENT(self, s):

        newstr = re.findall('[A-Z][^A-Z]*', s)
        mystr = {}
        for s in newstr:
            group = self.seperate_String_Num(s)
            mystr[group[0]] = group[1]
        return mystr


####################################### List group nodes and their number for 1.f#################################
class GroupDataHelper:
    def __init__(self, group: list):
        '''
        :param group: the group data list for initialization
        '''
        self.Group = group

    def ListGroup(self, exclude: list = []):
        """ return the group names and nodes they contain
        :param exclude: the list of data which we don't want to show
        """
        print('{:<52}{:6}'.format('Group names:', 'sizes:'))
        for index, row in self.Group.iterrows():

            flag = 0
            type = row['type_string']
            for ex in exclude:
                if ex in type:
                    flag = 1
            if (flag):
                continue
            else:
                ## the line below contains all the properties
                ##print(a[0].label,' ',a[0].user,' ',a[0].type_string,' ',a[0].description)

                print('{:<50}|{:5}'.format(row['Group_Name'], row['Node']))


def preprocess_group(data):
    """
    :param data: the return value of qb.all() of Group Node
    :return: a pd.DataFrame containing with data the DataList and columns Columns
    :rtype: DataFrame
    """
    DataList = []
    Data = {}
    Columns = ['User', 'Group_Name', 'Node', 'type_string','uuid']
    for column in Columns:
        Data[column] = []
    for g, in data:
        DataList = DataList + [[
            g.user.get_short_name(), g.label,
            len(g.nodes), g.type_string, g.uuid
        ]]
    DataF = pd.DataFrame(DataList, columns=Columns)
    return DataF


####### function to analyse structure data for 1.g ###############################
def PreprocessStructureGeneral(StructDatas):
    DataList = []
    Data = {}
    Columns = ['uuid','User', 'Cell_volume', 'Formula', 'Composition']
    for column in Columns:
        Data[column] = []
    for struc, in StructDatas:
        DataList.append([
            struc.uuid,struc.user.get_short_name(),struc.get_cell_volume(),struc.get_formula(),struc.get_composition()
        ])
    DataF = pd.DataFrame(DataList, columns=Columns)
    # for struc, in StructDatas:
    #     print(struc.uuid,struc.user,struc.get_cell_volume(),struc.get_formula(),struc.get_composition)
    return DataF

def AnalyseStructureElements(InputData):
    '''
    This function count the Elements and number of StructureNode.
    return the pd.DataFrame including elements and number of each element
    Output can be passed to the function ShowElements

    :param InputData: the qb.all() of StructureData
    :return pd.DataFrame with elements as columns and nodes as rows, value would be 1 if node contains this element and 0 otherwise
    :rtype pd.DataFrame
    '''
    print('Counting the number of all elements...')
    print('This process will take some time...')

    StrucList = []

    for struc, in InputData:
        form = struc.get_formula()
        struct = StrucFormulaHelper(form)
        StrucList = StrucList + [struct.FormAnalyse()]
    return pd.DataFrame(StrucList).fillna(0)


def AtomsNumNodes(StructDatas):
    '''
    This function return atom numbers and how many Nodes have this atom number;
    Output can be passed to ShowFormula directly or de-/serialization

    :param StructDatas: the qb.all() set of the StructureData
    :return: dictionary with keys as number of elements and values as all uuids(first 8 characters) and formulas
    :rtype: Python dictionary
    '''

    Newdict = {}
    print(
        'Counting 1.number of atoms and 2.number of nodes containing this atom number...'
    )
    print('This process will take some time...')
    for data, in StructDatas:
        #atoms = data.get_composition()
        #NumAtom = int(np.sum(list(CompositionDict.values())))

        ## count the length of sites to be Number of atoms
        NumAtom = len(data.sites)
        if NumAtom in Newdict.keys():
            Newdict[NumAtom].append('[' + data.uuid[:8])
            Newdict[NumAtom].append(data.get_formula() + ']')
        else:
            Newdict[NumAtom] = ['[' + data.uuid[:8], data.get_formula() + ']']

    return Newdict

def get_element_index(elements):
    """
    Use the aiida.elements as input
    :param : aiida.common.constants.elements which contain all elements and information
    :return the dictionary that has index as key and element as value
    """
    ElementOder={}
    for key in elements:
        ElementOder[key] = elements[key]['symbol']
    return ElementOder

def sort_element_index(Elements):
    """
    :param : the elements list we want to sort
    :return : the correct index order and correct elements order
    """
    ReferenceOder = get_element_index(AllElements) #dictionary with index as key and element as value
    keys=list(ReferenceOder.keys())  
    values=list(ReferenceOder.values())
    OderElement = [keys[values.index(element)] for element in Elements]
    
    ########## the correct oder after sorting ##########
    CorrectIndex, CorrectElements = (list(t) for t in zip(*sorted(zip(OderElement, Elements))))    
    return CorrectIndex, CorrectElements

def ShowElements(Data):
    '''
    visualize the Elements and number of them, the sorted by the number of elements

    :param Data: pd.DataFrame that contain elements as columns and nodes as rows, value would be 1 if node contains this element and 0 otherwise
    :return: None
    '''

    output_file('ShowingElements.html')
    #data = NumStructureNode()
    data = Data
    elements = list(data.columns)
    
    # zip sort
    #_ , elements = zip(*sorted(zip(counts, elements)))
    #print(elements)
    AtomicNumber,elements = sort_element_index(elements)
    
    #counts = list(data.astype(bool).sum(axis=0))
    counts = [data[element].astype(bool).sum(axis=0) for element in elements]
    #print(counts)
    #print(elements)

    source = ColumnDataSource(data=dict(
        elements=elements, counts=counts,AtomicNumber =AtomicNumber, color=inferno(len(elements))))

    TOOLTIPS = [
        ('AllElement', '@elements'),
        ('Atomic Number', '@AtomicNumber'),
        ('(x,y)', '($x, $y)'),
        ('Number of Structures containing this element', '@counts'),
    ]

    p = figure(y_range=[AllElements[key]['symbol'] for key in AllElements],
               x_range=(0, np.max(counts)+10),
               # This we make rectengular
               plot_height=FIGURE_WIDTH+100, plot_width=FIGURE_WIDTH,
               title='Number of Elements Information',
               #tools=[HoverTool(mode='hline')],
               tooltips=TOOLTIPS)
    #print('step figure done')
    p.hbar(y='elements',
           right='counts',
           height=0.5,
           left=0,
           color='color',
           source=source)
    #print('step hbar done')
    p.xaxis.axis_label = 'Number of Elements'
    p.yaxis.axis_label = 'Elements'
    output_notebook()
    p.xgrid.grid_line_color = None
    #p.legend = False
    show(p)


def ShowFormula(Data):
    '''
    This function visualize the number of nodes that contains certain number of elements, and
    Show the formula and id of some elements when move mouse here

    :param Data: dictionary with keys as number of elements and values as all uuids(first 8 characters) and formulas
    :return: Node
    '''

    output_file('ShowingFormula.html')
    data = Data
    elements = list(data.keys())
    counts = list(len(data[key]) / 2 for key in data.keys())
    formulas = list(data[key][:10] for key in data.keys())

    length = len(elements)
    if length > 256:
        # While this works through a different coloring with meaning is probably better
        inferno256_1 = inferno(256)
        colors = inferno256_1*int(length/256) + inferno(length%256)
    else:
        colors = inferno(length)
    source = ColumnDataSource(data=dict(elements=elements,
                                        counts=counts,
                                        formulas=formulas,
                                        color=colors))

    TOOLTIPS = [
        ('Number of Atoms', '@elements'),
        ('(x,y)', '($x, $y)'),
        ('Number of Nodes', '@counts'),
        ('Id and formula(first 5 nodes of all)', '@formulas'),
    ]

    p = figure(x_range=(0, np.max(counts) + 20),
               y_range=(0, np.max(elements)+5),
               plot_height=FIGURE_HEIGHT, plot_width=FIGURE_WIDTH,
               title='Number of Atoms Information',
               #tools=[HoverTool(mode='hline')],
               tooltips=TOOLTIPS)
    #print('step figure done')
    p.hbar(y='elements',
           right='counts',
           height=0.5,
           left=0,
           color='color',
           source=source)
    #print('step hbar done')
    p.xaxis.axis_label = 'Number of nodes'
    p.yaxis.axis_label = 'Number of atoms'
    output_notebook()
    p.xgrid.grid_line_color = None
    #p.legend = False
    show(p)


#################################################### end for 1.g #####################################################


######################################## Process Node functions for both Calculate Job and Workflow for 1.h#########################
def GetWorkflowDict(WNode):
    """
    Processing both the WorkflowNode and CalculateJob Node,count how many succeed and how many failed for each type
    The Output dictionary can be the input of ShowWorkflow

    :param WNode: pd.DataFrame Workflow or CalculateJob Node information array
    :return: a dictionary counting the succeed number and failed number
    :rtype: Python dictionary
    """

    Newdict = {}
    for index, node in WNode.iterrows():
        if 'FINISHED' in node['Process_State']:
            Newdict[node['node_type'] + '_succeed'] = Newdict.get(
                node['node_type'] + '_succeed', 0) + 1
            Newdict[node['node_type'] + '_not_succeed'] = Newdict.get(
                node['node_type'] + '_not_succeed', 0) + 0
        else:
            Newdict[node['node_type'] + '_not_succeed'] = Newdict.get(
                node['node_type'] + '_not_succeed', 0) + 1
            Newdict[node['node_type'] + '_succeed'] = Newdict.get(
                node['node_type'] + '_succeed', 0) + 0
    return Newdict


def GetCalNodeArray(CalcNode):
    '''
    This function works to return the main information of Process Node

    :param CalcNode: the qb.all() return data of CalcNode or WorkflowNode
    :return: pd.DataFrame containing node.pk, exit_state,exit_message,node_type of each node
    :rtype: pd.DataFrame
    '''

    data = []
    Columns = ['Node_uuid', 'Process_State','Exit Status','Exit_Message', 'node_type','Called by','label']
    for node, in CalcNode:
        if(node.caller==None):
            caller = 'None'
        else:
            caller = node.caller.uuid
        data = data + [[
            node.uuid,
            str(node.process_state),
            node.exit_status,
            str(node.exit_message), 
            node.node_type,
            str(caller),
            str(node.process_label)
            
        ]]

    return pd.DataFrame(data, columns=Columns)


def ShowWorkflow(WorkflowDict, Title):
    '''
    Visualiza the Workflow&CalcJob how many succeed and how many failed for each type

    :param WorkflowDict: a dictionary counting the succeed number and failed number. return value of function GetWorkflowDict
    :param Title: title of the output image
    :return: None
    '''
    output_file('CalcJob&WorkFlow.html')

    index = list(WorkflowDict.keys())
    ## make the name shorter
    index = [key.split('.')[-2] + key.split('.')[-1] for key in index]

    counts = list(WorkflowDict.values())
    #exit_message = exit_message
    #exit_state_string = exit_state
    #exit_state_digit = exit_state_digit
    length = len(index)
    if length > 256:
        # While this works through a different coloring with meaning is probably better
        inferno256_1 = inferno(256)
        colors = inferno256_1*int(length/256) + inferno(length%256)
    else:
        colors = inferno(length)
    percentage = [counts[i]/sum(counts) for i in range(len(counts))]
    startAngle = [counts[i]/sum(counts)*2*pi for i in range(len(counts))]
    
    source = ColumnDataSource(
        data=dict(index=index, counts=counts, color=colors,startAngle=startAngle,percentage=percentage))       
    
    TOOLTIPS = [
        ('Node number', '@counts'),
        ('percentage', '@percentage{%0.2f}'),
        ('(x,y)', '($x, $y)'),
        ('Node status', '@index'),
    ]

    HT = HoverTool(tooltips=TOOLTIPS, mode='vline')
    
    p = figure( plot_height=350, 
            title="Pie Chart", 
            toolbar_location=None,
            tools="hover", 
            tooltips=TOOLTIPS, 
            x_range=(-0.5, 1.0))

#     p = figure(y_range=(0, np.max(counts) + 10),
#                x_range=index,
#                plot_height=FIGURE_HEIGHT, plot_width=FIGURE_WIDTH,
#                title=Title,
#                #tools=[HoverTool(mode='vline')],
#                tooltips=TOOLTIPS)
    p.wedge( x=0, 
          y=1,
          radius=0.4,
          start_angle=cumsum('startAngle', include_zero=True), 
          end_angle=cumsum('startAngle'),
          line_color="white", 
          fill_color='color', 
          legend_field='index', 
          source=source)
    
#     p.vbar(x='index',
#            top='counts',
#            bottom=0,
#            width=1,
#            color='color',
#            source=source)
    #print('step hbar done')

    output_notebook()
    p.axis.axis_label=None
    p.axis.visible=False
    p.xgrid.grid_line_color = None

    #p.xaxis.axis_label = 'Exit status'
    #p.yaxis.axis_label = 'Number of nodes'
    #p.legend = True
    show(p)


####################### provenance for 1.i


def preprocess_provenance(Nodes):
    """
    :param Nodes: the return value of pd.all() of Nodes type
    :return : pd.DataFrame containing node_type,pk,incoming_node,outgoing_node of each node
    :rtype: pd.DataFrame
    """
    # this function is slow because we will dig the incoming and outgoing nodes of each node
    print('Begin looking for incoming and outgoing nodes of each node...')
    print(
        'The preprocessing is slow because we will dig the incoming and outgoing nodes of each node, please wait for a moment...'
    )
    print(
        'Approximate running time for smaller dataset with 5000+ Nodes is about 2 min...'
    )
    t = time.time()
    Newlist = []
    for n, in Nodes:
        #Newlist = Newlist + [[n.node_type, n.pk, n.get_incoming(only_uuid=True).all_nodes(), n.get_outgoing(only_uuid=True).all_nodes()]]
        Newlist = Newlist + [[
            n.node_type, n.pk,
            n.get_incoming(only_uuid=True).first(),
            n.get_outgoing(only_uuid=True).first()
        ]]
    Columns = ['Node_Type', 'PK', 'FirstInput', 'FirstOutput']
    provenance = pd.DataFrame(Newlist, columns=Columns)
    print('The preprocessing took {} seconds'.format(time.time() - t))
    return provenance


def Count_In_Out_Old_version(provenance):
    '''
    This function count the Nodes without incoming node, without outgoing nodes and without in/out.
    Return value is the dictionary with 3 types of nodes as keys and counts and values
    :param provenance : the pd.DataFrame from function preprocess_provenance
    :return : A dictionary counting number of nodes without incoming node, without outgoing nodes and without in/out.
    :rtype: python dictionary
    '''

    Namelist = ['No_Incoming', 'No_Outgoing', 'No_In&Out']

    Mydict = {}
    for index, n in provenance.iterrows():
        IncomingFlag, OutgoingFlag = False, False
        ### if list is empty then we have no incoming/outgoing
        if (n['FirstInput'] == None):
            IncomingFlag = True
            Mydict[Namelist[0]] = Mydict.get(Namelist[0], 0) + 1
        if (n['FirstOutput'] == None):
            OutgoingFlag = True
            Mydict[Namelist[1]] = Mydict.get(Namelist[1], 0) + 1
        if (IncomingFlag and OutgoingFlag):
            Mydict[Namelist[2]] = Mydict.get(Namelist[2], 0) + 1

    return Mydict


#### takes very long time
def Count_In_Out(provenance):
    '''
    This function count the Nodes without incoming node, without outgoing nodes and without in/out.
    Return value is the dictionary with 3 types of nodes as keys and counts and values
    :param provenance : the pd.DataFrame from function preprocess_provenance
    :return : A dictionary counting number of nodes without incoming node, without outgoing nodes and without in/out.
    :rtype: python dictionary
    '''

    Namelist = ['No_Incoming', 'No_Outgoing', 'No_In&Out']

    No_Incoming_Mydict,No_Outgoing_Mydict,No_InOut_Mydict = {},{},{}
    
    for index, n in provenance.iterrows():
        IncomingFlag, OutgoingFlag = False, False
        ### if list is empty then we have no incoming/outgoing
        if (n['FirstInput'] == None):
            IncomingFlag = True
            No_Incoming_Mydict[n['Node_Type']] = No_Incoming_Mydict.get(n['Node_Type'], 0) + 1
        if (n['FirstOutput'] == None):
            OutgoingFlag = True
            No_Outgoing_Mydict[n['Node_Type']] = No_Outgoing_Mydict.get(n['Node_Type'], 0) + 1
        if (IncomingFlag and OutgoingFlag):
            No_InOut_Mydict[n['Node_Type']] = No_InOut_Mydict.get(n['Node_Type'], 0) + 1

    return No_Incoming_Mydict,No_Outgoing_Mydict,No_InOut_Mydict



def Show_In_Out_Old_Version(Mydict):
    '''
    This function shows count the Nodes without incoming node, without outgoing nodes and without in/out

    :param Mydict : the dictionary output from function Count_In_Out, which is a dictionary counting number of nodes without incoming node, without outgoing nodes and without in/out
    :return : None
    '''

    output_file('Show_In_Out.html')

    index = list(Mydict.keys())
    counts = list(Mydict.values())

    source = ColumnDataSource(
        data=dict(index=index, counts=counts, color=Category20[len(index)]))

    TOOLTIPS = [
        ('Node number', '@counts'),
        ('(x,y)', '($x, $y)'),
        ('Node status', '@index'),
    ]

    HT = HoverTool(tooltips=TOOLTIPS, mode='vline')

    p = figure(y_range=(0, np.max(counts) + 500),
               x_range=index, plot_height=FIGURE_HEIGHT,plot_width=FIGURE_WIDTH,
               title='CalcNode Information',
               #tools=[HoverTool(mode='vline')],
               tooltips=TOOLTIPS)
    #print('step figure done')
    p.vbar(x='index',
           top='counts',
           bottom=0,
           width=1,
           color='color',
           source=source)
    #print('step hbar done')

    output_notebook()
    p.xgrid.grid_line_color = None

    p.xaxis.axis_label = 'Incoming and Outgoing status'
    p.yaxis.axis_label = 'Number of nodes'
    #p.legend = False
    show(p)

def Show_In_Out(No_Incoming_Mydict,No_Outgoing_Mydict,No_InOut_Mydict):
    '''
    This function shows count the Nodes without incoming node, without outgoing nodes and without in/out

    :param Mydict : the dictionary outputs from function Count_In_Out, which are 3 dictionaries counting number of nodes without incoming node, without outgoing nodes and without in/out for all kinds of nodes
    :return : None
    '''
    # would be nice to add total number of nodes, and percentage of it

    output_file('Show_In_Out.html')

    No_Incoming_Node_types = list(No_Incoming_Mydict.keys())
    No_Incoming_counts = list(No_Incoming_Mydict.values())
    
    No_Outgoing_Node_types = list(No_Outgoing_Mydict.keys())
    No_Outgoing_counts = list(No_Outgoing_Mydict.values())
    
    No_InOut_Node_types = list(No_InOut_Mydict.keys())
    No_InOut_counts = list(No_InOut_Mydict.values())
    
    labels = ['No_Incoming', 'No_Outgoing', 'No_In&Out']
    keyset = list(set(No_Incoming_Node_types+No_Outgoing_Node_types+No_InOut_Node_types))
    #keyset = No_Incoming_Node_types
    #source = ColumnDataSource(data=dict(index=index, counts=counts, color=Category20[len(index)]))
    color=Category20[len(keyset)]
    source = {'labels': labels}
    #print(No_Incoming_Node_types)
    for key in keyset:
        source[key] = [No_Incoming_Mydict.get(key, 0), No_Outgoing_Mydict.get(key, 0), No_InOut_Mydict.get(key, 0)]

    TOOLTIPS = [
        #('Node type', "$name @No_Incoming_Node_types: @$name"),
        ('Node type', "$name"),
        ('Node Number',"@$name"),
        ('(x,y)', '($x, $y)'),
        ('Node status', '@labels'),
    ]

    HT = HoverTool(tooltips=TOOLTIPS, mode='vline')

    p = figure(y_range=(0, np.max(No_Incoming_counts+No_Outgoing_counts+No_InOut_counts) + 1000),
               x_range=labels,
               plot_height=FIGURE_HEIGHT,plot_width=FIGURE_WIDTH,
               title='Provenance Health Information',
               tooltips=TOOLTIPS)
    p.add_layout(Legend(), 'right')
    #print('step figure done')
    p.vbar_stack(keyset,
                 x='labels',
                 #top='counts',
                 #bottom=0,
                 width=1,
                 color=color,
                 source=source,
                 legend_label=keyset)
    #print('step hbar done')

    p.xgrid.grid_line_color = None
    #p.legend.location = "bottom_left"

    p.xaxis.axis_label = 'Incoming and Outgoing status'
    p.yaxis.axis_label = 'Number of nodes'
    show(p)
