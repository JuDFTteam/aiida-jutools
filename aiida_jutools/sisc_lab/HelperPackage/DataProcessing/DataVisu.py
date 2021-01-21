import sys
from pathlib import Path

## add path to system path
def add_to_sys_path(path:Path):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from itertools import groupby
import re
from aiida.orm import QueryBuilder
import numpy as np
import time
import numpy as np
from bokeh.io import output_file, show
from bokeh.models import ColumnDataSource,HoverTool
from bokeh.plotting import figure
from bokeh.io import output_notebook
from bokeh.palettes import inferno
import pandas as pd

####### function to analyse structure data elements number and nodes number but not implemented ###############################
## class StrucData to analyse the elements 
class StrucFormulaHelper:
    '''
    To get the StructureData elements from the formula
    
    '''
    def __init__(self,formula):
        self.Formula = formula
        
    def FormAnalyse(self):
        #a = self.Formula
        a = self.COUNTELEMENT(self.Formula)
        return a
    
    def seperate_String_Num(self,s):

        groups = []
        uniquekeys = []
        for k, g in groupby(s, lambda x:x.isdigit()):
            groups.append(list(g))
            uniquekeys.append(k)
        if(uniquekeys[-1] == False):
            groups.append(['1'])
            uniquekeys.append(True)
        for i in range(len(groups)): 
            g = ''.join(groups[i])
            #if(uniquekeys[i]):
            #    g = int(g)
            groups[i] = g    
        #print(groups)
        return groups

    def COUNTELEMENT(self,s):

        newstr = re.findall('[A-Z][^A-Z]*', s)
        mystr = {}
        for s in newstr:
            group = self.seperate_String_Num(s)
            mystr[group[0]] = group[1]
        return mystr


def AnalyseStructureElements(InputData):
   
    '''
    May not be implemented yet
    This function count the Elements and number of StructureNode
    Output can be passed to ShowElements
    '''
    #### return the pd.DataFrame including elements and number of each element

    StrucList = []

    for struc, in InputData:
        form = struc.get_formula()
        struct = StrucFormulaHelper(form)
        StrucList = StrucList+ [struct.FormAnalyse()]
    return pd.DataFrame(StrucList).fillna(0)

####### function to analyse structure data for 1.g ###############################    


def AtomsNumNodes(StructDatas):
    '''
    This function return atom numbers and how many Nodes have this atom number;
    Out put can be passed to ShowFormula directly or de-/serialization
    '''

    Newdict = {}

    for data, in StructDatas:
        CompositionDict = data.get_composition()
        NumAtom = int(np.sum(list(CompositionDict.values())))
        if NumAtom in Newdict.keys():
            Newdict[NumAtom].append('['+data.uuid[:8])
            Newdict[NumAtom].append(data.get_formula()+']')
        else:
            Newdict[NumAtom] = ['['+data.uuid[:8],data.get_formula()+']']
            
    return Newdict

    


def ShowElements(Data):
    #### visualize the Elements and number of them
    output_file("ShowingElements.html")
    #data = NumStructureNode()
    data = Data
    elements = list(data.columns)
    counts = list(data.astype(bool).sum(axis=0))
    # zip sort
    counts,elements = zip(*sorted(zip(counts,elements)))
    
    #print(counts)
    #print(elements)
    
    source = ColumnDataSource(data=dict(elements=elements, counts=counts,color=inferno(len(elements))))
    
    TOOLTIPS = [
    ("element", "@elements"),
    ("(x,y)", "($x, $y)"),
    ("Number of Structures containing this element", "@counts"),
    ]

    p = figure( y_range=elements,x_range=(0,np.max(counts)), plot_width=800, plot_height=800, title="Elements Counts",tools = [HoverTool(mode='hline')], tooltips=TOOLTIPS)
    #print('step figure done')
    p.hbar(y="elements", right="counts", height=0.5, left=0, color='color',  source=source)
    #print('step hbar done')
    
    output_notebook()
    p.xgrid.grid_line_color = None
    #p.legend = False
    show(p)
    
    
def ShowFormula(Data):
    '''
    This function shows the 
    Show the formula and id of some elements
     
    '''

    output_file("ShowingFormula.html")
    data = Data
    elements = list(data.keys())
    counts = list(len(data[key])/2 for key in data.keys())
    formulas = list(data[key][:10] for key in data.keys())

    length = len(elements)
    source = ColumnDataSource(data=dict(elements=elements, counts=counts,formulas=formulas,color=inferno(length)))
    
    TOOLTIPS = [
    ("Number of Atoms", "@elements"),
    ("(x,y)", "($x, $y)"),
    ("Number of Nodes", "@counts"),
    ("Id and formula(first 5 nodes of all)", "@formulas"),
    ]

    p = figure(x_range=(0,np.max(counts)+20),y_range=(0,np.max(elements)), plot_width=800, plot_height=800, title="Atoms Count",tools = [HoverTool(mode='hline')], tooltips=TOOLTIPS)
    #print('step figure done')
    p.hbar(y="elements", right="counts", height=0.5, left=0, color='color',  source=source)
    #print('step hbar done')
    
    output_notebook()
    p.xgrid.grid_line_color = None
    #p.legend = False
    show(p)

#################################################### end #####################################################

####################################### List group nodes and their number for 1.f#################################
class GroupDataHelper:
    def __init__(self,group : list):
        '''
        :param group: the group data list for initialization
        '''
        self.Group = group
        
    def ListGroup(self,exclude: list=[]):
        """ return the group names and nodes they contain
        :param exclude: the list of data which we don't want to show
        """
        print('{:<52}{:6}'.format('Group names:','sizes:'))
        for index, row in self.Group.iterrows():
            
            flag=0
            type = row['type_string']
            for ex in exclude:
                if ex in type:
                    flag=1
            if(flag):
                continue     
            else:
                ## the line below contains all the properties
                ##print(a[0].label,' ',a[0].user,' ',a[0].type_string,' ',a[0].description)

                print('{:<50}|{:5}'.format(row['Group_Name'],row['Node']))
                
                

######################################## Process Node functions for both Calculate Job and Workflow for 1.h#########################
def GetWorkflowDict(WNode):
    '''
    Processing both the WorkflowNode and CalculateJob Node,count how many succeed and how many failed for each type
    The Output dictionary can be the input of ShowWorkflow
    '''
    from aiida.orm import WorkflowNode
    from aiida.orm import QueryBuilder
    

    Newdict = {}
    for index,node in WNode.iterrows():
        if 'FINISHED'in node['Process_State']:
            Newdict[node['node_type']+'_succeed'] = Newdict.get(node['node_type']+'_succeed',0) + 1
            Newdict[node['node_type']+'_not_succeed'] = Newdict.get(node['node_type']+'_not_succeed',0) + 0
        else:
            Newdict[node['node_type']+'_not_succeed'] = Newdict.get(node['node_type']+'_not_succeed',0) + 1
            Newdict[node['node_type']+'_succeed'] = Newdict.get(node['node_type']+'_succeed',0) + 0
    return Newdict


def GetCalNodeArray(CalcNode):
    '''
    This function works to return the main information of Process Node
    '''
    
    data = []
    Columns = ['Node_Pk','Process_State','Exit_Message','node_type']
    for node, in CalcNode:
        data = data + [[node.pk,str(node.process_state),str(node.exit_message),node.node_type]]
               
    return pd.DataFrame(data,columns = Columns)
    

def ShowWorkflow(WorkflowDict,Title):
    '''
    Visualiza the Workflow&CalcJob how many succeed and how many failed for each type
    '''
    output_file("CalcJob&WorkFlow.html")

    index = list(WorkflowDict.keys())
    counts = list(WorkflowDict.values())
    #exit_message = exit_message
    #exit_state_string = exit_state
    #exit_state_digit = exit_state_digit
      
    source = ColumnDataSource(data=dict(index=index, counts=counts, color=inferno(len(index))))
    
    TOOLTIPS = [
    ("Node number", "@counts"),
    ("(x,y)", "($x, $y)"),
    ("Node status", "@index"),   
    ]
   
    HT = HoverTool(
    tooltips=TOOLTIPS,

    mode='vline'
    )
    
    p = figure( y_range=(0,50), x_range=index, plot_width=800, plot_height=800, title=Title,tools = [HoverTool(mode='vline')],tooltips=TOOLTIPS)
    #print('step figure done')
    p.vbar(x="index", top="counts", bottom=0, width=1, color='color',  source=source)
    #print('step hbar done')
    
    output_notebook()
    p.xgrid.grid_line_color = None
    #p.legend = False
    show(p)

####################### provenance for 1.i
        
def preprocess_provenance(Nodes):
    t = time.time()
    Newlist = []
    for n, in Nodes:
        Newlist = Newlist + [[n.node_type, n.pk, n.get_incoming(only_uuid=True).all_nodes(), n.get_outgoing(only_uuid=True).all_nodes()]]
    Columns = ['Node_Type','PK','FirstInput','FirstOutput']
    provenance = pd.DataFrame(Newlist,columns = Columns)
    print('The preprocessing took {} seconds'.format(time.time()-t))
    return provenance

def Count_In_Out(provenance):
    '''
    This function count the Nodes without incoming node, without outgoing nodes and without in/out.
    Return value is the dictionary with 3 types of nodes as keys and counts and values
    :param provenance : the pd.DataFrame from function preprocess_provenance
    '''
    
    Namelist = ['No_Incoming','No_Outgoing','No_In&Out']

    Mydict = {}
    for index,n in provenance.iterrows():
        IncomingFlag,OutgoingFlag = False,False
        ### if list is empty then we have no incoming/outgoing
        if(len(n['FirstInput'])==0):
            IncomingFlag = True
            Mydict[Namelist[0]] = Mydict.get(Namelist[0],0)+1
        if(len(n['FirstOutput'])==0):
            OutgoingFlag = True
            Mydict[Namelist[1]] = Mydict.get(Namelist[1],0)+1
        if(IncomingFlag and OutgoingFlag):
            Mydict[Namelist[2]] = Mydict.get(Namelist[2],0)+1

    return Mydict

def Show_In_Out(Mydict):
    '''
    This function shows count the Nodes without incoming node, without outgoing nodes and without in/out
    :param Mydict : the dictionary output from function Count_In_Out
    '''
    from bokeh.io import output_file, show
    from bokeh.models import ColumnDataSource,HoverTool
    from bokeh.plotting import figure
    from bokeh.io import output_notebook
    from bokeh.palettes import Category20

    output_file("Show_In_Out.html")

    index = list(Mydict.keys())
    counts = list(Mydict.values())
      
    source = ColumnDataSource(data=dict(index=index, counts=counts, color=Category20[len(index)]))
    
    TOOLTIPS = [
    ("Node number", "@counts"),
    ("(x,y)", "($x, $y)"),
    ("Node status", "@index"),   
    ]
   
    HT = HoverTool(
    tooltips=TOOLTIPS,

    mode='vline'
    )
    
    p = figure( y_range=(0,6000), x_range=index, plot_width=800, plot_height=800, title="CalcNode Information",tools = [HoverTool(mode='vline')],tooltips=TOOLTIPS)
    #print('step figure done')
    p.vbar(x="index", top="counts", bottom=0, width=1, color='color',  source=source)
    #print('step hbar done')
    
    output_notebook()
    p.xgrid.grid_line_color = None
    #p.legend = False
    show(p)