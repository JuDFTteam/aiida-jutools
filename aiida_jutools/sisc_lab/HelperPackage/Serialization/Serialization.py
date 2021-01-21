### Class serialized is used to save data to file
import pandas as pd
import HelperPackage.DataProcessing.DataVisu as DV
import json
import time


class Serializer:
    """
    Serializer class to serialize each node type after preprocessing
    """
    def __init__(self,data):
        """
        :param data: can be the qb.all() return value of StructureData,Node,CalcJobNode,WorkflowNode
        """
        self.data = data

    
    def to_file(self,filepath,Node_type = None):
        '''
        This function serialize different data according to the Node_Type keyword
        So be careful to specify the type when doing analyse
        
        :param filepath: the filepath of .json file from serialization
        :param Node_type: can be 'Group','StructureFormula','StructureElement','ProcessNode','Provenance'
        :return: 0 when succeed
        :rtype: int
        '''
        ######## None type
        if(Node_type == None):
            return 0
        #### Group type 
        elif(Node_type == 'Group'):
            Data = DV.preprocess_group(self.data)
            ## the aiida object will cause error here, so ensure every value in the list is no object
            Data.to_json(filepath,orient='records')
            return 0
        ### structureFormula type
        elif(Node_type == 'StructureFormula'):
            dic = DV.AtomsNumNodes(self.data)
            with open(filepath, 'w') as fp:
                json.dump(dic,fp)
            return 0
        
        ###### structure Element type
        elif(Node_type == 'StructureElement'):
            
            Data = DV.AnalyseStructureElements(self.data)
            Data.to_json(filepath,orient='records')
            return 0
        ###### ProcessNode type
        elif(Node_type == 'ProcessNode'):
            data = DV.GetCalNodeArray(self.data)
            data.to_json(filepath,orient='records')
            return 0
        ###### Provenance type
        elif(Node_type == 'Provenance'):
            data = DV.preprocess_provenance(self.data)
            data.to_json(filepath,orient='records')
            return 0
    
    
def deserialize_from_file(filepath,Node_type = None):
    """
    :param filepath: the filepath of .json file from serialization
    :param Node_type: can be 'Group','StructureFormula','StructureElement','ProcessNode','Provenance'
    :return: the basic information inside .json file
    :rtype: pd.DataFrame or Dictionary(for 'StructureFormula')
    """
    if(Node_type == None):
        return 0
    elif(Node_type == 'Group'):
        return pd.read_json(filepath)
    elif(Node_type == 'StructureFormula'):
        with open(filepath, 'r') as f:
            data2 = json.load(f)
            
        ############ since the key here is saved to a string, but we want a int, so do the following to transfer
        Newdata = {}
        for key in data2:
            #print(int(key))
            Newdata[int(key)] = data2[key]
        return Newdata
    elif(Node_type == 'StructureElement'):
        return pd.read_json(filepath)
    elif(Node_type == 'ProcessNode'):
        return pd.read_json(filepath)
    elif(Node_type == 'Provenance'):
        return pd.read_json(filepath)
    