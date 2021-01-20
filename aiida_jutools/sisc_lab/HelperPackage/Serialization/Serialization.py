### Class serialized is used to save data to file
import pandas as pd
import HelperPackage.DataProcessing.DataVisu as DV
import json

class Serializer:
    def __init__(self,data):
        self.data = data
    def to_file(self,filepath,Node_type = None):
        '''
        This function serialize different data according to the Node_Type keyword
        So be careful to specify the type when doing analyse
        '''
        ######## None type
        if(Node_type == None):
            return 0
        #### Group type 
        elif(Node_type == 'Group'):
            DataList = []
            Data = {}
            Columns = ['User','Group_Name','Node','type_string']
            for column in Columns:
                Data[column] = []
            for g, in self.data:
                DataList = DataList + [[g.user.get_short_name(),g.label,len(g.nodes),g.type_string]]

                '''
                Data['User'] = Data['User'] + [g[0].user]
                Data['Grouplabel'] = Data['Grouplabel'] + [g[0].label]
                Data['Node'] = Data['Node'] + [len(g[0].nodes)]

                Data['User'] = [g.user]
                Data['Grouplabel'] = [g.label]
                Data['Node'] = [len(g.nodes)]

                DataList = DataList + [Data]

                '''

            #print(DataList)
            Data = pd.DataFrame(DataList,columns = Columns)
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
        
        elif(Node_type == 'ProcessNode'):
            data = DV.GetCalNodeArray(self.data)
            data.to_json(filepath,orient='records')
            return 0
    
    
def deserialize_from_file(filepath,Node_type = None):
    if(Node_type == None):
        return 0
    elif(Node_type == 'Group'):
        
        return 0
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
    
    