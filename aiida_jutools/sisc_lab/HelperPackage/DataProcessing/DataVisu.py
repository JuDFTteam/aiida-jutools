import sys
from pathlib import Path

## add path to system path
def add_to_sys_path(path:Path):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from itertools import groupby
import re

## class StrucData to analyse the elements
class StrucDataHelper:
    'To get the StructureData elements from the formula'
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
    

    
### List group nodes and their number
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
        for a in self.Group:
            flag=0
            type = a[0].type_string
            for ex in exclude:
                if ex in type:
                    flag=1
            if(flag):
                continue     
            else:
                ## the line below contains all the properties
                ##print(a[0].label,' ',a[0].user,' ',a[0].type_string,' ',a[0].description)

                print('{:<50}|{:5}'.format(a[0].label,len(a[0].nodes)))