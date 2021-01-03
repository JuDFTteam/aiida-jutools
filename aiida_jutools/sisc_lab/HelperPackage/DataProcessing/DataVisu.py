import sys
from pathlib import Path

## add path to system path
def add_to_sys_path(path:Path):
    if str(path) not in sys.path:
        sys.path.append(str(path))

from itertools import groupby
import re

## class StrucData to analyse the 
class StrucDataForm:
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
    
    
