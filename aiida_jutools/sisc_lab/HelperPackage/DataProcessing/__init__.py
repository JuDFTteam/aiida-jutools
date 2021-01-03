# __init__.py
# the code to initialization
import sys
from pathlib import Path
import os

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def add_to_sys_path(path:Path):
    if str(path) not in sys.path:
        sys.path.append(str(path))

add_to_sys_path(path)

print(path)

#import DataProcessing.DataVisu

if __name__ == 'main':
    print(path,'is added to system path')
else:
    print('__init__.py Initialization successful')