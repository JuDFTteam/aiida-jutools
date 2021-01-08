# __init__.py
# the code to initialization and add all modules to sys path
import sys
from pathlib import Path
import os
import aiida


aiida.load_profile()


path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def add_to_sys_path(path:Path):
    if str(path) not in sys.path:
        sys.path.append(str(path))

add_to_sys_path(path)


if __name__ == 'main':
    print(path,'is added to system path')
else:
    print(path+' loaded successfully')
    print('DataProcess Package Initialization successfully')
    print('Aiida file loaded successfully')