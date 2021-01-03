import sys
from pathlib import Path

def add_to_sys_path(path:Path):
    if str(path) not in sys.path:
        sys.path.append(str(path))

def read(a):
    print(a, 'from read')

def readfile(input:Path):
    print(input, 'from readfile')

class data:
    def __init__(self):
        print('class data initialization successful')
    def speak(self):
        print('this is a data')
