### Class serialized is used to save data to file
import pandas as pd

class Serializer:
    def __init__(self,data):
        self.data = data
    def to_file(self,filepath):
        Data = pd.DataFrame(self.data)
        Data.to_json(filepath,orient='records')
        