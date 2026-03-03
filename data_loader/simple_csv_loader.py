from utils.csv_utils import json_to_csv

class SimpleCSVLoader():
    def __init__(self) -> None:
        pass
    
    @classmethod
    def loadDataIntoCSV(cls,data,filename:str) -> None:
        json_to_csv(filename=filename,rows=data)
