import os


class PanExporter:
    def __init__(self, pandataset, filelocation):
        self.module_dir = os.path.dirname(os.path.dirname(__file__))
        self.pandataset=pandataset
        if filelocation == None:
            self.filelocation =self.module_dir+'\export'
        else:
            self.filelocation = filelocation


    def create(self):
        return True