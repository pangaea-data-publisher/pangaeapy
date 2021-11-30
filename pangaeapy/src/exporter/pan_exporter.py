import os


class PanExporter:
    def __init__(self, pandataset, filelocation=None):
        self.module_dir = os.path.dirname(os.path.dirname(__file__))
        self.pandataset=pandataset
        if filelocation == None:
            self.filelocation =os.path.join(self.module_dir,'export')
        else:
            self.filelocation = filelocation
        self.file = None
        self.logging = self.pandataset.logging
        print(self.logging)

    #check if export is possible
    def verify(self):
        return True

    #create the export file (as IO object if possible)
    def create(self):
        return True

    #save the file  at self.filelocation
    def save(self):
        return True