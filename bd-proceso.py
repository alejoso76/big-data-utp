import luigi
from pymongo import *

class VerRegistro(luigi.Task):
    nombd = luigi.Parameter()
    col = luigi.Parameter()
    ip ='localhost'
    puerto = 27017
    consulta = {}

    def requires(self):
        return []

    def run(self):
    cliente = MongoClient(self.ip, self.puerto)
    db = cliente[self.nombd]
    coleccion = db[self.col]
    #Consulta sencilla
    res = coleccion.find_one(consulta)
    print(res)

if __name__ == '__main__':
    proceso_ver = VerRegistro(nombd = 'Cultivos', col = 'Cacao')
    proceso_ver.consulta = {'DEPARTAMENTO':'RISARALDA'}
    proceso_ver.run()