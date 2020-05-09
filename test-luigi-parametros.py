import luigi

#Una clase es un target (Objetivo), el objetivo es crear una salida de numeros
class SalidaNumeros(luigi.Task):
    n = luigi.IntParameter()
    def requires(self):
        return []

    def output(self):
        #Se saca al archivo
        return luigi.LocalTarget('numeros{}.txt'.format(self.n))

    def run(self):
        #With es un metodo que se usa para abrir archivos -> 'Con' tal archivo hacer algo
        #* 'w' es para abrir en modo escritura
        with self.output().open('w') as f:
            for i in range(1, self.n + 1):
                #Dentro de las llaves va el numero
                f.write("{}\n".format(i))

class Cuadrados(luigi.Task):
    n = luigi.IntParameter(default = 10)
    #Requiere el proceso de salida de numeros para dar sus cuadrados
    def requires(self):
        return [SalidaNumeros(n = self.n)]

    def output(self):
        return luigi.LocalTarget('cuadrados{}.txt'.format(self.n))

    def run(self):
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for linea in fin:
                n = int(linea.strip())
                val = n*n
                fout.write("{}:{}\n".format(n, val))
            time.sleep(15)
#Primero se debe correr el luigid: luigid --port 8082
#python test-luigi-parametros.py Cuadrados --scheduler-host localhost --n 20
if __name__ == '__main__':
    luigi.run()


