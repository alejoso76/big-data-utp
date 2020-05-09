import luigi

#Una clase es un target (Objetivo), el objetivo es crear una salida de numeros
class SalidaNumeros(luigi.Task):
    def requires(self):
        return []

    def output(self):
        #Se saca al archivo
        return luigi.LocalTarget('numeros.txt')

    def run(self):
        #With es un metodo que se usa para abrir archivos -> 'Con' tal archivo hacer algo
        #* 'w' es para abrir en modo escritura
        with self.output().open('w') as f:
            for i in range(1,11):
                #Dentro de las llaves va el numero
                f.write("{}\n".format(i))

class Cuadrados(luigi.Task):
    #Requiere el proceso de salida de numeros para dar sus cuadrados
    def requires(self):
        return [SalidaNumeros()]

    def output(self):
        return luigi.LocalTarget('cuadrados.txt')

    def run(self):
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for linea in fin:
                n = int(linea.strip())
                val = n*n
                fout.write("{}:{}\n".format(n, val))

#python test-luigi.py Cuadrados --local-scheduler
if __name__ == '__main__':
    luigi.run()


