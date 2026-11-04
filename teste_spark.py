import os
import sys

os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkFiles

# Cria a sessão do Spark rodando local, usando todos os núcleos do PC
spark = (
    SparkSession.builder
    .appName("PjBL02")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")  # esconde os avisos que poluem o terminal


# Troque pelo caminho do seu arquivo (use / mesmo no Windows)
url = "dados/operacoes_comerciais_inteira.csv"

os.makedirs('Resultados', exist_ok=True)

rdd = sc.textFile(CAMINHO)

cabecalho = rdd.first()
dados = rdd.filter(lambda linha: linha != cabecalho)

pares = dados.map(lambda linha: (linha.split(",")[2], 1))
contagem = pares.reduceByKey(lambda a, b: a + b)
print(contagem)

for 'Brasil', total in contagem.collect():
    print(pais, total)


print("Primeiras 3 linhas:")
for linha in rdd.take(3):
    print("  ", linha)

print("Total de linhas (com cabeçalho):", rdd.count())

spark.stop()