import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-21-openjdk")
os.environ.setdefault("SPARK_HOME", "/usr/lib/spark")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_spark_home = os.environ["SPARK_HOME"]
sys.path.insert(0, os.path.join(_spark_home, "python"))
sys.path.insert(0, os.path.join(_spark_home, "python", "lib", "py4j-0.10.9.9-src.zip"))

import pyspark
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

url = "dados/operacoes_comerciais_inteira.csv"

os.makedirs('Resultados', exist_ok=True)

def salvar(nome_arquivo, titulo, resultado):
    texto = f'{titulo}\n{resultado}\n'
    print(texto)
    with open(f'Resultados/{nome_arquivo}.txt', 'w', encoding='utf-8') as arq:
        arq.write(texto)

rdd = sc.textFile(url)

cabecalho = rdd.first()

# tira a linha do cabecalho e deixa as outras
rdd = rdd.\
  filter(lambda x: x != cabecalho)

dados = rdd.\
  map(lambda x: x.split(';')).\
  filter(lambda x: len(x) == 10).\
  filter(lambda x: x[0].strip() != '' and x[4].strip() != '' and x[9].strip() != '').\
  filter(lambda x: x[1].strip().isdigit())

dados.persist(StorageLevel.MEMORY_AND_DISK)
print('Linhas válidas:', dados.count())

q1 = dados.\
  filter(lambda x: x[0] == 'Brazil').\
  count()
salvar('q01', '1. Número de transações envolvendo o Brasil (RDD):', q1)

q2 = dados.\
  filter(lambda x: x[0] == 'Brazil').\
  map(lambda x: (x[0], 1)).\
  reduceByKey(lambda a, b: a + b).\
  collect()
salvar('q02', '2. Número de transações envolvendo o Brasil utilizando PairRDD.', q2)

q3 = dados.\
  filter(lambda x: x[0] == 'Brazil').\
  filter(lambda x: int(x[1]) == 2016).\
  count()
salvar('q03', '3. Número de transações envolvendo o Brasil utilizando PairRDD.', q3)

q4 = dados.\
  filter(lambda x: x[0] == 'Brazil').\
  filter(lambda x: int(x[0]), 1).\
  map(lambda x: (x[0], int(x[1]), 1)).\
  reduceByKey(lambda a, b: a + b).\
  collect()

salvar('q04', '4. Número de transações envolvendo o Brasil utilizando PairRDD.', q4)

# q05–q10 atualizadas estão em pjBL2.py

spark.stop()