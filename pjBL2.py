# -*- coding: utf-8 -*-
"""PJBL2 – Processamento Distribuído com Spark.

Exercícios atualizados (vieram do Colab).
No VS Code (Flatpak): Ctrl+Shift+B, or Terminal → Run Task → Run pjBL2.
Do not use the ▶ Play button: it runs Flatpak Python 3.13, which has no Spark/Java.
"""

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

from pyspark import StorageLevel
from pyspark.sql import SparkSession

COLAB = False
if "google.colab" in sys.modules:
    from google.colab import drive

    drive.mount("/content/drive")
    COLAB = True

spark = (
    SparkSession.builder
    .appName("PjBL02")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("ERROR")

if COLAB:
    url = "/content/drive/MyDrive/Colab Notebooks/operacoes_comerciais_inteira.csv"
    resultados_dir = "/content/Resultados"
else:
    url = str(ROOT / "dados" / "operacoes_comerciais_inteira.csv")
    resultados_dir = str(ROOT / "Resultados")

os.makedirs(resultados_dir, exist_ok=True)


def salvar(nome_arquivo, titulo, resultado):
    texto = f"{titulo}\n{resultado}\n"
    print(texto)
    with open(f"{resultados_dir}/{nome_arquivo}.txt", "w", encoding="utf-8") as arq:
        arq.write(texto)


rdd = sc.textFile(url)
cabecalho = rdd.first()
print(cabecalho)

rdd = rdd.filter(lambda x: x != cabecalho)
dados = rdd.map(lambda x: x.split(";"))
print(dados.take(5))

# Filtrar registros válidos
dados = (
    dados.filter(lambda x: len(x) == 10)
    .filter(
        lambda x: x[0].strip() != ""
        and x[4].strip() != ""
        and x[9].strip() != ""
        and x[5].strip() != ""
    )
    .filter(lambda x: x[1].strip().isdigit())
)

dados.persist(StorageLevel.MEMORY_AND_DISK)
print("Linhas válidas:", dados.count())

q1 = dados.filter(lambda x: x[0] == "Brazil").count()
salvar("q01", "1. Número de transações envolvendo o Brasil (RDD):", q1)

q2 = (
    dados.filter(lambda x: x[0] == "Brazil")
    .map(lambda x: (x[0], 1))
    .reduceByKey(lambda a, b: a + b)
    .collect()
)
salvar("q02", "2. Número de transações envolvendo o Brasil utilizando PairRDD:", q2)

q3 = (
    dados.filter(lambda x: x[0] == "Brazil")
    .filter(lambda x: int(x[1]) == 2016)
    .count()
)
salvar("q03", "3. Número de transações envolvendo o Brasil em 2016:", q3)

q4 = (
    dados.filter(lambda x: x[0] == "Brazil" and int(x[1]) == 2016)
    .map(lambda x: ((x[0], int(x[1])), 1))
    .reduceByKey(lambda a, b: a + b)
    .collect()
)
salvar(
    "q04",
    "4. Número de transações envolvendo o Brasil em 2016 utilizando PairRDD:",
    q4,
)

q5 = (
    dados.filter(lambda x: int(x[1]) >= 2010)
    .map(lambda x: ((int(x[1]), x[4].strip().upper()), 1))
    .reduceByKey(lambda a, b: a + b)
    .sortByKey()
    .collect()
)
salvar("q05", "5. Número de transações por flow e por ano, a partir de 2010:", q5)

q6 = dados.filter(lambda x: int(x[1]) == 2016)
valor_total = q6.map(lambda x: float(x[5])).reduce(lambda a, b: a + b)
quantidade = q6.count()
media = valor_total / quantidade

salvar(
    "q06",
    "Média do valor das transações em 2016:",
    f"Valor total: {valor_total:}\n"
    f"Quantidade de transações: {quantidade}\n"
    f"Média: {media:.2f}",
)

q7 = (
    dados.filter(lambda x: int(x[1]) == 2016)
    .map(lambda x: (int(x[1]), (float(x[5]), 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .mapValues(lambda v: v[0] / v[1])
    .collect()
)
salvar(
    "q07",
    "7. Média da coluna Price para o ano de 2016 utilizando PairRDD:",
    q7,
)

q8 = (
    dados.sortBy(lambda x: x[0])
    .map(
        lambda x: (
            (int(x[1]), x[9].strip().upper()),
            (float(x[5]), float(x[5])),
        )
    )
    .reduceByKey(lambda a, b: (max(a[0], b[0]), min(a[1], b[1])))
    .sortByKey()
    .collect()
)
salvar(
    "q08",
    "8. Preço máximo e mínimo por categoria e por ano (PairRDD):",
    q8,
)

spark.stop()
