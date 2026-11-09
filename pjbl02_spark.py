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

_linux_java = "/usr/lib/jvm/java-21-openjdk"
_linux_spark = "/usr/lib/spark"
_brew_java = "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
if Path(_linux_java).is_dir():
    os.environ.setdefault("JAVA_HOME", _linux_java)
elif Path(_brew_java).is_dir():
    os.environ.setdefault("JAVA_HOME", _brew_java)
if Path(_linux_spark).is_dir():
    os.environ.setdefault("SPARK_HOME", _linux_spark)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

_spark_home = os.environ.get("SPARK_HOME", "")
_py4j = os.path.join(_spark_home, "python", "lib", "py4j-0.10.9.9-src.zip")
if os.path.isdir(os.path.join(_spark_home, "python")):
    sys.path.insert(0, os.path.join(_spark_home, "python"))
if os.path.isfile(_py4j):
    sys.path.insert(0, _py4j)

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

colunas = [c.strip() for c in cabecalho.split(";")]
i_pais = colunas.index("country_or_area")
i_flow = colunas.index("flow")
i_valor = colunas.index("trade_usd")

# Maior exportação = maior soma de trade_usd com flow Export, lido do cabeçalho.
q9 = (
    dados.filter(lambda x: x[i_flow].strip() == "Export")
    .map(lambda x: ((x[i_pais].strip(), x[i_flow].strip()), float(x[i_valor])))
    .reduceByKey(lambda a, b: a + b)
    .reduce(lambda a, b: a if a[1] >= b[1] else b)
)
salvar(
    "q09",
    "9. País com a maior exportação (PairRDD). Chave (country_or_area, flow) e valor (soma de trade_usd):",
    q9,
)

df = spark.createDataFrame(
    dados,
    [
        "country",
        "year",
        "commodity_code",
        "commodity",
        "flow",
        "price",
        "weight",
        "unit",
        "amount",
        "category",
    ],
)

def como_numero(coluna, tipo):
    texto = F.trim(F.col(coluna))
    valido = texto.rlike(r"^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$")
    return F.when(valido, texto.cast(tipo))


exportacoes = (
    df.filter(F.upper(F.trim(F.col("flow"))) == F.lit("EXPORT"))
    .withColumn("year", como_numero("year", "int"))
    .withColumn("price", como_numero("price", "double"))
    .withColumn("weight", como_numero("weight", "double"))
    .filter(
        F.col("year").isNotNull()
        & F.col("price").isNotNull()
        & F.col("weight").isNotNull()
        & (F.col("weight") > 0)
    )
    .withColumn("preco_por_kg", F.col("price") / F.col("weight"))
)

maior_preco_kg = exportacoes.agg(F.max("preco_por_kg").alias("preco_por_kg"))

q10 = (
    exportacoes.join(maior_preco_kg, "preco_por_kg")
    .select(
        F.col("year").alias("ano"),
        F.col("country").alias("pais"),
        F.upper(F.trim(F.col("category"))).alias("categoria"),
        F.col("preco_por_kg"),
    )
    .collect()
)
salvar(
    "q10",
    "10. Exportação com maior preço por kg (SparkSQL). Ano, país e categoria:",
    q10,
)

spark.stop()
