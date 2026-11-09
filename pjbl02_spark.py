# -*- coding: utf-8 -*-
"""PJBL2 – Processamento Distribuído com Spark.

Exercícios de análise de transações comerciais com PySpark (RDD, PairRDD e SparkSQL).

Linux: ./run.sh
macOS: ./run.sh
Windows: run.cmd
VS Code: Terminal → Run Task → Run pjBL2
"""

import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
os.chdir(ROOT)


def _is_jdk(path):
    home = Path(path)
    java = home / "bin" / ("java.exe" if sys.platform == "win32" else "java")
    return java.is_file() and (home / "release").is_file()


def _java_homes():
    """Prefer the JDK this machine already uses, then common install locations."""
    if sys.platform == "darwin":
        # Homebrew first, same as before: Apple Silicon, then Intel.
        yield "/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
        yield "/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"
        yield "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
        yield "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
        if Path("/usr/libexec/java_home").is_file():
            try:
                yield subprocess.check_output(
                    ["/usr/libexec/java_home"], text=True, stderr=subprocess.DEVNULL
                ).strip()
            except (OSError, subprocess.CalledProcessError):
                pass
        yield from Path("/Library/Java/JavaVirtualMachines").glob("*.jdk/Contents/Home")
        return

    if sys.platform == "win32":
        roots = [
            os.environ.get("ProgramFiles"),
            os.environ.get("ProgramFiles(x86)"),
            os.environ.get("LOCALAPPDATA"),
        ]
        patterns = (
            "Java/jdk-*",
            "Java/latest",
            "Eclipse Adoptium/jdk-*",
            "Microsoft/jdk-*",
            "Amazon Corretto/jdk*",
            "Zulu/zulu-*",
        )
        for root in roots:
            if not root:
                continue
            base = Path(root)
            for pattern in patterns:
                yield from base.glob(pattern)
        return

    yield "/usr/lib/jvm/java-21-openjdk"
    jvm = Path("/usr/lib/jvm")
    if jvm.is_dir():
        yield from sorted(jvm.glob("java-21*"))
        yield from sorted(jvm.glob("java-17*"))
        yield from sorted(p for p in jvm.iterdir() if p.is_dir())


java_home = os.environ.get("JAVA_HOME", "")
if not _is_jdk(java_home):
    # Drop a stale path (for example a Linux JAVA_HOME copied onto another OS).
    os.environ.pop("JAVA_HOME", None)
    for candidate in _java_homes():
        if _is_jdk(candidate):
            os.environ["JAVA_HOME"] = str(candidate)
            break
    else:
        java = shutil.which("java")
        if java:
            guessed = Path(java).resolve().parent.parent
            if _is_jdk(guessed):
                os.environ["JAVA_HOME"] = str(guessed)

if sys.platform == "win32" and _is_jdk(os.environ.get("JAVA_HOME", "")):
    java_bin = str(Path(os.environ["JAVA_HOME"]) / "bin")
    os.environ["PATH"] = java_bin + os.pathsep + os.environ.get("PATH", "")

# pip's pyspark locates itself. Only keep a system Spark that is really there.
spark_home = os.environ.get("SPARK_HOME", "")
if spark_home and not (Path(spark_home) / "bin").is_dir():
    os.environ.pop("SPARK_HOME", None)
    spark_home = ""
if not spark_home and Path("/usr/lib/spark/bin").is_dir():
    os.environ["SPARK_HOME"] = "/usr/lib/spark"
    spark_home = "/usr/lib/spark"

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

if spark_home:
    spark_python = Path(spark_home) / "python"
    if spark_python.is_dir():
        sys.path.insert(0, str(spark_python))
        for py4j in sorted((spark_python / "lib").glob("py4j-*-src.zip")):
            sys.path.insert(0, str(py4j))
            break

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
    with open(Path(resultados_dir) / f"{nome_arquivo}.txt", "w", encoding="utf-8") as arq:
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
