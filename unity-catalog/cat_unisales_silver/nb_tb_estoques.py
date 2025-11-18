# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("tb_estoque_silver_log")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False

# COMMAND ----------

dbutils.widgets.dropdown("modo_save", "overwrite", ["append", "overwrite"], "Modo de Save")
modo_save = dbutils.widgets.get("modo_save")

dbutils.widgets.text("catalogo_bronze", "cat_unisales_bronze", "Catálogo Bronze")
catalogo_bronze = dbutils.widgets.get("catalogo_bronze")

dbutils.widgets.text("catalogo_silver", "cat_unisales_silver", "Catálogo Silver")
catalogo_silver = dbutils.widgets.get("catalogo_silver")

dbutils.widgets.text("banco", "db_comercio", "Banco/Schema")
banco = dbutils.widgets.get("banco")

dbutils.widgets.text("tabela_destino", "tb_estoque", "Tabela Destino")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",
    f"{catalogo_silver}.{banco}.{tabela_destino}",
    "Caminho Tabela Silver"
)
table_path = dbutils.widgets.get("table_path")

# COMMAND ----------

# SQL da Bronze -> DataFrame Silver
query = f"""
SELECT
  dt,
  id_produto,
  id_loja,
  qtd_estoque
FROM {catalogo_bronze}.{banco}.tb_estoque
"""

logger.info("Executando query Bronze -> Silver para tb_estoque...")
df_all = spark.sql(query)

# Tratamentos / enriquecimentos
df_all = (
    df_all
    .withColumn("dt_estoque", F.col("dt"))
    .drop("dt")
    .withColumn("ano_estoque", F.year("dt_estoque"))
    .withColumn("mes_estoque", F.month("dt_estoque"))
    .withColumn("ano_mes_estoque", F.date_format("dt_estoque", "yyyy-MM"))
    # Dia da semana numérico (1 = domingo) e abreviação textual
    .withColumn("dia_semana_estoque_num", F.dayofweek("dt_estoque"))
    .withColumn("dia_semana_estoque", F.date_format("dt_estoque", "E"))
    # Faixa de estoque
    .withColumn(
        "faixa_estoque",
        F.when(F.col("qtd_estoque") < 105, F.lit("Baixo"))
         .when(F.col("qtd_estoque") < 115, F.lit("Médio"))
         .otherwise(F.lit("Alto"))
    )
    .withColumn("ts_carga_silver", F.current_timestamp())
)

ordered_cols = [
    "dt_estoque",
    "ano_estoque",
    "mes_estoque",
    "ano_mes_estoque",
    "dia_semana_estoque_num",
    "dia_semana_estoque",
    "id_loja",
    "id_produto",
    "qtd_estoque",
    "faixa_estoque",
    "ts_carga_silver",
]

df_all = df_all.select(*ordered_cols)

count_all = df_all.count()
logger.info(f"Registros preparados para Silver.tb_estoque: {count_all}")

# COMMAND ----------

# Criação / Escrita da tabela Delta Silver
if not spark.catalog.tableExists(table_path):
    logger.info(f"Tabela {table_path} não existe. Criando pela primeira vez...")

    (
        df_all.write
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("mergeSchema", "true")
        .format("delta")
        .mode(modo_save)
        .saveAsTable(table_path)
    )

    logger.info("Tabela criada com sucesso. Aplicando metadados e comentários...")

    column_descriptions = {
        "dt_estoque":             "Data de referência do saldo de estoque.",
        "ano_estoque":            "Ano da data de estoque.",
        "mes_estoque":            "Mês numérico da data de estoque.",
        "ano_mes_estoque":        "Ano e mês da data de estoque no formato yyyy-MM.",
        "dia_semana_estoque_num": "Dia da semana numérico (1=domingo, 7=sábado).",
        "dia_semana_estoque":     "Dia da semana abreviado (ex.: Mon, Tue).",
        "id_loja":                "Identificador da loja.",
        "id_produto":             "Identificador do produto.",
        "qtd_estoque":            "Quantidade em estoque para o produto/loja/data.",
        "faixa_estoque":          "Faixa categorizada da quantidade de estoque (Baixo, Médio, Alto).",
        "ts_carga_silver":        "Timestamp de carga na camada Silver.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Tabela Silver de estoque diário do comércio Unisales. Derivada da Bronze.tb_estoque, "
        "com enriquecimentos de calendário (ano, mês, dia da semana) e classificação de faixa de estoque."
    )
    DeltaTableMetadataManager.add_table_comment(table_path, table_description)

    logger.info("Comentários aplicados com sucesso.")

else:
    logger.info(f"Tabela {table_path} já existe. Escrevendo de acordo com modo_save={modo_save}...")
    (
        df_all.write
        .format("delta")
        .mode(modo_save)
        .saveAsTable(table_path)
    )

logger.info(
    f"Pipeline tb_estoque Silver concluída. "
    f"Registros carregados = {count_all}, tabela = {table_path}"
)
