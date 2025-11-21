# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("dim_calendario_gold_log")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False

# COMMAND ----------

dbutils.widgets.dropdown("modo_save", "overwrite", ["append", "overwrite"], "Modo de Save")
modo_save = dbutils.widgets.get("modo_save")

dbutils.widgets.text("catalogo_silver", "cat_unisales_silver", "Catálogo Silver")
catalogo_silver = dbutils.widgets.get("catalogo_silver")

dbutils.widgets.text("catalogo_gold", "cat_unisales_gold", "Catálogo Gold")
catalogo_gold = dbutils.widgets.get("catalogo_gold")

dbutils.widgets.text("banco", "db_comercio", "Banco/Schema")
banco = dbutils.widgets.get("banco")

dbutils.widgets.text("tabela_destino", "dim_calendario", "Tabela Destino (Gold)")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",
    f"{catalogo_gold}.{banco}.{tabela_destino}",
    "Caminho Tabela Gold"
)
table_path = dbutils.widgets.get("table_path")

logger.info(f"Construindo {table_path} a partir de {catalogo_silver}.{banco}.tb_vendas")

# COMMAND ----------

tb_vendas_silver = f"{catalogo_silver}.{banco}.tb_vendas"
df_vendas = spark.table(tb_vendas_silver)

df_min_max = df_vendas.select(
    F.min("dt_venda").alias("dt_min"),
    F.max("dt_venda").alias("dt_max")
).collect()[0]

dt_min = df_min_max["dt_min"]
dt_max = df_min_max["dt_max"]

logger.info(f"Range de datas encontrado: {dt_min} até {dt_max}")

# Se por algum motivo estiver vazio, define um range padrão
if dt_min is None or dt_max is None:
    logger.warning("Sem dados em tb_vendas. Usando range padrão de 30 dias a partir de hoje.")
    hoje = F.current_date()
    df_range = spark.range(0, 30).select(
        F.date_add(hoje, F.col("id")).alias("dt_calendario")
    )
else:
    # Gera sequência de datas dt_min -> dt_max
    df_range = (
        spark.sql(f"SELECT sequence(date('{dt_min}'), date('{dt_max}'), interval 1 day) as dt_seq")
        .select(F.explode("dt_seq").alias("dt_calendario"))
    )

# Enriquecimentos de calendário
df_dim = (
    df_range
    .withColumn("ano", F.year("dt_calendario"))
    .withColumn("mes", F.month("dt_calendario"))
    .withColumn("dia", F.dayofmonth("dt_calendario"))
    .withColumn("trimestre", F.quarter("dt_calendario"))
    .withColumn("ano_mes", F.date_format("dt_calendario", "yyyy-MM"))
    .withColumn("nome_mes", F.date_format("dt_calendario", "MMMM"))
    .withColumn("dia_semana_num", F.dayofweek("dt_calendario"))
    .withColumn("dia_semana_nome", F.date_format("dt_calendario", "EEEE"))
    .withColumn("is_final_semana", F.col("dia_semana_num").isin(1, 7))  # domingo e sábado
    .withColumn("ts_carga_gold", F.current_timestamp())
)


ordered_cols = [
    "dt_calendario",
    "ano",
    "mes",
    "dia",
    "trimestre",
    "ano_mes",
    "nome_mes",
    "dia_semana_num",
    "dia_semana_nome",
    "is_final_semana",
    "ts_carga_gold",
]

df_dim = df_dim.select(*ordered_cols)

count_dim = df_dim.count()
logger.info(f"Registros em dim_calendario: {count_dim}")

# COMMAND ----------

if not spark.catalog.tableExists(table_path):
    logger.info(f"Tabela {table_path} não existe. Criando pela primeira vez...")

    (
        df_dim.write
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("mergeSchema", "true")
        .format("delta")
        .mode(modo_save)
        .saveAsTable(table_path)
    )

    column_descriptions = {
        "dt_calendario":    "Data do calendário (chave da dimensão).",
        "ano":              "Ano da data.",
        "mes":              "Mês numérico da data (1-12).",
        "dia":              "Dia do mês.",
        "trimestre":        "Trimestre do ano (1-4).",
        "ano_mes":          "Ano e mês no formato yyyy-MM.",
        "nome_mes":         "Nome do mês (dependente de locale).",
        "dia_semana_num":   "Dia da semana numérico (1=segunda, 7=domingo).",
        "dia_semana_nome":  "Nome do dia da semana.",
        "is_final_semana":  "Flag booleana indicando se é sábado/domingo.",
        "ts_carga_gold":    "Timestamp de carga na Gold.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Dimensão de calendário na camada Gold, gerada a partir do range de datas presente em tb_vendas Silver. "
        "Usada para análises por ano, mês, dia da semana, fim de semana, etc., em dashboards e Genie."
    )
    DeltaTableMetadataManager.add_table_comment(table_path, table_description)

    logger.info("Tabela e comentários criados com sucesso.")
else:
    logger.info(f"Tabela {table_path} já existe. Atualizando conforme modo_save={modo_save}...")

    (
        df_dim.write
        .format("delta")
        .mode(modo_save)
        .saveAsTable(table_path)
    )

logger.info(f"Pipeline Gold.dim_calendario concluído. Registros = {count_dim}, tabela = {table_path}")
