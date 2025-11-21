# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("dim_lojas_gold_log")
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

dbutils.widgets.text("tabela_destino", "dim_lojas", "Tabela Destino (Gold)")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",
    f"{catalogo_gold}.{banco}.{tabela_destino}",
    "Caminho Tabela Gold"
)
table_path = dbutils.widgets.get("table_path")

logger.info(f"Construindo {table_path} a partir de {catalogo_silver}.{banco}.tb_lojas")

# COMMAND ----------

tb_lojas_silver = f"{catalogo_silver}.{banco}.tb_lojas"
df_lojas = spark.table(tb_lojas_silver)

# remove duplicidade caso exista
df_lojas = df_lojas.dropDuplicates(["id_loja"])

# adiciona coluna técnica de carga na Gold
df_dim = df_lojas.withColumn("ts_carga_gold", F.current_timestamp())

ordered_cols = [
    "id_loja",
    "id_loja_int",
    "nm_loja",
    "cidade",
    "regiao_loja",
    "uf",
    "is_capital",
    "ts_carga_silver",
    "ts_carga_gold",
]

df_dim = df_dim.select(*ordered_cols)

count_dim = df_dim.count()
logger.info(f"Registros em dim_lojas: {count_dim}")

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
        "id_loja":         "Chave de negócio da loja (string).",
        "id_loja_int":     "Chave numérica da loja.",
        "nm_loja":         "Nome da loja.",
        "cidade":          "Cidade onde a loja está localizada.",
        "regiao_loja":     "Região da loja (Grande Vitória, Interior, etc.).",
        "uf":              "UF da loja.",
        "is_capital":      "Flag booleana indicando se a loja está na capital.",
        "ts_carga_silver": "Timestamp de carga na camada Silver.",
        "ts_carga_gold":   "Timestamp de carga na camada Gold.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Dimensão de lojas na camada Gold, derivada de cat_unisales_silver.db_comercio.tb_lojas. "
        "Usada em análises de vendas por loja, cidade, região e UF, bem como no consumo via Genie."
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

logger.info(f"Pipeline Gold.dim_lojas concluído. Registros = {count_dim}, tabela = {table_path}")
