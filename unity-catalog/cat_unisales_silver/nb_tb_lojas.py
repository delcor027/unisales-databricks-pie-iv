# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("tb_lojas_silver_log")
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

dbutils.widgets.text("tabela_destino", "tb_lojas", "Tabela Destino")
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
  id_loja,
  nome,
  cidade,
  uf
FROM {catalogo_bronze}.{banco}.tb_lojas
"""

logger.info("Executando query Bronze -> Silver para tb_lojas...")
df_all = spark.sql(query)

# Tratamentos / enriquecimentos
df_all = (
    df_all
    .withColumn("nm_loja", F.col("nome"))
    .withColumn("id_loja_int", F.col("id_loja").cast("int"))
    # Região da loja (simples: Grande Vitória x Interior)
    .withColumn(
        "regiao_loja",
        F.when(F.col("cidade").isin("Vitoria", "Serra", "Vila Velha", "Cariacica", "Guarapari"),
               F.lit("Grande Vitória"))
         .otherwise(F.lit("Interior"))
    )
    # Flag capital
    .withColumn(
        "is_capital",
        (F.col("cidade") == F.lit("Vitoria")).cast("boolean")
    )
    .withColumn("ts_carga_silver", F.current_timestamp())
)

ordered_cols = [
    "id_loja",
    "id_loja_int",
    "nm_loja",
    "cidade",
    "regiao_loja",
    "uf",
    "is_capital",
    "ts_carga_silver",
]

df_all = df_all.select(*ordered_cols)

count_all = df_all.count()
logger.info(f"Registros preparados para Silver.tb_lojas: {count_all}")

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
        "id_loja":         "Identificador da loja (string) gerado na camada Bronze.",
        "id_loja_int":     "Identificador numérico da loja (cast de id_loja).",
        "nm_loja":         "Nome descritivo da loja.",
        "cidade":          "Cidade onde a loja está localizada.",
        "regiao_loja":     "Região categorizada da loja (ex.: Grande Vitória, Interior).",
        "uf":              "Unidade federativa da loja (estado).",
        "is_capital":      "Flag booleana indicando se a loja está na capital (Vitória).",
        "ts_carga_silver": "Timestamp de carga na camada Silver.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Tabela Silver de lojas do comércio Unisales. Derivada da Bronze.tb_lojas, "
        "com enriquecimentos de região, flag de capital e id numérico para suporte a análises e joins."
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
    f"Pipeline tb_lojas Silver concluída. "
    f"Registros carregados = {count_all}, tabela = {table_path}"
)
