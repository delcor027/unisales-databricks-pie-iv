# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("tb_vendas_silver_log")
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

dbutils.widgets.text("tabela_destino", "tb_vendas", "Tabela Destino")
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
  dt_venda,
  id_pedido,
  id_loja,
  id_produto,
  qtd,
  receita,
  desconto,
  ts_bronze
FROM {catalogo_bronze}.{banco}.tb_vendas
"""

logger.info("Executando query Bronze -> Silver para tb_vendas...")
df_all = spark.sql(query)

# Tratamentos / enriquecimentos
df_all = (
    df_all
    .withColumn("ano_venda", F.year("dt_venda"))
    .withColumn("mes_venda", F.month("dt_venda"))
    .withColumn("ano_mes_venda", F.date_format("dt_venda", "yyyy-MM"))
    .withColumn("dia_semana_venda_num", F.dayofweek("dt_venda"))
    .withColumn("dia_semana_venda", F.date_format("dt_venda", "E"))
    # Receita bruta = receita líquida + desconto
    .withColumn("receita_bruta", F.col("receita") + F.col("desconto"))
    # Percentual de desconto
    .withColumn(
        "perc_desconto",
        F.round(
            F.when(F.col("receita_bruta") > 0, F.col("desconto") / F.col("receita_bruta"))
             .otherwise(F.lit(0.0)),
            4
        )
    )
    # Valor unitário médio (considerando receita bruta)
    .withColumn(
        "vl_unitario_medio",
        F.round(
            F.when(F.col("qtd") > 0, F.col("receita_bruta") / F.col("qtd"))
             .otherwise(F.lit(None).cast("decimal(18,2)")),
            2
        )
    )
    .withColumn("ts_carga_silver", F.current_timestamp())
)

ordered_cols = [
    "dt_venda",
    "ano_venda",
    "mes_venda",
    "ano_mes_venda",
    "dia_semana_venda_num",
    "dia_semana_venda",
    "id_pedido",
    "id_loja",
    "id_produto",
    "qtd",
    "receita_bruta",
    "receita",
    "desconto",
    "perc_desconto",
    "vl_unitario_medio",
    "ts_bronze",
    "ts_carga_silver",
]

df_all = df_all.select(*ordered_cols)

count_all = df_all.count()
logger.info(f"Registros preparados para Silver.tb_vendas: {count_all}")

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
        "dt_venda":              "Data da venda (data do pedido/item).",
        "ano_venda":             "Ano da data de venda.",
        "mes_venda":             "Mês numérico da data de venda.",
        "ano_mes_venda":         "Ano e mês da data de venda no formato yyyy-MM.",
        "dia_semana_venda_num":  "Dia da semana numérico (1=domingo, 7=sábado).",
        "dia_semana_venda":      "Dia da semana abreviado (ex.: Mon, Tue).",
        "id_pedido":             "Identificador sintético do pedido (data-loja-sequência).",
        "id_loja":               "Identificador da loja.",
        "id_produto":            "Identificador do produto vendido.",
        "qtd":                   "Quantidade de itens na linha da venda.",
        "receita_bruta":         "Valor bruto da venda do item (receita líquida + desconto).",
        "receita":               "Receita líquida do item após desconto.",
        "desconto":              "Valor de desconto concedido na linha do item.",
        "perc_desconto":         "Percentual de desconto sobre a receita bruta da linha.",
        "vl_unitario_medio":     "Valor unitário médio (receita bruta dividida pela quantidade).",
        "ts_bronze":             "Timestamp de inserção do registro na Bronze.",
        "ts_carga_silver":       "Timestamp de carga na camada Silver.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Tabela Silver de vendas por item do comércio Unisales. Derivada da Bronze.tb_vendas, "
        "com enriquecimentos de calendário, cálculo de receita bruta, percentual de desconto "
        "e valor unitário médio para análises comerciais."
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
    f"Pipeline tb_vendas Silver concluída. "
    f"Registros carregados = {count_all}, tabela = {table_path}"
)
