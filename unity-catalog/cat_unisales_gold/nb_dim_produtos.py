# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("dim_produtos_gold_log")
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

dbutils.widgets.text("tabela_destino", "dim_produtos", "Tabela Destino (Gold)")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",
    f"{catalogo_gold}.{banco}.{tabela_destino}",
    "Caminho Tabela Gold"
)
table_path = dbutils.widgets.get("table_path")

logger.info(f"Construindo {table_path} a partir de {catalogo_silver}.{banco}.tb_produtos")

# COMMAND ----------

tb_produtos_silver = f"{catalogo_silver}.{banco}.tb_produtos"
df_produtos = spark.table(tb_produtos_silver)

df_produtos = df_produtos.dropDuplicates(["id_produto"])

df_dim = (
    df_produtos
    .withColumn("ts_carga_gold", F.current_timestamp())
)

ordered_cols = [
    "id_produto",
    "sku",
    "nm_produto",
    "categoria",
    "marca",
    "preco_lista",
    "faixa_preco",
    "rank_prod",
    "preco_rank_categoria",
    "nivel_popularidade",
    "is_produto_estrela",
    "weight_zipf",
    "preco_custo_estimado",
    "margem_bruta_valor",
    "margem_bruta_perc",
    "ts_carga_silver",
    "ts_carga_gold",
]

df_dim = df_dim.select(*ordered_cols)

count_dim = df_dim.count()
logger.info(f"Registros em dim_produtos: {count_dim}")

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
        "id_produto":           "Chave de negócio do produto.",
        "sku":                  "Código SKU do produto.",
        "nm_produto":           "Nome do produto.",
        "categoria":            "Categoria do produto.",
        "marca":                "Marca do produto.",
        "preco_lista":          "Preço de lista do produto.",
        "faixa_preco":          "Faixa de preço categorizada (Baixo, Médio, Alto).",
        "rank_prod":            "Rank global de popularidade (Zipf).",
        "preco_rank_categoria": "Rank de preço do produto dentro da categoria.",
        "nivel_popularidade":   "Nível de popularidade (Top, Média, Long tail).",
        "is_produto_estrela":   "Flag booleana de produto estrela (Top 10).",
        "weight_zipf":          "Peso de popularidade (Zipf).",
        "preco_custo_estimado": "Custo estimado do produto (simulação).",
        "margem_bruta_valor":   "Margem bruta em valor.",
        "margem_bruta_perc":    "Margem bruta percentual.",
        "ts_carga_silver":      "Timestamp de carga na Silver.",
        "ts_carga_gold":        "Timestamp de carga na Gold.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Dimensão de produtos na camada Gold, derivada de cat_unisales_silver.db_comercio.tb_produtos. "
        "Centraliza atributos de produto para análises comerciais, dashboards e uso via Genie."
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

logger.info(f"Pipeline Gold.dim_produtos concluído. Registros = {count_dim}, tabela = {table_path}")
