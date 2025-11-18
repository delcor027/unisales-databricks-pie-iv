# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")

from TableMetadataManager import DeltaTableMetadataManager
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("tb_produtos_silver_log")
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

dbutils.widgets.text("tabela_destino", "tb_produtos", "Tabela Destino")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",f"{catalogo_silver}.{banco}.{tabela_destino}", "Caminho Tabela Silver"
)
table_path = dbutils.widgets.get("table_path")

# COMMAND ----------

# SQL da Bronze -> DataFrame Silver
query = f"""
SELECT
  id_produto,
  sku,
  nome,
  categoria,
  marca,
  preco_lista,
  rank_prod,
  weight_zipf
FROM {catalogo_bronze}.{banco}.tb_produtos
"""

logger.info(f"Executando query Bronze -> Silver para tb_produtos...")
df_all = spark.sql(query)

# Tratamentos / enriquecimentos na Silver
# Janela para rank de preço dentro da categoria
w_cat_preco = Window.partitionBy("categoria").orderBy(F.col("preco_lista").desc())

df_all = (
    df_all
    # Nome amigável
    .withColumn("nm_produto", F.col("nome"))
    
    # Buckets de preço: Baixo / Médio / Alto
    .withColumn(
        "faixa_preco",
        F.when(F.col("preco_lista") < 10,  F.lit("Baixo"))
         .when(F.col("preco_lista") < 20, F.lit("Médio"))
         .otherwise(F.lit("Alto"))
    )
    
    # Nível de popularidade baseado no rank_prod
    .withColumn(
        "nivel_popularidade",
        F.when(F.col("rank_prod") <= 10,  F.lit("Top"))
         .when(F.col("rank_prod") <= 40, F.lit("Média"))
         .otherwise(F.lit("Long tail"))
    )
    
    # Flag de produto estrela (top 10 mais populares)
    .withColumn(
        "is_produto_estrela",
        (F.col("rank_prod") <= 10).cast("boolean")
    )
    
    # Custo estimado (ex.: 65% do preço lista) e margens
    .withColumn(
        "preco_custo_estimado",
        F.round(F.col("preco_lista") * F.lit(0.65), 2)
    )
    .withColumn(
        "margem_bruta_valor",
        F.round(F.col("preco_lista") - F.col("preco_custo_estimado"), 2)
    )
    .withColumn(
        "margem_bruta_perc",
        F.round(
            F.when(F.col("preco_lista") > 0,
                   F.col("margem_bruta_valor") / F.col("preco_lista"))
             .otherwise(F.lit(0.0)),
            4
        )
    )
    
    # Rank de preço dentro da categoria
    .withColumn(
        "preco_rank_categoria",
        F.row_number().over(w_cat_preco)
    )
    
    # Timestamp de carga
    .withColumn("ts_carga_silver", F.current_timestamp())
)

# Reordena colunas
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
]

df_all = df_all.select(*ordered_cols)

count_all = df_all.count()
logger.info(f"Registros preparados para Silver.tb_produtos: {count_all}")

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

    # Metadados / comentários das colunas
    column_descriptions = {
        "id_produto":            "Identificador de produto (chave de negócio) no domínio Unisales.",
        "sku":                   "Código SKU do produto.",
        "nm_produto":            "Nome descritivo do produto.",
        "categoria":             "Categoria do produto (ex.: Bebidas, Acessórios, etc.).",
        "marca":                 "Marca do produto.",
        "preco_lista":           "Preço de lista do produto na Bronze, sem descontos.",
        "faixa_preco":           "Faixa de preço categorizada (Baixo, Médio, Alto) com base no preço de lista.",
        "rank_prod":             "Posição de rank global do produto na distribuição Zipf (popularidade).",
        "preco_rank_categoria":  "Posição do produto em termos de preço dentro da própria categoria (1 = mais caro).",
        "nivel_popularidade":    "Nível de popularidade derivado do rank_prod (Top, Média, Long tail).",
        "is_produto_estrela":    "Flag booleana indicando se o produto pertence ao grupo Top 10 de popularidade.",
        "weight_zipf":           "Peso de popularidade (Zipf) utilizado na simulação de vendas.",
        "preco_custo_estimado":  "Custo estimado do produto (ex.: 65% do preço de lista, para fins de simulação).",
        "margem_bruta_valor":    "Margem bruta em valor monetário (preco_lista - preco_custo_estimado).",
        "margem_bruta_perc":     "Margem bruta percentual em relação ao preço de lista.",
        "ts_carga_silver":       "Timestamp de carga na camada Silver.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Tabela Silver de produtos do comércio Unisales. Derivada da Bronze.tb_produtos, preservando preço de lista, categoria, marca e pesos de popularidade (Zipf) para uso nas análises de vendas e estoque."
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
    f"Pipeline tb_produtos Silver concluída. "
    f"Registros carregados = {count_all}, tabela = {table_path}"
)
