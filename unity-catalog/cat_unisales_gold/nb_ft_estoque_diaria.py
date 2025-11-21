# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("ft_estoque_diaria_gold_log")
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

dbutils.widgets.text("tabela_destino", "ft_estoque_diaria", "Tabela Destino (Gold)")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",
    f"{catalogo_gold}.{banco}.{tabela_destino}",
    "Caminho Tabela Gold"
)
table_path = dbutils.widgets.get("table_path")

logger.info(f"Usando Silver={catalogo_silver}.{banco} -> Gold={table_path}, modo_save={modo_save}")

# COMMAND ----------

logger.info("Lendo tabelas Silver...")

tb_estoque_silver  = f"{catalogo_silver}.{banco}.tb_estoques"
tb_produtos_silver = f"{catalogo_silver}.{banco}.tb_produtos"
tb_lojas_silver    = f"{catalogo_silver}.{banco}.tb_lojas"

df_estoque  = spark.table(tb_estoque_silver)
df_produtos = spark.table(tb_produtos_silver)
df_lojas    = spark.table(tb_lojas_silver)

logger.info(
    f"Estoque: {df_estoque.count()} | "
    f"Produtos: {df_produtos.count()} | "
    f"Lojas: {df_lojas.count()}"
)

logger.info("Construindo fato de estoque diário com chaves e métricas...")

df_fato = (
    df_estoque
    .select(
        "dt_estoque",
        "id_loja",
        "id_produto",
        "faixa_estoque",
        "qtd_estoque",
        "ts_carga_silver",
    )
    .withColumn("ts_carga_gold", F.current_timestamp())
)

# Reordenação de colunas (dimensões primeiro, métricas depois)
ordered_cols = [
    "dt_estoque",
    "id_loja",
    "id_produto",
    "faixa_estoque",
    "qtd_estoque",
    "ts_carga_silver",
    "ts_carga_gold",
]

df_fato = df_fato.select(*ordered_cols)

count_fato = df_fato.count()
logger.info(f"Registros em ft_estoque_diaria: {count_fato}")

# COMMAND ----------

if not spark.catalog.tableExists(table_path):
    logger.info(f"Tabela {table_path} não existe. Criando pela primeira vez...")

    (
        df_fato.write
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("mergeSchema", "true")
        .format("delta")
        .mode(modo_save)
        .partitionBy("dt_estoque")
        .saveAsTable(table_path)
    )

    logger.info("Tabela criada com sucesso. Aplicando metadados e comentários...")

    column_descriptions = {
        "dt_estoque":      "Data de referência do snapshot de estoque (FK para dim_calendario.dt_calendario).",
        "id_loja":         "Identificador da loja (FK para dim_lojas.id_loja).",
        "id_produto":      "Identificador do produto (FK para dim_produtos.id_produto).",
        "faixa_estoque":   "Faixa categorizada de estoque (Baixo, Médio, Alto) no dia.",
        "qtd_estoque":     "Quantidade em estoque do produto na loja e dia (snapshot diário).",
        "ts_carga_silver": "Timestamp de carga do registro na camada Silver de estoque.",
        "ts_carga_gold":   "Timestamp de carga na camada Gold.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Fato diária de estoque na camada Gold do comércio Unisales. "
        "Cada linha representa o snapshot de estoque de um produto em uma loja e data, "
        "referenciando dimensões de loja e produto via chaves (id_loja, id_produto)."
    )
    DeltaTableMetadataManager.add_table_comment(table_path, table_description)

    logger.info("Comentários aplicados com sucesso.")
else:
    logger.info(f"Tabela {table_path} já existe. Atualizando conforme modo_save={modo_save}...")

    (
        df_fato.write
        .format("delta")
        .mode(modo_save)
        .partitionBy("dt_estoque")
        .saveAsTable(table_path)
    )

logger.info(
    f"Pipeline Gold.ft_estoque_diaria concluída. "
    f"Registros carregados = {count_fato}, tabela = {table_path}"
)
