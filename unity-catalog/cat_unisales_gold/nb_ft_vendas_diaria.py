# Databricks notebook source
import sys
import logging
from pyspark.sql import functions as F
from delta.tables import DeltaTable

sys.path.append("/Workspace/Repos/PRD/unisales-databricks-pie-iv/databricks-libs")
from TableMetadataManager import DeltaTableMetadataManager

spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("ft_vendas_diaria_gold_log")
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

dbutils.widgets.text("tabela_destino", "ft_vendas_diaria", "Tabela Destino (Gold)")
tabela_destino = dbutils.widgets.get("tabela_destino")

dbutils.widgets.text(
    "table_path",
    f"{catalogo_gold}.{banco}.{tabela_destino}",
    "Caminho Tabela Gold"
)
table_path = dbutils.widgets.get("table_path")

logger.info(f"Usando Silver={catalogo_silver}.{banco} -> Gold={table_path}, modo_save={modo_save}")

# COMMAND ----------

# Leitura das tabelas Silver
logger.info("Lendo tabelas Silver...")

tb_vendas_silver = f"{catalogo_silver}.{banco}.tb_vendas"
tb_produtos_silver = f"{catalogo_silver}.{banco}.tb_produtos"
tb_lojas_silver = f"{catalogo_silver}.{banco}.tb_lojas"
tb_estoque_silver = f"{catalogo_silver}.{banco}.tb_estoques"

df_vendas = spark.table(tb_vendas_silver)
df_produtos = spark.table(tb_produtos_silver)
df_lojas = spark.table(tb_lojas_silver)
df_estoque = spark.table(tb_estoque_silver)

logger.info(f"Vendas: {df_vendas.count()} | Produtos: {df_produtos.count()} | Lojas: {df_lojas.count()} | Estoque: {df_estoque.count()}")

# Join e enriquecimentos para a Fato Diária
logger.info("Realizando joins entre vendas, produtos, lojas e estoque...")

# Seleciona somente colunas relevantes das dimensões
df_produtos_sel = df_produtos.select(
    "id_produto",
    "sku",
    "nm_produto",
    "categoria",
    "marca",
    "faixa_preco",
    "nivel_popularidade",
    "is_produto_estrela"
)

df_lojas_sel = df_lojas.select(
    "id_loja",
    "id_loja_int",
    "nm_loja",
    "cidade",
    "regiao_loja",
    "uf",
    "is_capital"
)

df_estoque_sel = (
    df_estoque
    .select(
        F.col("dt_estoque"),
        F.col("id_loja").alias("id_loja_estoque"),
        F.col("id_produto").alias("id_produto_estoque"),
        F.col("qtd_estoque"),
        F.col("faixa_estoque")
    )
)


# Join principal: Vendas + Produtos + Lojas
df_join = (
    df_vendas.alias("v")
    .join(df_produtos_sel.alias("p"), on="id_produto", how="left")
    .join(df_lojas_sel.alias("l"), on="id_loja", how="left")
    .join(
        df_estoque_sel.alias("e"),
        (
            (F.col("v.dt_venda") == F.col("e.dt_estoque")) &
            (F.col("v.id_loja") == F.col("e.id_loja_estoque")) &
            (F.col("v.id_produto") == F.col("e.id_produto_estoque"))
        ),
        how="left"
    )
)

# se quiser ter a data de estoque explícita, mantém essa linha:
df_join = df_join.drop("id_loja_estoque", "id_produto_estoque")

# Agregação por dia / loja / produto
logger.info("Agregando métricas por dt_venda, loja e produto...")

# Chaves da fato (grão: dia x loja x produto)
chaves_fato = [
    "dt_venda",
    "id_loja",
    "id_produto",
]

# Agregações
df_fato = (
    df_join
    .groupBy(*chaves_fato)
    .agg(
        # Volume
        F.sum("qtd").alias("qtd_vendida"),
        F.countDistinct("id_pedido").alias("qtde_pedidos"),
        
        # Receita / desconto
        F.round(F.sum("receita_bruta"), 2).alias("receita_bruta_total"),
        F.round(F.sum("receita"), 2).alias("receita_liquida_total"),
        F.round(F.sum("desconto"), 2).alias("desconto_total"),
        
        # Estoque
        F.round(F.avg("qtd_estoque"), 2).alias("estoque_medio_dia"),
        F.max("qtd_estoque").alias("estoque_final_dia"),
        
        # Auxiliares para cálculo com expr
        F.sum("receita_bruta").alias("aux_sum_receita_bruta"),
        F.sum("desconto").alias("aux_sum_desconto"),
    )
)

# Cálculos derivados com expr (evita problemas de agregação dentro de when)
df_fato = (
    df_fato
    # Percentual médio de desconto no dia
    .withColumn(
        "perc_desconto_medio",
        F.expr(
            "CASE WHEN aux_sum_receita_bruta > 0 "
            "THEN ROUND(aux_sum_desconto / aux_sum_receita_bruta, 4) "
            "ELSE 0.0 END"
        )
    )
    # Valor unitário médio diário
    .withColumn(
        "vl_unitario_medio_dia",
        F.expr(
            "CASE WHEN qtd_vendida > 0 "
            "THEN ROUND(aux_sum_receita_bruta / qtd_vendida, 2) "
            "ELSE NULL END"
        )
    )
    # Ticket médio diário (por pedido)
    .withColumn(
        "ticket_medio_dia",
        F.expr(
            "CASE WHEN qtde_pedidos > 0 "
            "THEN ROUND(receita_liquida_total / qtde_pedidos, 2) "
            "ELSE 0.0 END"
        )
    )
    # Timestamp de carga Gold
    .withColumn("ts_carga_gold", F.current_timestamp())
)

# Remove colunas auxiliares
df_fato = df_fato.drop("aux_sum_receita_bruta", "aux_sum_desconto")

# Reordena colunas (dimensões primeiro, métricas depois)
ordered_cols = [
    "dt_venda",
    "id_loja",
    "id_produto",
    "qtd_vendida",
    "qtde_pedidos",
    "receita_bruta_total",
    "receita_liquida_total",
    "desconto_total",
    "perc_desconto_medio",
    "vl_unitario_medio_dia",
    "ticket_medio_dia",
    "estoque_medio_dia",
    "estoque_final_dia",
    "ts_carga_gold",
]


df_fato = df_fato.select(*ordered_cols)

count_fato = df_fato.count()
logger.info(f"Registros agregados para Gold.ft_vendas_diaria: {count_fato}")

# COMMAND ----------

# Escrita da tabela Delta Gold
if not spark.catalog.tableExists(table_path):
    logger.info(f"Tabela {table_path} não existe. Criando pela primeira vez...")

    (
        df_fato.write
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("mergeSchema", "true")
        .format("delta")
        .mode(modo_save)
        .partitionBy("dt_venda")
        .saveAsTable(table_path)
    )

    logger.info("Tabela criada com sucesso. Aplicando metadados e comentários...")

    column_descriptions = {
        "dt_venda":             "Data da venda (FK para dim_calendario.dt_calendario).",
        "id_loja":              "Identificador da loja (FK para dim_lojas.id_loja).",
        "id_produto":           "Identificador do produto (FK para dim_produtos.id_produto).",
        "qtd_vendida":          "Quantidade total vendida do produto na loja e dia.",
        "qtde_pedidos":         "Quantidade de pedidos distintos que contiveram o produto na loja e dia.",
        "receita_bruta_total":  "Receita bruta total da linha (soma de receita_bruta) na loja e dia.",
        "receita_liquida_total":"Receita líquida total (após descontos) na loja e dia.",
        "desconto_total":       "Total de descontos concedidos na loja e dia.",
        "perc_desconto_medio":  "Percentual médio de desconto (desconto_total / receita_bruta_total).",
        "vl_unitario_medio_dia":"Valor unitário médio ponderado pela quantidade vendida no dia.",
        "ticket_medio_dia":     "Ticket médio diário (receita_liquida_total / qtde_pedidos).",
        "estoque_medio_dia":    "Estoque médio diário do produto na loja (média de qtd_estoque).",
        "estoque_final_dia":    "Estoque final do produto na loja ao fim do dia (max de qtd_estoque).",
        "ts_carga_gold":        "Timestamp de carga na camada Gold.",
    }

    DeltaTableMetadataManager.add_column_comments(table_path, column_descriptions)

    table_description = (
        "Fato diária de vendas do comércio Unisales na camada Gold. "
        "Agrega métricas de vendas por dia, loja e produto, combinando informações "
        "de vendas, produtos, lojas e estoque. "
        "Tabela otimizada para consumo em dashboards (SQL Warehouse / Databricks Dashboards) "
        "e para exploração via Genie."
    )
    DeltaTableMetadataManager.add_table_comment(table_path, table_description)

    logger.info("Comentários aplicados com sucesso.")

else:
    logger.info(f"Tabela {table_path} já existe. Escrevendo de acordo com modo_save={modo_save}...")

    (
        df_fato.write
        .format("delta")
        .mode(modo_save)
        .partitionBy("dt_venda")
        .saveAsTable(table_path)
    )

logger.info(
    f"Pipeline Gold.ft_vendas_diaria concluída. "
    f"Registros carregados = {count_fato}, tabela = {table_path}"
)

