# Databricks notebook source
import sys
import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

logger = logging.getLogger("gold_constraints_log")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stdout)
    _h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
logger.propagate = False

# COMMAND ----------

dbutils.widgets.text("catalogo_gold", "cat_unisales_gold", "Catálogo Gold")
catalogo_gold = dbutils.widgets.get("catalogo_gold")

dbutils.widgets.text("banco", "db_comercio", "Banco/Schema")
banco = dbutils.widgets.get("banco")

logger.info(f"Aplicando constraints em {catalogo_gold}.{banco} ...")

# COMMAND ----------

# Tabelas alvo
dim_lojas_name      = "dim_lojas"
dim_produtos_name   = "dim_produtos"
dim_calendario_name = "dim_calendario"
ft_vendas_name      = "ft_vendas_diaria"
ft_estoque_name     = "ft_estoque_diaria"

dim_lojas      = f"{catalogo_gold}.{banco}.{dim_lojas_name}"
dim_produtos   = f"{catalogo_gold}.{banco}.{dim_produtos_name}"
dim_calendario = f"{catalogo_gold}.{banco}.{dim_calendario_name}"
ft_vendas      = f"{catalogo_gold}.{banco}.{ft_vendas_name}"
ft_estoque     = f"{catalogo_gold}.{banco}.{ft_estoque_name}"

def constraint_exists(
    constraint_name: str,
    table_catalog: str,
    table_schema: str,
    table_name: str,
) -> bool:
    """
    Verifica se uma constraint já existe usando INFORMATION_SCHEMA.TABLE_CONSTRAINTS.
    """
    query = f"""
    SELECT 1
    FROM {table_catalog}.information_schema.table_constraints
    WHERE table_schema    = '{table_schema}'
      AND table_name      = '{table_name}'
      AND constraint_name = '{constraint_name}'
    LIMIT 1
    """
    result = spark.sql(query).collect()
    return len(result) > 0


def run_ddl_with_check(
    constraint_name: str,
    table_fqn: str,
    table_name_only: str,
    sql: str,
):
    """
    Executa o ALTER TABLE ADD CONSTRAINT somente se a constraint ainda não existir.
    """
    logger.info(f"Verificando existência da constraint '{constraint_name}' em {table_fqn}...")

    if constraint_exists(
        constraint_name=constraint_name,
        table_catalog=catalogo_gold,
        table_schema=banco,
        table_name=table_name_only,
    ):
        logger.info(f"Constraint '{constraint_name}' já existe em {table_fqn}. Pulando criação.")
        return

    logger.info(f"Constraint '{constraint_name}' não existe. Aplicando...")
    logger.debug(f"SQL:\n{sql}")
    try:
        spark.sql(sql)
        logger.info(f"Constraint '{constraint_name}' aplicada com sucesso em {table_fqn}.")
    except Exception as e:
        logger.warning(f"Falha ao aplicar constraint '{constraint_name}' em {table_fqn}: {e}")


def ensure_not_null(
    table_fqn: str,
    table_name_only: str,
    column_name: str,
) -> bool:
    """
    Garante que a coluna não permita NULL:
      - Se tiver valores nulos -> loga e retorna False (não dá pra criar PK).
      - Se não tiver nulos -> ALTER TABLE ... ALTER COLUMN ... SET NOT NULL e retorna True.
    """
    logger.info(f"Verificando nullability de {table_fqn}.{column_name} ...")

    null_count_query = f"""
    SELECT COUNT(*) AS qtde
    FROM {table_fqn}
    WHERE {column_name} IS NULL
    """
    null_count = spark.sql(null_count_query).collect()[0]["qtde"]

    if null_count > 0:
        logger.warning(
            f"Coluna {table_fqn}.{column_name} possui {null_count} valores nulos. "
            f"Não é possível definir NOT NULL / PK sem tratar esses dados. "
            f"Pulando definição de PK para esta coluna."
        )
        return False

    logger.info(f"Coluna {table_fqn}.{column_name} não possui nulos. Aplicando SET NOT NULL...")
    alter_sql = f"""
    ALTER TABLE {table_fqn}
    ALTER COLUMN {column_name} SET NOT NULL
    """
    try:
        spark.sql(alter_sql)
        logger.info(f"Coluna {table_fqn}.{column_name} ajustada para NOT NULL com sucesso.")
        return True
    except Exception as e:
        logger.warning(
            f"Falha ao aplicar SET NOT NULL em {table_fqn}.{column_name}: {e}. "
            f"PK não será criada para essa coluna."
        )
        return False

# COMMAND ----------

# 1) PRIMARY KEYS nas dimensões

# dim_lojas: PK em id_loja
pk_dim_lojas_name = "pk_dim_lojas"

if ensure_not_null(dim_lojas, dim_lojas_name, "id_loja"):
    sql_pk_dim_lojas = f"""
    ALTER TABLE {dim_lojas}
    ADD CONSTRAINT {pk_dim_lojas_name}
    PRIMARY KEY (id_loja) NOT ENFORCED
    """
    run_ddl_with_check(
        constraint_name=pk_dim_lojas_name,
        table_fqn=dim_lojas,
        table_name_only=dim_lojas_name,
        sql=sql_pk_dim_lojas,
    )

# dim_produtos: PK em id_produto
pk_dim_produtos_name = "pk_dim_produtos"

if ensure_not_null(dim_produtos, dim_produtos_name, "id_produto"):
    sql_pk_dim_produtos = f"""
    ALTER TABLE {dim_produtos}
    ADD CONSTRAINT {pk_dim_produtos_name}
    PRIMARY KEY (id_produto) NOT ENFORCED
    """
    run_ddl_with_check(
        constraint_name=pk_dim_produtos_name,
        table_fqn=dim_produtos,
        table_name_only=dim_produtos_name,
        sql=sql_pk_dim_produtos,
    )

# dim_calendario: PK em dt_calendario
pk_dim_calendario_name = "pk_dim_calendario"

if ensure_not_null(dim_calendario, dim_calendario_name, "dt_calendario"):
    sql_pk_dim_calendario = f"""
    ALTER TABLE {dim_calendario}
    ADD CONSTRAINT {pk_dim_calendario_name}
    PRIMARY KEY (dt_calendario) NOT ENFORCED
    """
    run_ddl_with_check(
        constraint_name=pk_dim_calendario_name,
        table_fqn=dim_calendario,
        table_name_only=dim_calendario_name,
        sql=sql_pk_dim_calendario,
    )

# COMMAND ----------

# 2) FOREIGN KEYS nas fatos

# ft_vendas_diaria → dim_calendario (dt_venda → dt_calendario)
fk_ft_vendas_dim_calendario_name = "fk_ft_vendas_dim_calendario"
sql_fk_ft_vendas_dim_calendario = f"""
ALTER TABLE {ft_vendas}
ADD CONSTRAINT {fk_ft_vendas_dim_calendario_name}
FOREIGN KEY (dt_venda)
REFERENCES {dim_calendario} (dt_calendario)
NOT ENFORCED
"""
run_ddl_with_check(
    constraint_name=fk_ft_vendas_dim_calendario_name,
    table_fqn=ft_vendas,
    table_name_only=ft_vendas_name,
    sql=sql_fk_ft_vendas_dim_calendario,
)

# ft_vendas_diaria → dim_lojas (id_loja)
fk_ft_vendas_dim_lojas_name = "fk_ft_vendas_dim_lojas"
sql_fk_ft_vendas_dim_lojas = f"""
ALTER TABLE {ft_vendas}
ADD CONSTRAINT {fk_ft_vendas_dim_lojas_name}
FOREIGN KEY (id_loja)
REFERENCES {dim_lojas} (id_loja)
NOT ENFORCED
"""
run_ddl_with_check(
    constraint_name=fk_ft_vendas_dim_lojas_name,
    table_fqn=ft_vendas,
    table_name_only=ft_vendas_name,
    sql=sql_fk_ft_vendas_dim_lojas,
)

# ft_vendas_diaria → dim_produtos (id_produto)
fk_ft_vendas_dim_produtos_name = "fk_ft_vendas_dim_produtos"
sql_fk_ft_vendas_dim_produtos = f"""
ALTER TABLE {ft_vendas}
ADD CONSTRAINT {fk_ft_vendas_dim_produtos_name}
FOREIGN KEY (id_produto)
REFERENCES {dim_produtos} (id_produto)
NOT ENFORCED
"""
run_ddl_with_check(
    constraint_name=fk_ft_vendas_dim_produtos_name,
    table_fqn=ft_vendas,
    table_name_only=ft_vendas_name,
    sql=sql_fk_ft_vendas_dim_produtos,
)

# ft_estoque_diaria → dim_calendario (dt_estoque → dt_calendario)
fk_ft_estoque_dim_calendario_name = "fk_ft_estoque_dim_calendario"
sql_fk_ft_estoque_dim_calendario = f"""
ALTER TABLE {ft_estoque}
ADD CONSTRAINT {fk_ft_estoque_dim_calendario_name}
FOREIGN KEY (dt_estoque)
REFERENCES {dim_calendario} (dt_calendario)
NOT ENFORCED
"""
run_ddl_with_check(
    constraint_name=fk_ft_estoque_dim_calendario_name,
    table_fqn=ft_estoque,
    table_name_only=ft_estoque_name,
    sql=sql_fk_ft_estoque_dim_calendario,
)

# ft_estoque_diaria → dim_lojas (id_loja)
fk_ft_estoque_dim_lojas_name = "fk_ft_estoque_dim_lojas"
sql_fk_ft_estoque_dim_lojas = f"""
ALTER TABLE {ft_estoque}
ADD CONSTRAINT {fk_ft_estoque_dim_lojas_name}
FOREIGN KEY (id_loja)
REFERENCES {dim_lojas} (id_loja)
NOT ENFORCED
"""
run_ddl_with_check(
    constraint_name=fk_ft_estoque_dim_lojas_name,
    table_fqn=ft_estoque,
    table_name_only=ft_estoque_name,
    sql=sql_fk_ft_estoque_dim_lojas,
)

# ft_estoque_diaria → dim_produtos (id_produto)
fk_ft_estoque_dim_produtos_name = "fk_ft_estoque_dim_produtos"
sql_fk_ft_estoque_dim_produtos = f"""
ALTER TABLE {ft_estoque}
ADD CONSTRAINT {fk_ft_estoque_dim_produtos_name}
FOREIGN KEY (id_produto)
REFERENCES {dim_produtos} (id_produto)
NOT ENFORCED
"""
run_ddl_with_check(
    constraint_name=fk_ft_estoque_dim_produtos_name,
    table_fqn=ft_estoque,
    table_name_only=ft_estoque_name,
    sql=sql_fk_ft_estoque_dim_produtos,
)

logger.info("Aplicação de PKs e FKs na camada Gold concluída.")
