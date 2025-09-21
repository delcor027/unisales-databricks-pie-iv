-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gerador de Dados Sintéticos — Bronze (Databricks Free)
-- MAGIC
-- MAGIC **Objetivo:** popular as tabelas *Bronze* do catálogo `cat_unisales_bronze` no schema `db_comercio` com dados sintéticos realistas (produtos, lojas, estoque e vendas) para o PIE.
-- MAGIC
-- MAGIC **Como usar:**
-- MAGIC 1) Selecione seu **SQL Warehouse**.
-- MAGIC 2) Execute as células **na ordem**: _Setup → Reset (opcional) → Produtos/Lojas → Estoque → Vendas → Conferência_.
-- MAGIC 3) Este notebook **não usa variáveis ${...}** nem parâmetros `:param` para evitar o erro de parâmetros do DBSQL. Em vez disso, usa um **CTE de parâmetros**.

-- COMMAND ----------

-- Produtos com popularidade tipo Zipf (~1/rank^1.1)
CREATE OR REPLACE TABLE cat_unisales_bronze.db_comercio.tb_produtos AS
WITH base AS (
  SELECT
    p AS rank_prod,
    CONCAT('P', lpad(CAST(p AS STRING), 4, '0')) AS id_produto,
    CONCAT('SKU-', lpad(CAST(p AS STRING), 4, '0')) AS sku,
    CONCAT('Produto ', p) AS nome,
    element_at(map(0,'Acessórios',1,'Bebidas',2,'Cozinha',3,'Limpeza',4,'Higiene',5,'Snacks'), (p % 6)) AS categoria,
    element_at(map(0,'MarcaA',1,'MarcaB',2,'MarcaC',3,'MarcaD'), (p % 4)) AS marca,
    CAST( 5 + (p % 20) + (rand(123)*10) AS DECIMAL(18,2)) AS preco_lista
  FROM (SELECT sequence(1, 120) AS s)
  LATERAL VIEW explode(s) e AS p
),
zipf AS (
  SELECT *,
         POW(1.0 / rank_prod, 1.1) / SUM(POW(1.0 / rank_prod, 1.1)) OVER () AS weight_zipf
  FROM base
)
SELECT * FROM zipf;

-- Lojas
CREATE OR REPLACE TABLE cat_unisales_bronze.db_comercio.tb_lojas AS
SELECT
  CAST(seq AS STRING)  AS id_loja,
  CONCAT('Loja ', seq) AS nome,
  element_at(map(1,'Vitoria',2,'Serra',3,'Vila Velha',4,'Cariacica',5,'Guarapari',6,'Linhares'), ((seq-1)%6)+1) AS cidade,
  'ES' AS uf
FROM (SELECT sequence(1, 6) AS s)
LATERAL VIEW explode(s) e AS seq;


-- COMMAND ----------

-- Cria/repõe a tabela a partir de um SELECT com CTE
CREATE OR REPLACE TABLE cat_unisales_bronze.db_comercio.tb_estoque AS
WITH cal AS (
  SELECT explode(sequence(to_date('2025-01-01'), to_date(current_timestamp()), INTERVAL 1 DAY)) AS dt
)
SELECT
  c.dt,
  p.id_produto,
  l.id_loja,
  -- pmod para evitar resultado negativo do hash % 20
  CAST(100 + pmod(hash(p.id_produto, l.id_loja, c.dt), 20) AS INT) AS qtd_estoque
FROM cal c
CROSS JOIN cat_unisales_bronze.db_comercio.tb_produtos p
CROSS JOIN cat_unisales_bronze.db_comercio.tb_lojas l
WHERE p.rank_prod <= 40;


-- COMMAND ----------

-- Tabela final BRONZE: vendas (CTAS com CTEs)
CREATE OR REPLACE TABLE cat_unisales_bronze.db_comercio.tb_vendas AS
WITH d AS (
  SELECT sequence(to_date('2025-01-01'), to_date(current_timestamp()), INTERVAL 1 DAY) AS ds
),
cal AS (
  SELECT
    dt,
    1.0 + 0.15 * sin(2*pi() * (dayofweek(dt)-1)/7) AS fator_sazonal
  FROM (SELECT explode(ds) AS dt FROM d)
),

-- Grade por dia/loja com volume de transações
grade AS (
  SELECT
    c.dt,
    l.id_loja,
    CAST(greatest(1, 220 / 6 * c.fator_sazonal * (0.8 + rand(123)*0.4)) AS INT) AS tx_dia_loja
  FROM cal c
  CROSS JOIN cat_unisales_bronze.db_comercio.tb_lojas l
),

-- Transações (id de pedido local)
tx AS (
  SELECT
    g.dt,
    g.id_loja,
    posexplode(sequence(1, g.tx_dia_loja)) AS (idx, id_pedido_local)
  FROM grade g
),

-- Nº de itens por transação (1..6), viés p/ 1–3
tx_itens AS (
  SELECT
    t.dt,
    t.id_loja,
    t.id_pedido_local,
    CAST(ceil(least(6, 1 + rand(123)*3 + rand(123)*2)) AS INT) AS n_itens
  FROM tx t
),

-- Explode itens + número pseudo-aleatório determinístico por item
tx_itens_exp AS (
  SELECT
    ti.dt,
    ti.id_loja,
    ti.id_pedido_local,
    posexplode(sequence(1, ti.n_itens)) AS (pos, nr_item),
    -- pseudo-U(0,1) determinístico por (dt, loja, pedido, pos)
    pmod(hash(ti.dt, ti.id_loja, ti.id_pedido_local, pos), 1000000) / 1000000.0 AS u_hash
  FROM tx_itens ti
),

-- Amostra ponderada por Zipf (determinística) de produtos
itens AS (
  SELECT
    e.dt,
    e.id_loja,
    e.id_pedido_local,
    e.nr_item,
    p.id_produto,
    p.preco_lista,
    p.weight_zipf
  FROM tx_itens_exp e
  JOIN cat_unisales_bronze.db_comercio.tb_produtos p
    ON e.u_hash < p.weight_zipf * 3          -- <<< sem rand() no JOIN
),

-- CTE que PRECISA projetar dt
itens_precificados AS (
  SELECT
    i.dt,
    i.id_loja,
    i.id_pedido_local,
    i.id_produto,
    CAST(
      1
      + IF(rand(123) < 0.35, 1, 0)
      + IF(rand(123) < 0.10, 1, 0)
      AS INT
    ) AS qtd,
    i.preco_lista,
    CASE
      WHEN rand(123) < 0.12
        THEN CAST((0.05 + rand(123)*0.15) AS DECIMAL(5,4))
      ELSE CAST(0.00 AS DECIMAL(5,4))
    END AS perc_desc
  FROM itens i
)

-- SELECT FINAL: agora com FROM correto
SELECT
  dt AS dt_venda,
  CONCAT(
    CAST(year(dt) AS STRING),
    lpad(CAST(month(dt) AS STRING), 2, '0'),
    lpad(CAST(day(dt)   AS STRING), 2, '0'),
    '-', id_loja, '-',
    lpad(CAST(id_pedido_local AS STRING), 6, '0')
  ) AS id_pedido,
  id_loja,
  id_produto,
  qtd,
  CAST(preco_lista * qtd * (1 - perc_desc) AS DECIMAL(18,2)) AS receita,
  CAST(preco_lista * qtd *  perc_desc     AS DECIMAL(18,2)) AS desconto,
  current_timestamp()                                        AS ts_bronze
FROM itens_precificados;



-- COMMAND ----------

-- Conferência rápida: contagens e amostras
SELECT 'tb_produtos' AS tabela, COUNT(*) AS linhas FROM cat_unisales_bronze.db_comercio.tb_produtos
UNION ALL SELECT 'tb_lojas'   , COUNT(*) FROM cat_unisales_bronze.db_comercio.tb_lojas
UNION ALL SELECT 'tb_estoque' , COUNT(*) FROM cat_unisales_bronze.db_comercio.tb_estoque
UNION ALL SELECT 'tb_vendas'  , COUNT(*) FROM cat_unisales_bronze.db_comercio.tb_vendas;
