# Databricks notebook source
# MAGIC %sql
# MAGIC -- Catalog: Bronze / Silver / Gold
# MAGIC CREATE CATALOG IF NOT EXISTS cat_unisales_bronze;
# MAGIC CREATE CATALOG IF NOT EXISTS cat_unisales_silver;
# MAGIC CREATE CATALOG IF NOT EXISTS cat_unisales_gold;
# MAGIC
# MAGIC -- Schemas: Bronze / Silver / Gold
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_unisales_bronze.db_comercio;
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_unisales_silver.db_comercio;
# MAGIC CREATE SCHEMA IF NOT EXISTS cat_unisales_gold.db_comercio;
