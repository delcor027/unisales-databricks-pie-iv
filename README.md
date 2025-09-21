# Projeto PIE - Analytics para PMEs com Databricks Free

Este repositório contém o projeto de extensão acadêmico (PIE)
desenvolvido para análise de vendas e estoque de pequenos negócios
utilizando **Databricks Free**.\
O objetivo é demonstrar a criação de um **pipeline de dados completo
(Bronze → Silver → Gold)** com geração de **dados sintéticos** e
disponibilização de indicadores no **Databricks AI/BI (Lakeview e
Genie)**.

## Estrutura do Projeto

-   **Bronze**: ingestão de dados fictícios (produtos, lojas, estoque, vendas).
-   **Silver**: padronização, deduplicação e checagem de qualidade.
-   **Gold**: agregações analíticas (fato vendas diárias, ticket médio, curva ABC).
-   **AI/BI**: dashboards e consultas em linguagem natural via Genie.

## Tecnologias Utilizadas

-   Databricks Free (Unity Catalog, SQL Warehouses)
-   Delta Lake
-   Databricks AI/BI (Lakeview, Genie)

## Objetivo Acadêmico

Este projeto foi construído como parte do **Projeto Integrador de
Extensão (PIE)**, com foco em **ODS 8 (Trabalho decente e crescimento
econômico)** e **ODS 9 (Indústria, inovação e infraestrutura)**.
