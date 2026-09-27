# MVP: Pipeline de Dados na Nuvem — Olist E-commerce

Pipeline de dados end-to-end construído no Databricks Free Edition utilizando arquitetura Medalhão (Bronze, Silver, Gold) sobre o dataset público da Olist. Projeto desenvolvido como MVP da disciplina de Engenharia de Dados.

---

## Contexto de Negócio e Perguntas (Etapa 2 e 4.1)

### Problema

O objetivo deste trabalho é entender o comportamento de compra dos clientes brasileiros no marketplace Olist, identificando padrões de volume, valor, forma de pagamento e distribuição geográfica ao longo do tempo.

### Perguntas de Negócio

1. Quais categorias de produto concentram o maior volume de pedidos e receita?
2. Qual o ticket médio por categoria e como ele varia entre regiões do Brasil?
3. Quais formas de pagamento são mais utilizadas e qual o parcelamento médio?
4. Como o volume de pedidos evoluiu ao longo dos meses de 2017 e 2018?
5. Quais estados têm maior concentração de clientes compradores?
6. Existe relação entre a avaliação do pedido e o valor gasto pelo cliente?

### Sobre o Dataset

O dataset é disponibilizado publicamente pela Olist no Kaggle ([Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)) e cobre aproximadamente 100 mil pedidos realizados entre 2016 e 2018 em diversas plataformas de marketplace brasileiras.

**Licença:** CC BY-NC-SA 4.0 — permite uso acadêmico e não comercial com atribuição.

### Estrutura dos Dados Brutos

O dataset é composto por 9 arquivos CSV:

| Arquivo | Descrição | Registros |
|---|---|---|
| olist_orders_dataset.csv | Pedidos com status e datas | 99.441 |
| olist_order_items_dataset.csv | Itens de cada pedido com preço e frete | 112.650 |
| olist_order_payments_dataset.csv | Forma e valor de pagamento por pedido | 103.886 |
| olist_order_reviews_dataset.csv | Nota e comentário do cliente | 104.162 |
| olist_products_dataset.csv | Dados do produto (categoria, dimensões, peso) | 32.951 |
| olist_customers_dataset.csv | Localização do cliente | 99.441 |
| olist_sellers_dataset.csv | Localização do vendedor | 3.095 |
| olist_geolocation_dataset.csv | Coordenadas geográficas por CEP | 1.000.163 |
| product_category_name_translation.csv | Tradução das categorias PT/EN | 71 |

---

## Carga dos Dados (Etapa 4.2)

Os arquivos CSV foram baixados diretamente do Kaggle, descompactados localmente e carregados via interface gráfica do Databricks para um Volume no Unity Catalog, no seguinte caminho:

```
/Volumes/workspace/default/olist_raw/
```

O Volume foi criado com as seguintes configurações:
- **Catalog:** workspace
- **Schema:** default
- **Volume name:** olist_raw
- **Volume type:** Managed

Após o upload, a leitura e persistência dos arquivos como tabelas Delta na camada Bronze foi realizada via notebook. O script de ingestão está disponível em:
[`01_bronze_ingestion.ipynb`](https://github.com/Diogo-Pinto/mvp-pipeline-olist/blob/main/01_bronze_ingestion.ipynb)

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

### Arquitetura Medalhão

O pipeline segue a arquitetura Medalhão com três camadas:

- **Bronze:** dados brutos ingeridos diretamente dos CSVs, sem transformação, com metadados de controle (`_source_file`, `_ingestion_date`)
- **Silver:** dados limpos, tipados corretamente, sem duplicatas e padronizados
- **Gold:** tabela fato central (`gold_fato_pedidos`) com todos os dados integrados e prontos para análise

### Modelo Gold: Esquema Estrela Simplificado

A camada Gold foi modelada em uma tabela fato central que consolida as dimensões relevantes para as análises de padrão de compra:

```
gold_fato_pedidos
    order_id
    order_item_id
    customer_id
    product_id
    seller_id
    order_purchase_timestamp
    ano_compra
    mes_compra
    order_status
    categoria_pt
    categoria_en
    preco_item
    frete
    valor_total_item
    forma_pagamento
    parcelas
    valor_total_pedido
    estado_cliente
    cidade_cliente
    estado_vendedor
```

### Catálogo de Dados — gold_fato_pedidos

Tabela fato central do projeto. Granularidade: um registro por item de pedido. Originada do join entre silver_order_items, silver_orders, silver_customers, silver_products, silver_category_translation, silver_sellers e silver_order_payments.

| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| order_id | string | Identificador único do pedido | Hash alfanumérico |
| order_item_id | integer | Sequência do item dentro do pedido | 1 a N |
| customer_id | string | Identificador único do cliente | Hash alfanumérico |
| product_id | string | Identificador único do produto | Hash alfanumérico |
| seller_id | string | Identificador único do vendedor | Hash alfanumérico |
| order_purchase_timestamp | timestamp | Data e hora da compra | 2016-09-04 a 2018-09-03 |
| ano_compra | integer | Ano extraído do timestamp de compra | 2016, 2017, 2018 |
| mes_compra | integer | Mês extraído do timestamp de compra | 1 a 12 |
| order_status | string | Status do pedido | delivered, shipped, canceled, unavailable, invoiced, processing, created, approved |
| categoria_pt | string | Categoria do produto em português | 73 categorias distintas |
| categoria_en | string | Categoria do produto em inglês | 71 categorias distintas |
| preco_item | double | Preço unitário do item | R$ 0,85 a R$ 6.735,00 |
| frete | double | Valor do frete do item | R$ 0,00 a R$ 409,68 |
| valor_total_item | double | Soma de preço e frete do item | Calculado: preco_item + frete |
| forma_pagamento | string | Forma de pagamento principal do pedido | credit_card, boleto, voucher, debit_card, not_defined |
| parcelas | integer | Número de parcelas do pagamento | 1 a 24 |
| valor_total_pedido | double | Valor total pago no pedido (todos os pagamentos) | Agregado da silver_order_payments |
| estado_cliente | string | UF do cliente comprador | 27 estados brasileiros |
| cidade_cliente | string | Cidade do cliente comprador | Texto livre |
| estado_vendedor | string | UF do vendedor | 27 estados brasileiros |

---

## Pipeline de Dados (Etapa 4.4)

O pipeline foi organizado em 5 notebooks independentes, cada um responsável por uma etapa do fluxo:

| Notebook | Etapa | Descrição |
|---|---|---|
| [01_bronze_ingestion](https://github.com/Diogo-Pinto/mvp-pipeline-olist/blob/main/01_bronze_ingestion.ipynb) | Bronze | Leitura dos CSVs e persistência como tabelas Delta com metadados de controle |
| [02_silver_transformation](https://github.com/Diogo-Pinto/mvp-pipeline-olist/blob/main/02_silver_transformation.ipynb) | Silver | Limpeza, tipagem, padronização e remoção de duplicatas |
| [03_gold_modeling](https://github.com/Diogo-Pinto/mvp-pipeline-olist/blob/main/03_gold_modeling.ipynb) | Gold | Join das tabelas Silver e construção da tabela fato central |
| [04_qualidade_dados](https://github.com/Diogo-Pinto/mvp-pipeline-olist/blob/main/04_qualidade_dados.ipynb) | Qualidade | Análise de completude, unicidade, acurácia e consistência |
| [05_analise_final](https://github.com/Diogo-Pinto/mvp-pipeline-olist/blob/main/05_analise_final.ipynb) | Análise | Respostas às 6 perguntas de negócio com discussão dos resultados |

O fluxo completo do pipeline segue a sequência:

```
CSVs no Volume
    → 01_bronze_ingestion → 9 tabelas bronze_*
    → 02_silver_transformation → 7 tabelas silver_*
    → 03_gold_modeling → gold_fato_pedidos
    → 04_qualidade_dados → análise de qualidade
    → 05_analise_final → respostas às perguntas
```

---

## Qualidade de Dados (Etapa 4.5)

### Problemas Detectados e Tratamentos

**1. Completude**

| Tabela | Campo | Nulos | % | Tratamento |
|---|---|---|---|---|
| bronze_orders | order_approved_at | 160 | 0,16% | Mantido. Pedidos cancelados ou em processamento naturalmente não têm aprovação. |
| bronze_orders | order_delivered_carrier_date | 1.783 | 1,79% | Mantido. Pedidos em trânsito ou cancelados não têm data de coleta. |
| bronze_orders | order_delivered_customer_date | 2.965 | 2,98% | Mantido. Mesma justificativa acima. |
| bronze_products | product_category_name | 610 | 1,85% | Removido na Silver. Produtos sem categoria não contribuem para análises por categoria. |
| bronze_products | Campos descritivos (nome, descrição, fotos) | 610 | 1,85% | Mantidos. Não utilizados na Gold. |
| bronze_products | Dimensões físicas (peso, altura, largura, comprimento) | 2 | 0,01% | Mantidos. Não utilizados na Gold. |

**2. Unicidade**

| Tabela | Chave analisada | Duplicatas | Avaliação |
|---|---|---|---|
| bronze_order_items | order_id | 13.984 | Comportamento esperado. A chave única real é order_id + order_item_id. Um pedido pode ter múltiplos itens. |
| bronze_order_payments | order_id | 4.446 | Comportamento esperado. Um pedido pode ter múltiplas formas de pagamento. Tratado por agregação na Gold. |
| bronze_orders | order_id | 0 | Sem duplicatas. |
| bronze_customers | customer_id | 0 | Sem duplicatas. |
| bronze_products | product_id | 0 | Sem duplicatas. |
| bronze_sellers | seller_id | 0 | Sem duplicatas. |

**3. Acurácia**

- Sem preços negativos ou zerados em order_items.
- Frete mínimo de R$ 0,00: considerado válido (retirada em loja ou promoção).
- Preço máximo de R$ 6.735,00: plausível para eletrônicos ou itens premium.
- 3 registros com payment_type = "not_defined" (0,003% do total): mantidos sem tratamento especial.
- Sem valores de pagamento negativos.
- Campo review_score continha valores malformados (datas no lugar de inteiros): tratado com `try_cast` na análise final, convertendo valores inválidos para null antes do agrupamento.

**4. Consistência**

- Estados de clientes e vendedores padronizados para maiúsculo na Silver (evita inconsistências como "sp" vs "SP").
- Colunas de data convertidas de string para Timestamp na Silver para permitir operações temporais corretas.

---

## Análise de Dados (Etapa 4.5)

### Pergunta 1: Quais categorias concentram maior volume de pedidos e receita?

| Categoria | Volume de Itens | Receita Total | Ticket Médio |
|---|---|---|---|
| bed_bath_table | 10.953 | R$ 1.023.434,76 | R$ 93,44 |
| health_beauty | 9.465 | R$ 1.233.131,72 | R$ 130,28 |
| sports_leisure | 8.431 | R$ 954.852,55 | R$ 113,25 |
| furniture_decor | 8.160 | R$ 711.927,69 | R$ 87,25 |
| computers_accessories | 7.644 | R$ 888.724,61 | R$ 116,26 |
| housewares | 6.795 | R$ 615.628,69 | R$ 90,60 |
| watches_gifts | 5.859 | R$ 1.166.176,98 | R$ 199,04 |
| telephony | 4.430 | R$ 309.860,23 | R$ 69,95 |
| garden_tools | 4.268 | R$ 470.495,28 | R$ 110,24 |
| auto | 4.140 | R$ 578.966,65 | R$ 139,85 |

Cama, mesa e banho lidera em volume, mas saúde e beleza gera mais receita com ticket médio de R$ 130. Relógios e presentes é o caso mais estratégico: apenas 7ª em volume mas 2ª em receita com o maior ticket médio entre as top 10 (R$ 199), indicando que categorias de menor volume podem ser mais relevantes do ponto de vista de receita por item.

### Pergunta 2: Como o ticket médio varia entre regiões?

| Região | Ticket Médio | Volume de Itens | Receita Total |
|---|---|---|---|
| Norte | R$ 159,19 | 1.983 | R$ 315.679,56 |
| Nordeste | R$ 148,51 | 9.951 | R$ 1.477.813,10 |
| Centro-Oeste | R$ 130,85 | 6.375 | R$ 834.169,41 |
| Sul | R$ 120,00 | 15.647 | R$ 1.877.578,61 |
| Sudeste | R$ 114,36 | 74.682 | R$ 8.540.290,22 |

Norte e Nordeste apresentam ticket médio mais alto que o Sudeste, apesar de concentrarem volume muito menor. Uma hipótese é que consumidores nessas regiões compram com menos frequência, mas optam por itens de maior valor quando compram. O Sudeste, com menor ticket médio, concentra 68% dos itens e 70% da receita total.

### Pergunta 3: Quais formas de pagamento são mais usadas e qual o parcelamento médio?

| Forma de Pagamento | Volume | Média de Parcelas | Ticket Médio do Pedido |
|---|---|---|---|
| credit_card | 83.253 | 3,6 | R$ 182,67 |
| boleto | 22.362 | 1,0 | R$ 176,33 |
| voucher | 2.927 | 1,3 | R$ 129,47 |
| debit_card | 1.652 | 1,0 | R$ 149,32 |

O cartão de crédito domina com 75% das transações e parcelamento médio de 3,6 vezes, confirmando o comportamento típico do consumidor brasileiro. O boleto aparece em segundo com ticket médio próximo ao cartão (R$ 176 vs R$ 182), mostrando que não é usado apenas para compras de baixo valor. Vouchers têm o menor ticket médio (R$ 129), indicando uso predominante em compras menores ou com desconto.

### Pergunta 4: Como o volume de pedidos evoluiu ao longo dos meses?

O crescimento em 2017 é consistente, saindo de 750 pedidos em janeiro para 7.289 em novembro, pico do ano e provável reflexo da Black Friday. Em 2018 o volume se estabiliza entre 6.000 e 7.000 pedidos mensais, indicando maturidade da operação. O salto entre dezembro/2017 (5.513) e janeiro/2018 (7.069) confirma que o crescimento foi estrutural e não apenas sazonal.

### Pergunta 5: Quais estados têm maior concentração de clientes?

| Estado | Clientes | Pedidos | Receita Total |
|---|---|---|---|
| SP | 40.501 | 40.501 | R$ 5.067.633,16 |
| RJ | 12.350 | 12.350 | R$ 1.759.651,13 |
| MG | 11.354 | 11.354 | R$ 1.552.481,83 |
| RS | 5.345 | 5.345 | R$ 728.897,47 |
| PR | 4.923 | 4.923 | R$ 666.063,51 |
| SC | 3.546 | 3.546 | R$ 507.012,13 |
| BA | 3.256 | 3.256 | R$ 493.584,14 |
| DF | 2.080 | 2.080 | R$ 296.498,41 |
| ES | 1.995 | 1.995 | R$ 268.643,45 |
| GO | 1.957 | 1.957 | R$ 282.836,70 |

São Paulo concentra 40.501 clientes, mais do que RJ e MG somados. Os três estados do Sudeste representam cerca de 58% da base de clientes, refletindo a concentração econômica brasileira. Combinado com o resultado da Pergunta 2, o padrão que emerge é: o Sudeste compra mais vezes e em maior volume, enquanto regiões periféricas compram menos porém com ticket médio mais alto.

### Pergunta 6: Existe relação entre avaliação e valor gasto?

| Nota | Pedidos | Valor Médio | Valor Mínimo | Valor Máximo |
|---|---|---|---|---|
| 1 | 9.406 | R$ 164,90 | R$ 3,54 | R$ 13.440,00 |
| 2 | 2.941 | R$ 143,66 | R$ 5,31 | R$ 3.999,00 |
| 3 | 7.961 | R$ 127,48 | R$ 3,50 | R$ 2.919,40 |
| 4 | 18.987 | R$ 132,11 | R$ 0,85 | R$ 4.690,00 |
| 5 | 57.066 | R$ 134,43 | R$ 0,85 | R$ 6.735,00 |

Pedidos com nota 1 têm o maior valor médio (R$ 164,90), enquanto notas intermediárias ficam entre R$ 127 e R$ 143. Isso sugere uma relação inversa entre satisfação e valor gasto: compras de maior valor tendem a gerar mais insatisfação, possivelmente por expectativas mais altas em relação ao produto ou por problemas logísticos em itens de maior porte. Pedidos com nota 5 representam a maioria absoluta (57.066), indicando que a experiência geral é positiva independente do valor.

### Conclusão Geral

O pipeline revelou um marketplace em forte crescimento no período analisado, dominado pelo Sudeste em volume mas com regiões periféricas apresentando maior valor por compra. O cartão de crédito parcelado é o meio de pagamento predominante e categorias como relógios/presentes e saúde/beleza se destacam em receita mesmo sem liderar em volume. A relação inversa entre valor gasto e satisfação aponta para uma oportunidade de melhoria na experiência pós-venda em compras de maior ticket.

---

## Autoavaliação

### Objetivos atingidos

Todas as 6 perguntas de negócio definidas no início do trabalho foram respondidas com dados concretos. O pipeline completo foi implementado seguindo a arquitetura Medalhão (Bronze, Silver, Gold), com cada camada cumprindo sua responsabilidade: preservação dos dados brutos, limpeza e padronização, e modelagem para análise.

### Dificuldades encontradas

A principal dificuldade foi a configuração inicial do ambiente Databricks Free Edition, especialmente a criação de volumes no Unity Catalog e a conexão com o repositório GitHub via Databricks Repos. A curva de aprendizado com PySpark também exigiu adaptação, já que a sintaxe de transformações difere do Pandas, mais comum para quem está iniciando.

Outro ponto foi o campo `review_score` da tabela de avaliações, que continha valores malformados (datas no lugar de inteiros), o que gerou erro no cast e exigiu o uso de `try_cast` para tratamento adequado.

### Trabalhos futuros

- Incorporar a tabela de geolocalização para análises espaciais mais detalhadas (coordenadas por CEP)
- Incluir a análise de reviews (texto dos comentários) com processamento de linguagem natural para entender os motivos das avaliações negativas
- Automatizar o pipeline com Databricks Workflows para execução periódica
- Construir um dashboard interativo no Databricks SQL conectado à tabela Gold
- Expandir a modelagem com tabelas dimensão separadas (dim_produto, dim_cliente, dim_tempo) para um esquema estrela completo
