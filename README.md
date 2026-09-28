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

**Licença:** CC BY-NC-SA 4.0 (Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International). Permite uso, adaptação e compartilhamento para fins não comerciais, mediante atribuição da fonte e manutenção da mesma licença em trabalhos derivados. O uso acadêmico neste MVP está em conformidade com os termos.

### Estrutura dos Dados Brutos

O dataset é composto por 9 arquivos CSV:

| Arquivo | Descrição | Registros |
|---|---|---|
| olist_orders_dataset.csv | Pedidos com status e datas | 99.441 |
| olist_order_items_dataset.csv | Itens de cada pedido com preço e frete | 112.650 |
| olist_order_payments_dataset.csv | Forma e valor de pagamento por pedido | 103.886 |
| olist_order_reviews_dataset.csv | Nota e comentário do cliente | 99.224 |
| olist_products_dataset.csv | Dados do produto (categoria, dimensões, peso) | 32.951 |
| olist_customers_dataset.csv | Localização do cliente | 99.441 |
| olist_sellers_dataset.csv | Localização do vendedor | 3.095 |
| olist_geolocation_dataset.csv | Coordenadas geográficas por CEP | 1.000.163 |
| product_category_name_translation.csv | Tradução das categorias PT/EN | 71 |

### Delimitação de Escopo

Duas fontes foram ingeridas na camada Bronze para preservar a integridade do dataset original, mas não seguiram para as camadas Silver e Gold:

- **olist_geolocation_dataset.csv:** contém coordenadas de latitude e longitude por CEP. Como nenhuma das perguntas de negócio exige análise espacial em nível de coordenada, a granularidade de estado disponível em `customers` e `sellers` foi suficiente. Mantida na Bronze para eventual uso futuro.
- **Campos de texto livre de reviews:** os comentários escritos pelos clientes foram preservados, mas não analisados. A Pergunta 6 utiliza apenas a nota numérica. Análise textual está listada como trabalho futuro.

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

Após o upload, a leitura e persistência dos arquivos como tabelas Delta na camada Bronze foi realizada via notebook. O script de ingestão está disponível em [`01_bronze_ingestion`](https://github.com/Diogo-Pinto/mvp-pipeline-olist).

**Observação técnica relevante:** a leitura inicial dos CSVs com as opções padrão do Spark gerou desalinhamento de colunas no arquivo de reviews, pois os comentários dos clientes contêm quebras de linha e vírgulas dentro do campo de texto. A ingestão foi corrigida com as opções `multiLine`, `quote` e `escape`, o que reduziu o total de registros de 104.162 para 99.224. A diferença correspondia a registros fantasma criados pela quebra indevida de comentários multilinha. O detalhamento está na seção de Qualidade de Dados.

<img width="900" height="578" alt="image" src="https://github.com/user-attachments/assets/7229b0a1-e2ae-472e-ab95-4d6ded2ce7dc" />

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

### Arquitetura Medalhão

O pipeline segue a arquitetura Medalhão com três camadas:

- **Bronze:** dados brutos ingeridos diretamente dos CSVs, sem transformação de conteúdo, com metadados de controle (`_source_file`, `_ingestion_date`)
- **Silver:** dados limpos, tipados corretamente, sem duplicatas e padronizados
- **Gold:** tabela fato central (`gold_fato_pedidos`) com todos os dados integrados e prontos para análise

### Modelo Gold

A camada Gold foi modelada em uma tabela fato central que consolida as dimensões relevantes para as análises de padrão de compra. Optou-se por um modelo desnormalizado (flat) em vez de esquema estrela completo por dois motivos: o volume de dados não justifica a normalização do ponto de vista de armazenamento, e a consulta única sem joins favorece a performance analítica, que é o padrão recomendado em ambientes Lakehouse.

```
gold_fato_pedidos (112.650 registros, 20 colunas)
    ├── Identificadores: order_id, order_item_id, customer_id, product_id, seller_id
    ├── Temporal: order_purchase_timestamp, ano_compra, mes_compra
    ├── Status: order_status
    ├── Produto: categoria_pt, categoria_en
    ├── Financeiro: preco_item, frete, valor_total_item, valor_total_pedido
    ├── Pagamento: forma_pagamento, parcelas
    └── Geografia: estado_cliente, cidade_cliente, estado_vendedor
```

**Granularidade e implicação analítica:** a tabela tem granularidade de item de pedido. Campos que descrevem o pedido como um todo (`forma_pagamento`, `parcelas`, `valor_total_pedido`) se repetem em cada item do mesmo pedido. Qualquer agregação sobre esses campos exige deduplicação prévia por `order_id`, caso contrário pedidos com muitos itens seriam contados múltiplas vezes. Essa precaução foi aplicada na análise da Pergunta 3.

### Catálogo de Dados no Unity Catalog

Todas as 18 tabelas do pipeline e as 20 colunas da tabela Gold foram documentadas diretamente no Unity Catalog via comandos `COMMENT ON TABLE` e `ALTER COLUMN ... COMMENT`, tornando o catálogo do Databricks a fonte oficial de documentação. O script está em [`06_catalogo_dados`](https://github.com/Diogo-Pinto/mvp-pipeline-olist).

A linhagem dos dados é gerada automaticamente pelo Unity Catalog e pode ser visualizada na aba Lineage de cada tabela, mostrando graficamente a origem e as transformações aplicadas.

### Catálogo transcrito — gold_fato_pedidos

Tabela fato central do projeto. Granularidade: um registro por item de pedido.

| Campo | Tipo | Descrição e linhagem | Domínio |
|---|---|---|---|
| order_id | string | Identificador único do pedido. Origem: silver_orders.order_id | Hash alfanumérico de 32 caracteres |
| order_item_id | integer | Sequência do item dentro do pedido. Origem: silver_order_items.order_item_id | 1 a N |
| customer_id | string | Identificador do cliente no pedido. Origem: silver_orders.customer_id | Hash alfanumérico |
| product_id | string | Identificador único do produto. Origem: silver_order_items.product_id | Hash alfanumérico |
| seller_id | string | Identificador único do vendedor. Origem: silver_order_items.seller_id | Hash alfanumérico |
| order_purchase_timestamp | timestamp | Data e hora da compra. Origem: silver_orders, convertido de string para timestamp na Silver | 2016-09-04 a 2018-09-03 |
| ano_compra | integer | Derivado: extraído de order_purchase_timestamp via year() | 2016, 2017, 2018 |
| mes_compra | integer | Derivado: extraído de order_purchase_timestamp via month() | 1 a 12 |
| order_status | string | Situação do pedido. Origem: silver_orders.order_status | delivered, shipped, canceled, unavailable, invoiced, processing, created, approved |
| categoria_pt | string | Categoria em português. Origem: silver_products, join por product_id | 73 categorias distintas |
| categoria_en | string | Categoria em inglês. Origem: silver_category_translation, join por categoria_pt | 71 categorias distintas |
| preco_item | double | Preço unitário sem frete. Origem: silver_order_items.price | R$ 0,85 a R$ 6.735,00 |
| frete | double | Valor do frete do item. Origem: silver_order_items.freight_value | R$ 0,00 a R$ 409,68 |
| valor_total_item | double | Derivado: soma de preco_item e frete | Calculado |
| forma_pagamento | string | Forma principal de pagamento do pedido. Origem: silver_order_payments, agregado por order_id | credit_card, boleto, voucher, debit_card, not_defined |
| parcelas | integer | Derivado: média de payment_installments por pedido, arredondada | 1 a 24 |
| valor_total_pedido | double | Derivado: soma de payment_value de todos os pagamentos do pedido | Agregado |
| estado_cliente | string | UF do cliente. Origem: silver_customers.customer_state, padronizado para maiúsculo | 27 UFs |
| cidade_cliente | string | Cidade do cliente. Origem: silver_customers.customer_city | Texto livre |
| estado_vendedor | string | UF do vendedor. Origem: silver_sellers.seller_state, padronizado para maiúsculo | 27 UFs |

### Catálogo — Tabelas Bronze e Silver

| Tabela | Camada | Descrição | Registros |
|---|---|---|---|
| bronze_orders | Bronze | Pedidos brutos ingeridos sem transformação | 99.441 |
| bronze_order_items | Bronze | Itens de pedido brutos | 112.650 |
| bronze_order_payments | Bronze | Pagamentos brutos | 103.886 |
| bronze_order_reviews | Bronze | Avaliações brutas, lidas com multiLine | 99.224 |
| bronze_products | Bronze | Produtos brutos com categoria e atributos físicos | 32.951 |
| bronze_customers | Bronze | Clientes brutos | 99.441 |
| bronze_sellers | Bronze | Vendedores brutos | 3.095 |
| bronze_category_translation | Bronze | Tradução de categorias PT/EN | 71 |
| bronze_geolocation | Bronze | Coordenadas por CEP, fora do escopo analítico | 1.000.163 |
| silver_orders | Silver | Datas convertidas para timestamp, duplicatas removidas | 99.441 |
| silver_order_items | Silver | Preço e frete tipados, valores inválidos removidos | 112.650 |
| silver_order_payments | Silver | Valores tipados, registros negativos removidos | 103.886 |
| silver_order_reviews | Silver | Deduplicada por review_id, domínio da nota validado | 98.410 |
| silver_products | Silver | Produtos sem categoria removidos | 32.341 |
| silver_customers | Silver | Siglas de estado padronizadas | 99.441 |
| silver_sellers | Silver | Siglas de estado padronizadas | 3.095 |
| silver_category_translation | Silver | Traduções sem correspondência removidas | 71 |
| gold_fato_pedidos | Gold | Tabela fato central integrada | 112.650 |

---

## Pipeline de Dados (Etapa 4.4)

O pipeline foi organizado em 6 notebooks independentes, cada um responsável por uma etapa do fluxo. A ramificação por etapa foi escolhida em vez de um notebook único para facilitar a depuração e permitir reexecução isolada de cada camada.

| Notebook | Etapa | Descrição |
|---|---|---|
| 01_bronze_ingestion | Bronze | Leitura dos CSVs e persistência como tabelas Delta com metadados de controle |
| 02_silver_transformation | Silver | Limpeza, tipagem, padronização e remoção de duplicatas |
| 03_gold_modeling | Gold | Join das tabelas Silver e construção da tabela fato central |
| 04_qualidade_dados | Qualidade | Análise de completude, unicidade, acurácia e consistência |
| 05_analise_final | Análise | Respostas às 6 perguntas de negócio com discussão dos resultados |
| 06_catalogo_dados | Catálogo | Aplicação das descrições de tabelas e colunas no Unity Catalog |

Todos os notebooks estão disponíveis no repositório: [github.com/Diogo-Pinto/mvp-pipeline-olist](https://github.com/Diogo-Pinto/mvp-pipeline-olist)

<img width="852" height="890" alt="image" src="https://github.com/user-attachments/assets/68368a7d-7575-46a6-a456-cc8235698db7" />

<img width="1202" height="1190" alt="image" src="https://github.com/user-attachments/assets/37f91f18-e175-4681-a045-0f4dd8eece94" />

### Fluxo do pipeline

```
CSVs no Volume (/Volumes/workspace/default/olist_raw)
    │
    ├─→ 01_bronze_ingestion ──→ 9 tabelas bronze_*
    │
    ├─→ 02_silver_transformation ──→ 8 tabelas silver_*
    │
    ├─→ 03_gold_modeling ──→ gold_fato_pedidos
    │
    ├─→ 04_qualidade_dados ──→ análise sobre a camada Bronze
    │
    ├─→ 05_analise_final ──→ respostas às perguntas (consome Gold e Silver)
    │
    └─→ 06_catalogo_dados ──→ documentação no Unity Catalog
```

### Transformações aplicadas na camada Gold

A construção da tabela fato envolveu seis joins sucessivos e uma agregação prévia:

1. **Agregação de pagamentos:** como um pedido pode ter múltiplas formas de pagamento (por exemplo cartão mais voucher), os registros de `silver_order_payments` foram agregados por `order_id` antes do join, evitando multiplicação indevida de linhas na fato.
2. **silver_order_items + silver_orders** (inner join por order_id): define a granularidade da fato como item de pedido.
3. **+ silver_customers** (left join por customer_id): adiciona a geografia do comprador.
4. **+ silver_products** (left join por product_id): adiciona a categoria do produto.
5. **+ silver_category_translation** (left join por categoria): adiciona a categoria traduzida para inglês.
6. **+ silver_sellers** (left join por seller_id): adiciona a UF do vendedor.
7. **+ pagamentos agregados** (left join por order_id): adiciona forma de pagamento e valor total.

Os joins com dimensões usam `left` para preservar todos os itens de pedido mesmo quando o produto ou vendedor correspondente não existe nas tabelas de apoio.

---

## Qualidade de Dados (Etapa 4.5)

Foram identificados dois problemas reais de qualidade no dataset, além de achados que a inspeção inicial sinalizou como suspeitos mas que se revelaram comportamento legítimo das tabelas.

### Problema 1: leitura incorreta de campos multilinha (crítico)

O achado mais relevante não estava no conteúdo dos dados, mas na forma como eram lidos.

**Sintoma:** erro de conversão ao tipar o campo `review_score`, com fragmentos de texto aparecendo em colunas de data e datas aparecendo na coluna de nota.

**Causa:** o arquivo de reviews contém comentários escritos por clientes, campos de texto livre com quebras de linha e vírgulas. Com as opções padrão de leitura de CSV do Spark, cada quebra de linha dentro de um comentário era interpretada como fim de registro, gerando linhas fantasma com colunas desalinhadas.

**Correção:** reingestão com as opções `multiLine`, `quote` e `escape` configuradas na camada Bronze.

**Impacto:** o total caiu de 104.162 para 99.224 registros. Os 4.938 eliminados eram integralmente linhas inválidas.

**Validação:** após a correção, a verificação de domínio retornou zero valores não conversíveis e zero valores fora da escala de 1 a 5. Os 99.224 registros são integralmente válidos, confirmando que o problema foi eliminado na origem e não apenas mascarado nas camadas seguintes.

Este problema passaria despercebido em uma inspeção superficial de completude ou unicidade, já que as linhas fantasma não eram nulas nem duplicadas. Teria contaminado silenciosamente a análise da Pergunta 6.

### Problema 2: duplicatas em review_id

A tabela `bronze_order_reviews` apresenta 814 duplicatas por `review_id`. Diferente dos casos de granularidade legítima descritos abaixo, este é um problema real: `review_id` deveria ser único por definição, já que representa uma avaliação individual. A presença de duplicatas indica falha de integridade na origem, provavelmente reenvio de formulário ou erro de exportação.

**Tratamento:** deduplicação por `review_id` na camada Silver, resultando em 98.410 registros únicos.

### Completude

| Tabela | Campo | Nulos | % | Tratamento |
|---|---|---|---|---|
| bronze_orders | order_approved_at | 160 | 0,16% | Mantido. Pedidos cancelados ou em processamento não têm aprovação. |
| bronze_orders | order_delivered_carrier_date | 1.783 | 1,79% | Mantido. Pedidos em trânsito ou cancelados não têm data de coleta. |
| bronze_orders | order_delivered_customer_date | 2.965 | 2,98% | Mantido. Mesma justificativa. |
| bronze_products | product_category_name | 610 | 1,85% | Removido na Silver. Produtos sem categoria não contribuem para análises por categoria. |
| bronze_products | Campos descritivos e dimensões | 610 a 2 | 1,85% a 0,01% | Mantidos. Não utilizados na Gold. |
| bronze_order_reviews | review_comment_title | 87.656 | 88,34% | Mantido. Preenchimento opcional na plataforma. |
| bronze_order_reviews | review_comment_message | 58.247 | 58,70% | Mantido. Preenchimento opcional na plataforma. |

As tabelas `bronze_customers`, `bronze_sellers`, `bronze_category_translation`, `bronze_order_items` e `bronze_order_payments` não apresentaram valores nulos.

### Unicidade: distinguindo problema real de granularidade legítima

| Tabela | Chave | Duplicatas | Avaliação |
|---|---|---|---|
| bronze_order_reviews | review_id | 814 | **Problema real.** Tratado por deduplicação na Silver. |
| bronze_order_items | order_id | 13.984 | Falso positivo. A chave real é composta (order_id mais order_item_id), pois um pedido contém vários itens. |
| bronze_order_payments | order_id | 4.446 | Falso positivo. Um pedido pode ter múltiplas formas de pagamento. Tratado por agregação na Gold. |
| bronze_orders | order_id | 0 | Sem duplicatas. |
| bronze_customers | customer_id | 0 | Sem duplicatas. |
| bronze_products | product_id | 0 | Sem duplicatas. |
| bronze_sellers | seller_id | 0 | Sem duplicatas. |

A verificação inicial foi feita sobre o primeiro campo identificador de cada tabela, que é a abordagem automatizável, mas a interpretação exigiu entender a granularidade real de cada uma. Tratar os 13.984 registros de `order_items` como duplicatas e removê-los teria destruído a informação de itens múltiplos por pedido.

### Acurácia

- Nenhum preço negativo ou zerado em `order_items`.
- Frete mínimo de R$ 0,00: válido, representa frete grátis ou promoção.
- Preço máximo de R$ 6.735,00: plausível para eletrônicos ou itens premium.
- 3 registros com `payment_type` igual a "not_defined" (0,003% do total): mantidos na Silver, excluídos apenas na análise da Pergunta 3.
- Nenhum valor de pagamento negativo.
- Campo `review_score` integralmente dentro do domínio válido após a correção de ingestão. Distribuição na Bronze: 11.424 notas 1, 3.151 notas 2, 8.179 notas 3, 19.142 notas 4 e 57.328 notas 5.
- O `try_cast` aplicado na Silver foi mantido como salvaguarda defensiva, garantindo que eventuais valores malformados em cargas futuras não interrompam o pipeline.

### Consistência

- Siglas de estado de clientes e vendedores padronizadas para maiúsculo na Silver, prevenindo inconsistências como "sp" contra "SP".
- Colunas de data convertidas de string para timestamp na Silver, viabilizando operações temporais corretas nas análises.
- Categorias de produto padronizadas via join com a tabela oficial de tradução, garantindo nomenclatura consistente.

---

## Análise de Dados (Etapa 4.5)

Todas as análises consideram apenas pedidos com status `delivered`, totalizando 110.197 itens distribuídos em 96.478 pedidos únicos. Essa decisão garante que as métricas reflitam compras efetivamente concluídas, excluindo pedidos cancelados ou indisponíveis que distorceriam os valores de receita.

### Pergunta 1: Quais categorias concentram maior volume de pedidos e receita?

**Top 10 por volume:**

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

<img width="610" height="457" alt="image" src="https://github.com/user-attachments/assets/8c539eb9-f85f-4119-8784-53a13e1fcc7b" />

**Top 10 por receita:**

| Categoria | Receita Total | Volume de Itens | Ticket Médio |
|---|---|---|---|
| health_beauty | R$ 1.233.131,72 | 9.465 | R$ 130,28 |
| watches_gifts | R$ 1.166.176,98 | 5.859 | R$ 199,04 |
| bed_bath_table | R$ 1.023.434,76 | 10.953 | R$ 93,44 |
| sports_leisure | R$ 954.852,55 | 8.431 | R$ 113,25 |
| computers_accessories | R$ 888.724,61 | 7.644 | R$ 116,26 |
| furniture_decor | R$ 711.927,69 | 8.160 | R$ 87,25 |
| housewares | R$ 615.628,69 | 6.795 | R$ 90,60 |
| cool_stuff | R$ 610.204,10 | 3.718 | R$ 164,12 |
| auto | R$ 578.966,65 | 4.140 | R$ 139,85 |
| toys | R$ 471.286,48 | 4.030 | R$ 116,94 |

<img width="610" height="360" alt="image" src="https://github.com/user-attachments/assets/671eaed4-4da1-4a80-a607-f5c1fffc19c8" />

**Discussão:** os dois rankings divergem de forma reveladora. Cama, mesa e banho lidera em volume com 10.953 itens, mas cai para terceiro em receita. Relógios e presentes faz o movimento oposto: sétimo em volume, segundo em receita, sustentado pelo ticket médio de R$ 199,04, o maior entre as categorias de alto volume. A categoria `cool_stuff` sequer aparece no top 10 de volume mas figura em oitavo lugar em receita.

A implicação prática é que uma estratégia de marketplace focada apenas em volume de transações subestimaria categorias de alto valor unitário. Para a Olist, que cobra comissão sobre valor transacionado, categorias como relógios e presentes são desproporcionalmente relevantes em relação ao seu volume.

### Pergunta 2: Qual o ticket médio por categoria e como varia entre regiões?

A análise foi conduzida em três dimensões: por região, por categoria e no cruzamento entre ambas.

**2a. Ticket médio por região:**

| Região | Ticket Médio | Volume de Itens | Receita Total |
|---|---|---|---|
| Norte | R$ 159,19 | 1.983 | R$ 315.679,56 |
| Nordeste | R$ 148,51 | 9.951 | R$ 1.477.813,10 |
| Centro-Oeste | R$ 130,85 | 6.375 | R$ 834.169,41 |
| Sul | R$ 120,00 | 15.647 | R$ 1.877.578,61 |
| Sudeste | R$ 114,36 | 74.682 | R$ 8.540.290,22 |

<img width="588" height="347" alt="image" src="https://github.com/user-attachments/assets/b20e7fa9-afd3-44fa-99f3-122408c6961f" />

**2b. Ticket médio por categoria (mínimo 100 itens vendidos):**

Maiores tickets:

| Categoria | Ticket Médio | Volume | Receita Total |
|---|---|---|---|
| computers | R$ 1.098,92 | 199 | R$ 218.684,14 |
| home_appliances_2 | R$ 467,33 | 231 | R$ 107.953,95 |
| agro_industry_and_commerce | R$ 342,55 | 206 | R$ 70.566,10 |
| musical_instruments | R$ 283,13 | 651 | R$ 184.315,74 |
| small_appliances | R$ 277,74 | 658 | R$ 182.754,12 |
| fixed_telephony | R$ 216,92 | 255 | R$ 55.315,21 |
| construction_tools_safety | R$ 211,88 | 183 | R$ 38.773,22 |
| watches_gifts | R$ 199,04 | 5.859 | R$ 1.166.176,98 |
| furniture_bedroom | R$ 184,97 | 103 | R$ 19.051,80 |
| air_conditioning | R$ 184,51 | 289 | R$ 53.323,56 |

<img width="655" height="488" alt="image" src="https://github.com/user-attachments/assets/effb004f-91e4-403a-8cd2-2a589c1c5f6c" />

Menores tickets:

| Categoria | Ticket Médio | Volume | Receita Total |
|---|---|---|---|
| food_drink | R$ 55,55 | 269 | R$ 14.942,88 |
| electronics | R$ 56,81 | 2.729 | R$ 155.043,93 |
| food | R$ 57,58 | 499 | R$ 28.731,15 |
| christmas_supplies | R$ 58,25 | 150 | R$ 8.737,84 |
| drinks | R$ 59,64 | 361 | R$ 21.529,84 |
| telephony | R$ 69,95 | 4.430 | R$ 309.860,23 |
| books_technical | R$ 71,11 | 263 | R$ 18.702,23 |
| fashion_underwear_beach | R$ 73,28 | 127 | R$ 9.305,95 |
| fashion_bags_accessories | R$ 75,23 | 1.985 | R$ 149.329,39 |
| fashion_male_clothing | R$ 83,62 | 125 | R$ 10.452,33 |

<img width="643" height="365" alt="image" src="https://github.com/user-attachments/assets/3aa966be-4b81-41e2-a0e8-17de9cdda425" />

**2c. Cruzamento categoria x região (ticket médio em R$):**

| Categoria | Norte | Nordeste | Centro-Oeste | Sudeste | Sul |
|---|---|---|---|---|---|
| bed_bath_table | 104,50 | 100,14 | 89,85 | 92,43 | 97,38 |
| computers_accessories | 140,38 | 135,96 | 142,16 | 111,72 | 112,47 |
| furniture_decor | 120,74 | 100,90 | 88,02 | 82,97 | 95,09 |
| health_beauty | 198,49 | 176,34 | 135,03 | 120,46 | 125,40 |
| sports_leisure | 160,62 | 128,91 | 110,73 | 109,54 | 115,92 |

<img width="1180" height="398" alt="image" src="https://github.com/user-attachments/assets/d778d103-8ffd-48e7-a3c1-03a93b16e64f" />

**Discussão:** a visão por categoria mostra amplitude enorme, de R$ 55,55 em alimentos e bebidas a R$ 1.098,92 em computadores, uma razão de vinte vezes. Categorias de baixo ticket tendem a ser de consumo recorrente e baixo envolvimento na decisão de compra, enquanto as de alto ticket concentram bens duráveis.

O cruzamento da seção 2c é o achado mais relevante desta análise. A hipótese natural ao observar o ticket médio regional era que as regiões Norte e Nordeste comprassem categorias diferentes, mais caras. O pivot desmente isso: **dentro da mesma categoria**, o ticket médio no Norte é sistematicamente superior ao do Sudeste. Em saúde e beleza a diferença chega a 65% (R$ 198,49 contra R$ 120,46). O padrão se repete nas cinco categorias analisadas, sem exceção.

Isso significa que a diferença regional não é efeito de composição de mix de produtos, mas de comportamento de compra. Duas explicações são plausíveis e não mutuamente exclusivas: consumidores em regiões com menor oferta de varejo físico recorrem ao e-commerce para compras de maior valor, que justificam o frete e o prazo de entrega mais longos; ou compram em maior quantidade por transação para diluir o custo logístico. Ambas apontam para a mesma conclusão de negócio, que é a de que o custo de aquisição de cliente nessas regiões pode ser compensado por um valor por transação significativamente maior.

### Pergunta 3: Quais formas de pagamento são mais usadas e qual o parcelamento médio?

Análise sobre 96.478 pedidos únicos entregues, após deduplicação por `order_id`, já que forma de pagamento e parcelamento são atributos do pedido e não do item.

| Forma de Pagamento | Pedidos | Participação | Média de Parcelas | Ticket Médio |
|---|---|---|---|---|
| credit_card | 73.119 | 75,8% | 3,5 | R$ 165,67 |
| boleto | 19.191 | 19,9% | 1,0 | R$ 144,33 |
| voucher | 2.683 | 2,8% | 1,3 | R$ 123,09 |
| debit_card | 1.484 | 1,5% | 1,0 | R$ 140,41 |

<img width="657" height="332" alt="image" src="https://github.com/user-attachments/assets/b91ea6b1-5109-49d4-a21d-034aeb285c33" />

**Discussão:** o cartão de crédito responde por 75,8% dos pedidos com parcelamento médio de 3,5 vezes, confirmando o comportamento característico do consumidor brasileiro de fracionar compras mesmo em valores moderados. Considerando o ticket médio de R$ 165,67, a parcela típica fica em torno de R$ 47.

O boleto ocupa quase 20% dos pedidos com ticket médio de R$ 144,33, cerca de 13% abaixo do cartão. A diferença existe mas é menor do que se esperaria de um meio de pagamento que não permite parcelamento. A leitura mais provável é que a escolha pelo boleto reflete perfil de acesso a crédito, provavelmente clientes sem cartão ou que preferem evitá-lo, e não apenas o valor da compra. Para o marketplace, isso significa que o boleto sustenta uma fatia relevante de receita e não pode ser tratado como canal marginal de transações pequenas.

O cartão de débito representa apenas 1,5% dos pedidos, refletindo o período analisado, anterior à popularização de meios de pagamento instantâneo no Brasil.

**Nota metodológica:** a primeira versão desta análise agregava diretamente sobre a granularidade de item, o que inflava a contagem de pedidos e elevava artificialmente o ticket médio, já que pedidos com muitos itens eram contados múltiplas vezes e têm valor total maior. A deduplicação por `order_id` corrigiu a distorção. O caso ilustra como a granularidade da tabela fato precisa ser considerada em cada agregação.

### Pergunta 4: Como o volume de pedidos evoluiu ao longo dos meses?

| Ano | Mês | Pedidos | Receita Total |
|---|---|---|---|
| 2017 | 1 | 750 | R$ 111.798,36 |
| 2017 | 2 | 1.653 | R$ 234.223,40 |
| 2017 | 3 | 2.546 | R$ 359.198,85 |
| 2017 | 4 | 2.303 | R$ 340.669,68 |
| 2017 | 5 | 3.546 | R$ 489.338,25 |
| 2017 | 6 | 3.135 | R$ 421.923,37 |
| 2017 | 7 | 3.872 | R$ 481.604,52 |
| 2017 | 8 | 4.193 | R$ 554.699,70 |
| 2017 | 9 | 4.150 | R$ 607.399,67 |
| 2017 | 10 | 4.478 | R$ 648.247,65 |
| 2017 | 11 | 7.289 | R$ 987.765,37 |
| 2017 | 12 | 5.513 | R$ 726.033,19 |
| 2018 | 1 | 7.069 | R$ 924.645,00 |
| 2018 | 2 | 6.555 | R$ 826.437,13 |
| 2018 | 3 | 7.003 | R$ 953.356,25 |
| 2018 | 4 | 6.798 | R$ 973.534,09 |
| 2018 | 5 | 6.749 | R$ 977.544,69 |
| 2018 | 6 | 6.099 | R$ 856.077,86 |
| 2018 | 7 | 6.159 | R$ 867.953,46 |
| 2018 | 8 | 6.351 | R$ 838.576,64 |

<img width="540" height="712" alt="image" src="https://github.com/user-attachments/assets/1240d6a1-6272-4c50-8bc0-d62a9ec32008" />

**Discussão:** o ano de 2017 mostra crescimento acelerado e consistente, com o volume mensal multiplicando-se por quase dez vezes entre janeiro (750 pedidos) e novembro (7.289). O pico de novembro é quase certamente efeito de Black Friday, e a queda subsequente em dezembro para 5.513 pedidos sugere antecipação de compras de fim de ano.

O comportamento em 2018 é qualitativamente diferente. O volume se estabiliza na faixa de 6.000 a 7.000 pedidos mensais, sem tendência clara de crescimento. Dois pontos merecem atenção: primeiro, o patamar de janeiro de 2018 (7.069) é superior ao de dezembro de 2017, o que indica que o crescimento de novembro não foi apenas sazonal mas incorporou base de clientes de forma permanente. Segundo, a estabilização pode indicar tanto maturidade de mercado quanto limitação de capacidade operacional, hipótese que exigiria dados adicionais para ser testada.

A receita acompanha o volume com proporcionalidade, sugerindo que o ticket médio permaneceu estável ao longo do período, sem inflação de preços ou mudança significativa de mix.

**Delimitação:** a série termina em agosto de 2018 porque o dataset se encerra em setembro daquele ano, e o mês final está incompleto. Os meses de setembro a dezembro de 2016 foram excluídos por conterem volume residual, que corresponde ao período inicial de operação da plataforma.

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

<img width="658" height="497" alt="image" src="https://github.com/user-attachments/assets/3f2ac2dd-7d34-454f-809b-58330b713c28" />

**Discussão:** São Paulo concentra 40.501 clientes, mais do que Rio de Janeiro e Minas Gerais somados. Os três estados do Sudeste representam aproximadamente 58% da base, refletindo a concentração econômica e populacional brasileira, mas também possivelmente a concentração de vendedores da plataforma, que reduz custo e prazo de frete para compradores próximos.

**Limitação metodológica:** o número de clientes é idêntico ao de pedidos em todos os estados. Isso ocorre porque no dataset da Olist o campo `customer_id` é gerado por pedido, não por pessoa. A identificação de cliente recorrente exigiria o campo `customer_unique_id`, disponível na tabela original mas não incorporado a esta modelagem. Consequentemente, esta análise mede concentração de transações por estado, não de indivíduos distintos. A correção está listada como trabalho futuro.

Combinando com a Pergunta 2, emerge o padrão central deste MVP: o Sudeste domina em volume absoluto, enquanto Norte e Nordeste apresentam maior valor por transação. São estratégias comerciais distintas para mercados distintos.

### Pergunta 6: Existe relação entre avaliação e valor gasto?

| Nota | Pedidos | Valor Médio | Valor Mínimo | Valor Máximo |
|---|---|---|---|---|
| 1 | 9.292 | R$ 164,87 | R$ 3,54 | R$ 13.440,00 |
| 2 | 2.908 | R$ 143,73 | R$ 5,31 | R$ 3.999,00 |
| 3 | 7.884 | R$ 127,60 | R$ 3,50 | R$ 2.919,40 |
| 4 | 18.861 | R$ 132,36 | R$ 0,85 | R$ 4.690,00 |
| 5 | 56.664 | R$ 134,66 | R$ 0,85 | R$ 6.735,00 |

<img width="711" height="356" alt="image" src="https://github.com/user-attachments/assets/bdf49377-d856-45b6-b593-9732ef00aa4a" />

**Discussão:** existe relação, mas não é linear. Pedidos com nota 1 apresentam o maior valor médio de todos (R$ 164,87), enquanto as notas 2 a 5 se concentram numa faixa estreita entre R$ 127 e R$ 143. O padrão não é de correlação contínua, é de concentração de insatisfação severa nas compras de maior valor.

A explicação mais provável envolve assimetria de expectativa e de risco. Compras caras geram expectativa proporcionalmente maior, e qualquer falha de entrega ou divergência de produto tem consequência mais grave para o comprador. Além disso, itens de maior valor tendem a ser maiores fisicamente, o que aumenta a probabilidade de avaria no transporte e de atraso logístico.

O valor máximo na nota 1 (R$ 13.440,00) é o maior de toda a base, superando inclusive o máximo entre pedidos com nota 5. Isso reforça a leitura de que falhas em compras de alto valor produzem avaliações extremamente negativas.

Do ponto de vista de negócio, a distribuição de volume é tranquilizadora: 56.664 pedidos com nota 5 contra 9.292 com nota 1, uma proporção de seis para um. A experiência majoritária é positiva. O problema é específico e endereçável, concentrado no segmento de alto ticket, e sugere que investimento em acompanhamento logístico diferenciado para pedidos acima de determinado valor teria retorno direto em satisfação.

**Delimitação importante:** esta análise usa apenas pedidos com status `delivered`, o que significa que mede satisfação entre clientes que efetivamente receberam o produto. A distribuição de notas na camada Bronze, que inclui pedidos não entregues, é menos favorável: 11.424 notas 1 contra os 9.292 aqui reportados. A diferença sugere que avaliações de pedidos cancelados ou não entregues concentram notas baixas, o que é intuitivo. A conclusão sobre a relação entre valor e insatisfação permanece válida, mas o nível geral de satisfação medido aqui é otimista em relação à experiência completa da base.

### Conclusão Geral

O pipeline revelou um marketplace em transição entre fase de crescimento acelerado (2017) e consolidação (2018), com três padrões estruturais claros.

O primeiro é a dissociação entre volume e valor na dimensão geográfica. O Sudeste concentra 68% dos itens e 70% da receita, mas apresenta o menor ticket médio do país. Norte e Nordeste invertem a relação, e o cruzamento por categoria confirmou que isso é comportamento de compra, não mix de produtos. A mesma categoria custa sistematicamente mais nessas regiões.

O segundo é a centralidade do crédito parcelado. Três quartos dos pedidos passam por cartão de crédito com parcelamento médio de 3,5 vezes, enquanto o boleto sustenta quase 20% do volume com ticket médio apenas 13% inferior, indicando que o meio de pagamento reflete perfil de acesso a crédito mais do que valor da compra.

O terceiro é a concentração de insatisfação no alto ticket. Pedidos com avaliação mínima têm valor médio 22% superior à média geral, apontando uma lacuna específica de experiência em compras de maior valor.

Do ponto de vista de Engenharia de Dados, o exercício reforçou duas lições. A qualidade do pipeline se define na camada mais próxima da origem: o problema de leitura multilinha nos reviews passaria despercebido em uma inspeção superficial e teria contaminado silenciosamente todas as análises subsequentes. E a granularidade da tabela fato precisa ser considerada em cada agregação: a análise de formas de pagamento produziu números plausíveis mas incorretos até que a deduplicação por pedido fosse aplicada. Nos dois casos, o erro não gerava exceção, apenas resultados errados.

---

## Autoavaliação

### Objetivos atingidos

As seis perguntas de negócio definidas antes do início da coleta foram integralmente respondidas. O pipeline completo foi implementado seguindo a arquitetura Medalhão, com cada camada cumprindo sua responsabilidade específica: preservação e rastreabilidade na Bronze, limpeza e padronização na Silver, e modelagem integrada na Gold. Todas as tabelas foram persistidas como Delta e documentadas no Unity Catalog.

Considero que o resultado mais valioso do trabalho não foram as respostas em si, mas a estrutura que permitiu chegar a elas de forma reprodutível. O pipeline pode ser reexecutado do zero com dados atualizados sem nenhuma intervenção manual além do upload dos arquivos.

### Dificuldades encontradas

A maior dificuldade foi conceitual, não técnica. Sendo minha primeira experiência com Databricks, Spark e arquitetura Lakehouse, a curva inicial envolveu entender por que cada camada existe antes de conseguir implementá-las com propósito. A tentação inicial era pular direto da ingestão para a análise, e foi preciso disciplina para respeitar a separação de responsabilidades.

Tecnicamente, três pontos exigiram esforço.

A configuração do ambiente, especialmente a criação de volumes no Unity Catalog e a integração com GitHub via Databricks Repos, consumiu tempo por diferenças entre a documentação oficial e a interface da Free Edition.

A sintaxe do PySpark difere substancialmente do Pandas, que é a referência mais comum para quem inicia em dados. Conceitos como avaliação preguiçosa e a necessidade de ações explícitas para materializar resultados não são intuitivos no começo.

O problema de leitura dos reviews foi o mais instrutivo. O erro inicial apontava para um cast inválido, mas a causa real estava duas camadas acima, na forma como o CSV era interpretado. Diagnosticar isso exigiu entender que o sintoma e a causa podem estar distantes no pipeline, e que corrigir o sintoma teria propagado dados incorretos silenciosamente.

### Erros cometidos e corrigidos

Dois erros merecem registro porque ambos produziam resultados plausíveis, o que os torna mais perigosos do que erros que geram exceção.

O primeiro foi a leitura padrão do CSV de reviews, que gerou 4.938 registros corrompidos. O pipeline executava sem erro até o momento em que uma conversão de tipo falhou, várias etapas adiante.

O segundo foi a agregação da Pergunta 3 diretamente sobre a granularidade de item. Os números resultantes eram internamente consistentes e pareciam corretos, mas contavam pedidos múltiplas vezes e inflavam o ticket médio. Só percebi ao cruzar o total com a contagem de pedidos entregues da camada Bronze, que não batia.

A lição comum aos dois é que validação cruzada entre camadas é mais confiável do que inspeção isolada de resultados.

### Limitações reconhecidas

A análise de clientes na Pergunta 5 mede transações, não pessoas, pois o campo `customer_unique_id` não foi incorporado à modelagem. Percebi isso ao interpretar os resultados, quando a igualdade exata entre número de clientes e de pedidos chamou atenção.

A tabela de geolocalização foi ingerida mas não utilizada, o que representa trabalho de ingestão sem retorno analítico. Foi decisão consciente de escopo, mas em um cenário profissional teria sido melhor avaliar isso antes da coleta.

Os comentários textuais das avaliações, provavelmente o dado mais rico do dataset para entender as notas baixas, permaneceram intocados.

A análise da Pergunta 6 mede satisfação apenas entre pedidos entregues, o que produz um retrato mais favorável do que a experiência completa da base de clientes.

### Trabalhos futuros

- Incorporar `customer_unique_id` para distinguir clientes recorrentes de novos e analisar taxa de recompra, o que permitiria calcular valor de tempo de vida do cliente
- Aplicar processamento de linguagem natural sobre os comentários das avaliações para identificar os motivos declarados das notas baixas, complementando o achado da Pergunta 6
- Estender a análise de satisfação para incluir pedidos não entregues, separando insatisfação com o produto de insatisfação com a logística
- Utilizar a tabela de geolocalização para análise espacial de distância entre vendedor e comprador, testando a hipótese de que o ticket médio regional se relaciona com custo logístico
- Automatizar a execução do pipeline com Databricks Workflows, encadeando os notebooks com dependências explícitas
- Construir dashboard no Databricks SQL conectado à camada Gold, substituindo a leitura de outputs em notebook
- Separar as camadas em schemas distintos (`bronze`, `silver`, `gold`) em vez de prefixos de nomenclatura, aproximando a organização do padrão adotado em ambientes produtivos
- Implementar testes de qualidade automatizados com expectativas declarativas, incluindo validação de contagem entre camadas, transformando a análise manual da etapa 4.5 em validação contínua a cada execução
