# BI Florestal & Prognose de Volume Comercial de Eucalipto

Projeto prático da disciplina **COM11075 – Tópicos Especiais em BD II** (UFES • CCENS • Departamento de Computação), voltado à implementação de uma arquitetura de **Business Intelligence (BI)** e **Machine Learning (ML)** aplicada ao planejamento de colheita florestal e à prognose de volume comercial de eucalipto integrada a variáveis climáticas.

---

## 1. Objetivos

- Integrar dados de **Inventário Florestal Contínuo (IFC)** e dados **meteorológicos diários (BR-DWGD)** em um **Data Warehouse** multidimensional (constelação de fatos).
- Construir um processo de **ETL reexecutável em Python**, com limpeza, padronização, integração espacial/temporal, tratamento de inconsistências e geração de logs de transformação.
- Disponibilizar:
  - **Camada analítica para BI:** banco **DuckDB** populado com as tabelas fato e dimensão para alimentar dashboards gerenciais.
  - **Camada analítica para ML:** datasets tratados em **CSV** para treinamento, validação e avaliação de modelos de previsão e prognose florestal utilizando o [`experimental-ml-framework`](https://github.com/aleerangel/experimental-ml-framework).

---

## 2. Bases de Dados Utilizadas

### 2.1 Inventário Florestal Contínuo (IFC) — *Base Privada*
- **Fonte:** empresa florestal privada com atuação na região de Virginópolis/MG (uso autorizado para fins acadêmicos, mantida fora do repositório via `.gitignore`).
- **Período:** 2006 a 2012, com ciclos de medição contínua (periodicidade anual, com intervalos entre 6 e 24 meses nos pares válidos).
- **Volume:** **180.278 registros** de fustes carregados em `Fato_Inventario_Fuste` e **3.262 pares válidos** de medição (`t1 -> t2`, **100 variáveis** enriquecidas) no dataset de prognose (`dados/processados/dataset_ml_prognose.csv`).
- **Principais variáveis:** `PROJETO`, `TALHAO`, `CICLO`, `ESPACAMENTO`, `MG`, `DATA_PLANTIO`, `DAP`, `AltObs`, `Volume_m3`, `Fuste`, `idade1`, `idade2`, `volumeha1`, `volumeha2`, `ba1`, `hd1`, `nha1`, `ima1`, `ima2`, `ica`, `sobrevivencia_pct`.
- **Coordenadas:** mapeamento espacial dos projetos em `dados/publicos/projetos_coordenadas.csv` (98 projetos).

### 2.2 Base Meteorológica BR-DWGD — *Base Pública*
- **Fonte:** *Brazilian Daily Weather Gridded Data* (BR-DWGD), desenvolvida por Alexandre Cândido Xavier (UFES).
- **Período extraído:** 2006 a 2012 (série mensal via `projects/ee-alexandrexavier/assets/BR-DWGD_Monthly`, **8.232 registros** brutos em `dados/publicos/br_dwgd_mensal.csv` e **7.980 registros** integrados em `Fato_Clima`).
- **Extração:** via **Google Earth Engine (GEE)** (`etl/extract_br_dwgd.py`) para os pontos de grade correspondentes aos projetos em Virginópolis/MG.
- **Variáveis:** precipitação (`pr`), evapotranspiração (`ET`, convertida de `mm/dia` para `mm/mês` no ETL), temperatura máxima (`Tmax`), temperatura mínima (`Tmin`), umidade relativa (`RH`), radiação solar (`Rs`) e velocidade do vento (`u2`), além de índices agroclimáticos derivados (`balanco_hidrico`, `indice_umidade`, `tmed`, `dtr`, `vpd`, `razao_agua_radiacao`).

---

## 3. KPIs e Perguntas Analíticas

### Principais KPIs
- **Volume Comercial Médio por Hectare:**
  $$\text{Volume Médio/ha} = \frac{\sum \text{Volume dos Fustes}}{\sum \text{Área das Parcelas}}$$
- **Incremento Médio Anual (IMA):**
  $$\text{IMA} = \frac{\text{volumeha}_2}{\text{idade}_2 / 12}$$
- **Incremento Corrente Anual (ICA):**
  $$\text{ICA} = \frac{\text{volumeha}_2 - \text{volumeha}_1}{(\text{idade}_2 - \text{idade}_1) / 12}$$
- **Taxa de Sobrevivência (%):**
  $$\text{Sobrevivência (\%)} = \left(\frac{\text{Árvores vivas em } t_2}{\text{Árvores inicialmente plantadas}}\right) \times 100$$
- **Balanço Hídrico Acumulado:**
  $$\text{Balanço Hídrico} = \sum (pr_t - ET_t)$$

### Perguntas Analíticas
1. Qual é o estoque de madeira disponível (volume/ha e área basal) por projeto, talhão, rotação, ciclo e material genético?
2. Em que idade ocorre o cruzamento entre **ICA** e **IMA** (ponto técnico de corte) para cada material genético e talhão?
3. Como períodos de déficit hídrico e extremos térmicos afetam o incremento volumétrico e a inclinação das curvas de crescimento?
4. Como evolui a mortalidade (redução de fustes/ha) ao longo da idade biológica e entre diferentes safras de plantio?
5. É possível prever o volume comercial futuro (prognose florestal) combinando variáveis dendrométricas e climáticas com modelos de Machine Learning?

---

## 4. Arquitetura da Solução e Modelo Multidimensional

### Fluxo da Arquitetura
1. **Fontes:** Planilhas IFC privadas (`.xlsx`) + série climática BR-DWGD extraída via Google Earth Engine (`.csv`) + tabela de coordenadas dos talhões.
2. **ETL (`Python` + `Pandas` + `NumPy`):** limpeza, padronização de datas e decimais, tratamento de nulos/outliers, geração de chaves substitutas (`SK_*`), integração espaço-temporal, enriquecimento biométrico/agroclimático e carga no DuckDB.
3. **Armazenamento:**
   - **Data Warehouse (`DuckDB`):** constelação de fatos com `5` dimensões e `2` tabelas fato.
   - **Datasets para ML (`CSV`):** bases tratadas para treinamento e avaliação de modelos preditivos.
4. **Camada Analítica:**
   - **BI:** dashboards para análise de estoque, crescimento (ICA × IMA), clima e mortalidade.
   - **ML:** modelos de prognose florestal treinados e avaliados com o `experimental-ml-framework`.

### Modelo Multidimensional (Constelação de Fatos)
- **Dimensões:**
  - `Dim_Tempo` (`SK_Tempo`, `Data_Medicao`, `Ano_IFC`, `Ano`, `Mes`)
  - `Dim_Localizacao` (`SK_Localizacao`, `PROJETO`, `COD_TALHAO`, `TALHAO`, `Parcela`, `Area`, `AreaCorr`)
  - `Dim_Idade` (`SK_Idade`, `IDADE`, `Idade_Anos`, `Fase_Desenvolvimento`, `ROT`, `CICLO`)
  - `Dim_Silvicultura` (`SK_Silvicultura`, `MG`, `ESPACAMENTO`)
  - `Dim_Fuste` (`SK_Fuste`, `Cova`, `Fuste`, `Fila`, `Codigos`)
- **Fatos:**
  - `Fato_Inventario_Fuste` (`SK_Tempo`, `SK_Idade`, `SK_Localizacao`, `SK_Silvicultura`, `SK_Fuste`, `DAP`, `AltObs`, `Volume_m3`)
  - `Fato_Clima` (`SK_Tempo`, `SK_Localizacao`, `pr`, `ET`, `RH`, `u2`, `Rs`, `Tmax`, `Tmin`)

---

## 5. Tecnologias Empregadas

- **Linguagem:** Python (`3.15+`)
- **ETL & Análise de Dados:** `pandas`, `numpy`, `openpyxl`
- **Extração Climática:** `earthengine-api` (Google Earth Engine)
- **Data Warehouse (SGBD):** `DuckDB`
- **Machine Learning:** `scikit-learn`, `experimental-ml-framework`
- **Business Intelligence:** Power BI / ferramenta de BI conectada ao DuckDB

---

## 6. Estrutura do Repositório

```text
BI-florestal/
├── .gitignore                          # Regras de exclusão (dados privados, .venv, DuckDB, etc.)
├── README.md                           # Documentação principal do projeto
├── requirements.txt                    # Dependências Python (inclui experimental-ml-framework)
├── Proposta_BI.pdf                     # Apresentação da Etapa 1
├── Trabalho 01-Arquitetura de BI.pdf   # Enunciado oficial do trabalho
├── modelo_estrela.pdf                  # Diagrama do modelo multidimensional (constelação de fatos)
├── dados_florestais_brutos/            # [IGNORADO NO GIT] Planilhas privadas do IFC (2006–2012)
├── dados/
│   ├── publicos/                       # Coordenadas dos projetos e série mensal BR-DWGD (GEE)
│   └── processados/                    # [IGNORADO NO GIT] Staging intermediário e dataset_ml_prognose.csv
├── etl/
│   ├── extract_br_dwgd.py              # Extração climática mensal via Google Earth Engine
│   ├── extract.py                      # Etapa Extract (leitura IFC bruto + BR-DWGD -> staging)
│   ├── transform.py                    # Etapa Transform (limpeza, padronização, enriquecimento ML)
│   ├── load.py                         # Etapa Load (carga no DuckDB + relatório de auditoria)
│   └── run_etl.py                      # Orquestrador completo (Extract -> Transform -> Load)
├── dw/
│   ├── schema.sql                      # DDL das 5 dimensões, 2 fatos e índices no DuckDB
│   ├── validate_dw.py                  # Auditoria pós-carga (contagens, 0 chaves órfãs e KPIs)
│   └── bi_florestal.duckdb             # [IGNORADO NO GIT] Banco DuckDB gerado pelo ETL
├── ml/                                 # [Etapa 3 / ML] Experimentos com experimental-ml-framework
├── dashboards/                         # [Etapa 3 / BI] Dashboards gerenciais conectados ao DuckDB
└── docs/
    └── relatorio_etl.md                # Relatório auditável (antes × depois, carga e dificuldades)
```

---

## 7. Como Reproduzir e Executar o Pipeline

1. **Clonar o repositório:**
   ```bash
   git clone https://github.com/aleerangel/BI-florestal.git
   cd BI-florestal
   ```

2. **Criar e ativar o ambiente virtual:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. **Instalar as dependências:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Dados privados:**
   - Por restrição de confidencialidade, os arquivos brutos do inventário florestal (`dados_florestais_brutos/*.xlsx`) não são versionados no GitHub. Para execução completa do ETL, posicione as planilhas nessa pasta localmente.
   - Os dados climáticos públicos (`dados/publicos/br_dwgd_mensal.csv`) e todos os scripts do pipeline estão disponíveis no repositório.

5. **Executar o ETL e validar o Data Warehouse:**
   - **Execução completa (orquestrador):**
     ```bash
     python etl/run_etl.py
     python dw/validate_dw.py
     ```
   - **Execução modular etapa por etapa (`Extract -> Transform -> Load`):**
     ```bash
     python etl/extract.py
     python etl/transform.py
     python etl/load.py
     python dw/validate_dw.py
     ```
