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
- **Período:** 2006 a 2012, com ciclos de medição contínua (periodicidade anual, com intervalos entre 10 e 14 meses).
- **Volume:** ~153 mil registros na base bruta (21 variáveis) e ~470 pares de medição na base agregada (53 variáveis).
- **Principais variáveis:** `PROJETO`, `TALHAO`, `CICLO`, `ESPACAMENTO`, `MG`, `DATA_PLANTIO`, `DAP`, `AltObs`, `Volume`, `Fuste`, `idade1`, `idade2`, `volumeha1`, `volumeha2`, `ba1`, `ba2`, `hd1`, `hd2`, `nha`, `classeprodutividade`.
- **Coordenadas:** mapeamento espacial dos talhões a partir do nome do projeto na região de Virginópolis/MG (tabela auxiliar de coordenadas).

### 2.2 Base Meteorológica BR-DWGD — *Base Pública*
- **Fonte:** *Brazilian Daily Weather Gridded Data* (BR-DWGD), desenvolvida por Alexandre Cândido Xavier (UFES).
- **Período:** 1961 a 2022 (periodicidade diária, resolução espacial de ~11 km).
- **Extração:** via **Google Earth Engine (GEE)** para os pontos de grade correspondentes aos projetos em Virginópolis/MG, convertida para CSV tabular e versionada no repositório.
- **Variáveis:** precipitação (`pr`), evapotranspiração (`ET`), temperatura máxima (`Tmax`), temperatura mínima (`Tmin`), umidade relativa (`RH`), radiação solar (`Rs`) e velocidade do vento (`u2`).

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
2. **ETL / ELT (`Python` + `experimental-ml-framework` + `Pandas`):** limpeza, padronização de datas e decimais, tratamento de nulos/outliers, geração de chaves substitutas (`SK_*`), integração espaço-temporal e registro de transformações (antes/depois).
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
- **ETL & Análise de Dados:** `pandas`, `numpy`, `openpyxl`, `experimental-ml-framework`
- **Extração Climática:** `earthengine-api` (Google Earth Engine)
- **Data Warehouse (SGBD):** `DuckDB`
- **Machine Learning:** `scikit-learn`, `experimental-ml-framework`
- **Business Intelligence:** Power BI / ferramenta de BI conectada ao DuckDB

---

## 6. Estrutura do Repositório

```text
BI-florestal/
├── .gitignore                          # Regras de exclusão (dados privados, .venv, etc.)
├── README.md                           # Documentação principal do projeto
├── requirements.txt                    # Dependências Python (inclui experimental-ml-framework)
├── Proposta_BI.pdf                     # Apresentação da Etapa 1
├── Trabalho 01-Arquitetura de BI.pdf   # Enunciado oficial do trabalho
├── modelo_estrela.pdf                  # Diagrama do modelo multidimensional (constelação de fatos)
├── dados_florestais_brutos/            # [IGNORADO NO GIT] Planilhas privadas do IFC (2006–2012)
├── dados/                              # [Planejado] Dados públicos (BR-DWGD) e saídas tratadas (CSVs privados ignorados)
├── etl/                                # [Planejado] Scripts de extração (GEE/IFC), transformação e carga
├── dw/                                 # [Planejado] DDL, banco DuckDB e scripts de validação do DW
├── ml/                                 # [Planejado] Experimentos, treinamento e métricas com experimental-ml-framework
├── dashboards/                         # [Planejado] Arquivos e documentação dos dashboards de BI
└── docs/                               # [Planejado] Logs de transformações (antes/depois) e notas técnicas
```

---

## 7. Como Reproduzir o Ambiente

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
   - Os dados climáticos públicos (BR-DWGD) e os scripts do pipeline ficam disponíveis no repositório.
