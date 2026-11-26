# Relatório de Execução e Transformações do ETL (Etapa 2)

## 1. Resumo da Extração (Inventário Florestal Contínuo)

| Arquivo | Linhas Brutas | Colunas |
|---|---:|---:|
| `Virginopolis 2006.xlsx` | 33,771 | 20 |
| `Virginopolis 2007.xlsx` | 30,702 | 20 |
| `Virginopolis 2008.xlsx` | 27,167 | 20 |
| `Virginopolis 2009.xlsx` | 24,235 | 20 |
| `Virginopolis 2010.xlsx` | 19,333 | 20 |
| `Virginopolis 2011.xlsx` | 19,732 | 20 |
| `Virginopolis 2012.xlsx` | 25,338 | 21 |

- **Total bruto consolidado:** 180,278 registros
- **Registros descartados sem `MedInicio`:** 0
- **Duplicatas exatas removidas:** 0
- **Total carregado em `Fato_Inventario_Fuste`:** 180,278 registros
- **Valores nulos em `Volume_m3` (árvores sem `AltObs` medida em campo):** 101,534

---

## 2. Principais Transformações (Antes × Depois)

| Campo / Problema | Antes (Bruto) | Depois (Tratado) | Solução Adotada |
|---|---|---|---|
| `DATA_PLANTIO` / `MedInicio` | `Timestamp` (2006/11/12), serial Excel `38393` (2007–09), string `'13/02/2005'` (2010) | `DATE` ISO padronizado (`YYYY-MM-DD`) | Conversor unificado (`parse_data_mista`) |
| `PROJETO` | `'Agua Branca'` vs `'Água Branca'`, `'Homero'` vs `'Homéro'` | `'Água Branca'`, `'Homéro'` | Normalização Unicode + mapa canônico |
| `COD_TALHAO` | `'011'` (2006–09) vs `'11'` (2010) | `'011'` | Zero-padding para 3 dígitos (`zfill(3)`) |
| `IDADE` | Fórmula Excel `'=((H2-G2)/365.25)*12'` | `float` em meses + `Idade_Anos` + `Fase_Desenvolvimento` | Recálculo determinístico quando ausente |
| `CICLO` | 100% nulo em 2006 | `1` (Int64) | Imputação padrão de primeiro ciclo |
| `ESPACAMENTO` | Ausente em 2006–2011; presente em 2012 (`'3,33 x 3,00'`) | Propagado por `(PROJETO, COD_TALHAO)` ou `'Não informado'` | Propagação relacional entre campanhas |
| `Volume (m³)` | `'#NUM!'` quando `DAP=0` e `AltObs=0` | `NULL` (`NaN`) | Coerção numérica segura (`pd.to_numeric(..., errors='coerce')`) |

---

## 3. Carga do Data Warehouse (DuckDB)

| Tabela | Registros Carregados |
|---|---:|
| `Dim_Tempo` | 457 |
| `Dim_Idade` | 1,438 |
| `Dim_Localizacao` | 1,908 |
| `Dim_Silvicultura` | 267 |
| `Dim_Fuste` | 1,909 |
| `Fato_Inventario_Fuste` | 180,278 |
| `Fato_Clima` | 7,980 |

---

## 4. Dataset Mestre para Machine Learning (`dataset_ml_prognose.csv`)

- **Total de pares de medição (`t1 -> t2`):** 3,321
- **Projetos distintos nos pares:** 88
- **Variáveis climáticas integradas no intervalo exato `[t1, t2]`:** `pr_acum`, `et_acum`, `balanco_hidrico`, `tmax_media`, `tmin_media`, `rh_media`, `rs_media`, `u2_media`.
