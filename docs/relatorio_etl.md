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

## 3. Mapeamento Origem → Destino (Linhagem dos Atributos)

| Origem (Bruto) | Transformação Aplicada | Destino no DW / ML |
|---|---|---|
| `MedInicio` (IFC) e `Data`, `Ano`, `Mes` (BR-DWGD) | Parse unificado (`parse_data_mista`) para `DATE` ISO + extração de `Ano` e `Mes` | `Dim_Tempo` (`Data_Medicao`, `Ano`, `Mes`) |
| `ANO IFC` (IFC) | Coerção para `Int64` | `Dim_Tempo` (`Ano_IFC`) |
| `DATA_PLANTIO` + `IDADE` (IFC) | Recálculo de `IDADE` quando ausente + derivação de `Idade_Anos` e `Fase_Desenvolvimento` | `Dim_Idade` (`IDADE`, `Idade_Anos`, `Fase_Desenvolvimento`) |
| `ROT`, `CICLO` (IFC) | Coerção numérica + imputação de `CICLO = 1` quando nulo (2006) | `Dim_Idade` (`ROT`, `CICLO`) |
| `PROJETO`, `COD_TALHAO`, `TALHAO`, `Parcela`, `Area`, `AreaCorr` | Grafia canônica de `PROJETO`, `zfill(3)` em `COD_TALHAO` + linha âncora por `PROJETO` para o clima | `Dim_Localizacao` |
| `MG`, `ESPACAMENTO` (IFC) | Limpeza de texto + propagação de `ESPACAMENTO` (de 2012) por `(PROJETO, COD_TALHAO)` | `Dim_Silvicultura` |
| `Cova`, `Fuste`, `Fila`, `Codigos` (IFC) | Coerção para `Int64` + preenchimento de `Codigos` nulo com `'N'` | `Dim_Fuste` |
| `DAP`, `AltObs`, `Volume (m³)` (IFC) | Coerção segura (`#NUM!` → `NULL`) + ligação com as 5 `SK_*` | `Fato_Inventario_Fuste` (`DAP`, `AltObs`, `Volume_m3`) |
| `pr`, `ET`, `RH`, `u2`, `Rs`, `Tmax`, `Tmin` (BR-DWGD) | Conversão de `ET` (`mm/dia` → `mm/mês`) + ligação por `SK_Tempo` e `SK_Localizacao` | `Fato_Clima` |
| Parcelas pareadas (`t1 -> t2`) + Clima no intervalo | Expansão de volume por área basal, `ima`, `ica`, `sobrevivencia_pct`, `invid2`, `ln_*` e índices (`vpd`, `dtr`, `tmed`, `indice_umidade`, `balanco_hidrico`, `razao_agua_radiacao`, `delta_*`, `media_*`) | `dataset_ml_prognose.csv` (camada ML) |

---

## 4. Carga do Data Warehouse (DuckDB)

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

## 5. Dataset Mestre para Machine Learning (`dataset_ml_prognose.csv`)

- **Total de pares válidos de medição (`t1 -> t2`):** 3,262
- **Pares filtrados por intervalo atípico (`dt_meses` fora de `[6, 24]`) ou sobrevivência `> 120%`:** 59
- **Projetos distintos nos pares:** 88
- **Total de colunas no dataset enriquecido:** 100
- **Enriquecimento biométrico (`t1 -> t2`):** `invid2` ($1/I_2$), `razao_idade` ($I_1/I_2$), `dap_min1`, `ht_min1`, `ht_max1`, `ln_ba1`, `ln_volumeha1`, `ln_volumeha2`.
- **Enriquecimento agroclimático:** variáveis no intervalo exato `[t1, t2]` (`pr_acum`, `et_acum`, `balanco_hidrico`, `indice_umidade`, `tmax_media`, `tmin_media`, `tmed_media`, `dtr_media`, `rh_media`, `vpd_media` via Tetens/FAO-56, `rs_media`, `u2_media`, `razao_agua_radiacao`), além dos respectivos blocos anuais (`*_1`, `*_2`), diferenças (`delta_*`) e médias (`media_*`) entre `ano1` e `ano2`.

---

## 6. Dificuldades Encontradas e Soluções Adotadas

1. **Unidade da Evapotranspiração (`ET`) no BR-DWGD:** na coleção `BR-DWGD_Monthly`, a banda `ET` representa a média diária (`mm/dia`), enquanto `pr` representa o acumulado mensal (`mm/mês`). *Solução:* multiplicou-se `ET` por `days_in_month` no ETL para obter `mm/mês` reais e evitar balanço hídrico artificialmente positivo.
2. **Viés de amostragem de altura/volume entre campanhas (`2006–2010` vs `2011–2012`):** nos anos iniciais, apenas ~25–30% das árvores possuíam `AltObs` e `Volume (m³)` medidos; em `2011–2012`, mais de 92% possuíam. *Solução:* na agregação por parcela para ML, o volume total foi expandido proporcionalmente à área basal medida (`vol_medido * (ba_total / ba_com_vol)`).
3. **Heterogeneidade das planilhas anuais:** datas em três formatos distintos (`Timestamp`, serial Excel e texto `DD/MM/YYYY`), abas extras em `2008` (`Plan2`, `Coefs`), erros `#NUM!` em árvores com `DAP=0` e `ESPACAMENTO` presente apenas em 2012. *Solução:* parser unificado (`parse_data_mista`), leitura explícita da aba `Plan1`, coerção segura de numéricos e propagação relacional de espaçamento por `(PROJETO, COD_TALHAO)`.
4. **Variação de `Area`/`AreaCorr` e grafia de projetos entre anos:** pequenas diferenças cadastrais impediam a junção direta na `Dim_Localizacao` e no cruzamento com `Fato_Clima`. *Solução:* normalização canônica de `PROJETO` (`Água Branca`, `Homéro`, `J. Leôncio`) e agregação determinística da dimensão por `(PROJETO, COD_TALHAO, TALHAO, Parcela)`.
