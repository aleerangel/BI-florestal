"""Etapa 3 do ETL (Load): carga do Data Warehouse DuckDB e relatório de auditoria."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


def construir_e_carregar_dw(
    df_inv: pd.DataFrame,
    df_clima: pd.DataFrame,
    caminho_schema: Path,
    caminho_db: Path,
) -> dict[str, int]:
    """Cria as dimensões com surrogate keys, popula o DuckDB e retorna contagens."""
    caminho_db.parent.mkdir(parents=True, exist_ok=True)
    if caminho_db.exists():
        caminho_db.unlink()

    con = duckdb.connect(str(caminho_db))
    con.execute(caminho_schema.read_text(encoding="utf-8"))

    # 1. Dim_Tempo (união de datas do inventário e meses climáticos)
    tempo_inv = df_inv[["Data_Medicao", "ANO_IFC", "Ano", "Mes"]].rename(
        columns={"ANO_IFC": "Ano_IFC"}
    )
    tempo_cli = df_clima[["Data_Medicao", "Ano", "Mes"]].copy()
    tempo_cli["Ano_IFC"] = pd.NA
    tempo_cli = tempo_cli[["Data_Medicao", "Ano_IFC", "Ano", "Mes"]]

    dim_tempo = (
        pd.concat([tempo_inv, tempo_cli], ignore_index=True)
        .drop_duplicates(subset=["Data_Medicao", "Ano_IFC", "Ano", "Mes"])
        .sort_values(["Data_Medicao", "Ano_IFC"], na_position="last")
        .reset_index(drop=True)
    )
    dim_tempo.insert(0, "SK_Tempo", np.arange(1, len(dim_tempo) + 1))

    # 2. Dim_Idade
    dim_idade = (
        df_inv[["IDADE", "Idade_Anos", "Fase_Desenvolvimento", "ROT", "CICLO"]]
        .drop_duplicates()
        .sort_values(["ROT", "CICLO", "IDADE"])
        .reset_index(drop=True)
    )
    dim_idade.insert(0, "SK_Idade", np.arange(1, len(dim_idade) + 1))

    # 3. Dim_Localizacao (inclui uma linha âncora por PROJETO para Fato_Clima)
    loc_inv = (
        df_inv.groupby(["PROJETO", "COD_TALHAO", "TALHAO", "Parcela"], dropna=False, as_index=False)
        .agg({"Area": "first", "AreaCorr": "first"})
    )
    loc_inv["_is_clima_anchor"] = False

    projetos_clima = pd.DataFrame({"PROJETO": sorted(df_clima["PROJETO"].unique())})
    projetos_clima["COD_TALHAO"] = pd.NA
    projetos_clima["TALHAO"] = pd.NA
    projetos_clima["Parcela"] = pd.NA
    projetos_clima["Area"] = pd.NA
    projetos_clima["AreaCorr"] = pd.NA
    projetos_clima["_is_clima_anchor"] = True

    dim_loc_full = (
        pd.concat([loc_inv, projetos_clima], ignore_index=True)
        .sort_values(["PROJETO", "_is_clima_anchor", "COD_TALHAO", "Parcela"], na_position="first")
        .reset_index(drop=True)
    )
    dim_loc_full.insert(0, "SK_Localizacao", np.arange(1, len(dim_loc_full) + 1))
    dim_loc = dim_loc_full.drop(columns=["_is_clima_anchor"])

    # 4. Dim_Silvicultura
    dim_silv = (
        df_inv[["MG", "ESPACAMENTO"]]
        .drop_duplicates()
        .sort_values(["MG", "ESPACAMENTO"])
        .reset_index(drop=True)
    )
    dim_silv.insert(0, "SK_Silvicultura", np.arange(1, len(dim_silv) + 1))

    # 5. Dim_Fuste
    dim_fuste = (
        df_inv[["Cova", "Fuste", "Fila", "Codigos"]]
        .drop_duplicates()
        .sort_values(["Fila", "Cova", "Fuste", "Codigos"])
        .reset_index(drop=True)
    )
    dim_fuste.insert(0, "SK_Fuste", np.arange(1, len(dim_fuste) + 1))

    dim_loc_inv = dim_loc_full[~dim_loc_full["_is_clima_anchor"]][
        ["SK_Localizacao", "PROJETO", "COD_TALHAO", "TALHAO", "Parcela"]
    ]
    dim_loc_proj = dim_loc_full[dim_loc_full["_is_clima_anchor"]][["SK_Localizacao", "PROJETO"]]

    # Fato_Inventario_Fuste
    fato_inv = (
        df_inv.merge(
            dim_tempo,
            left_on=["Data_Medicao", "ANO_IFC", "Ano", "Mes"],
            right_on=["Data_Medicao", "Ano_IFC", "Ano", "Mes"],
            how="left",
        )
        .merge(dim_idade, on=["IDADE", "Idade_Anos", "Fase_Desenvolvimento", "ROT", "CICLO"], how="left")
        .merge(dim_loc_inv, on=["PROJETO", "COD_TALHAO", "TALHAO", "Parcela"], how="left")
        .merge(dim_silv, on=["MG", "ESPACAMENTO"], how="left")
        .merge(dim_fuste, on=["Cova", "Fuste", "Fila", "Codigos"], how="left")
    )[
        [
            "SK_Tempo",
            "SK_Idade",
            "SK_Localizacao",
            "SK_Silvicultura",
            "SK_Fuste",
            "DAP",
            "AltObs",
            "Volume_m3",
        ]
    ]

    # Fato_Clima (usa a linha âncora de projeto em Dim_Localizacao)
    dim_tempo_cli = dim_tempo[dim_tempo["Ano_IFC"].isna()][["SK_Tempo", "Data_Medicao"]]
    fato_clima = (
        df_clima.merge(dim_tempo_cli, on="Data_Medicao", how="inner")
        .merge(dim_loc_proj, on="PROJETO", how="inner")
    )[["SK_Tempo", "SK_Localizacao", "pr", "ET", "RH", "u2", "Rs", "Tmax", "Tmin"]]

    tabelas = [
        ("Dim_Tempo", dim_tempo),
        ("Dim_Idade", dim_idade),
        ("Dim_Localizacao", dim_loc),
        ("Dim_Silvicultura", dim_silv),
        ("Dim_Fuste", dim_fuste),
        ("Fato_Inventario_Fuste", fato_inv),
        ("Fato_Clima", fato_clima),
    ]
    for nome_tabela, df_carga in tabelas:
        con.register("tmp_df", df_carga)
        con.execute(f"INSERT INTO {nome_tabela} SELECT * FROM tmp_df")
        con.unregister("tmp_df")

    contagens = {
        t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t, _ in tabelas
    }
    con.close()
    return contagens


def gerar_relatorio_etl(
    resumo_ifc: dict[str, object],
    contagens_dw: dict[str, int],
    df_ml: pd.DataFrame,
    resumo_ml: dict[str, int],
    caminho_relatorio: Path,
) -> None:
    """Registra resumo executivo e exemplos antes/depois das transformações."""
    linhas_arq = "\n".join(
        f"| `{a['arquivo']}` | {a['linhas']:,} | {a['colunas']} |"
        for a in resumo_ifc["arquivos"]  # type: ignore[index]
    )
    linhas_dw = "\n".join(f"| `{k}` | {v:,} |" for k, v in contagens_dw.items())

    conteudo = f"""# Relatório de Execução e Transformações do ETL (Etapa 2)

## 1. Resumo da Extração (Inventário Florestal Contínuo)

| Arquivo | Linhas Brutas | Colunas |
|---|---:|---:|
{linhas_arq}

- **Total bruto consolidado:** {resumo_ifc['linhas_brutas']:,} registros
- **Registros descartados sem `MedInicio`:** {resumo_ifc['linhas_sem_data']:,}
- **Duplicatas exatas removidas:** {resumo_ifc['duplicatas_removidas']:,}
- **Total carregado em `Fato_Inventario_Fuste`:** {resumo_ifc['linhas_finais']:,} registros
- **Valores nulos em `Volume_m3` (árvores sem `AltObs` medida em campo):** {resumo_ifc['nulos_volume_apos_parse']:,}

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
{linhas_dw}

---

## 4. Dataset Mestre para Machine Learning (`dataset_ml_prognose.csv`)

- **Total de pares válidos de medição (`t1 -> t2`):** {resumo_ml['pares_validos']:,}
- **Pares filtrados por intervalo atípico (`dt_meses` fora de `[6, 24]`) ou sobrevivência `> 120%`:** {resumo_ml['pares_filtrados_outlier']:,}
- **Projetos distintos nos pares:** {df_ml['PROJETO'].nunique() if not df_ml.empty else 0}
- **Total de colunas no dataset enriquecido:** {len(df_ml.columns)}
- **Enriquecimento biométrico (`t1 -> t2`):** `invid2` ($1/I_2$), `razao_idade` ($I_1/I_2$), `dap_min1`, `ht_min1`, `ht_max1`, `ln_ba1`, `ln_volumeha1`, `ln_volumeha2`.
- **Enriquecimento agroclimático:** variáveis no intervalo exato `[t1, t2]` (`pr_acum`, `et_acum`, `balanco_hidrico`, `indice_umidade`, `tmax_media`, `tmin_media`, `tmed_media`, `dtr_media`, `rh_media`, `vpd_media` via Tetens/FAO-56, `rs_media`, `u2_media`, `razao_agua_radiacao`), além dos respectivos blocos anuais (`*_1`, `*_2`), diferenças (`delta_*`) e médias (`media_*`) entre `ano1` e `ano2`.

---

## 5. Dificuldades Encontradas e Soluções Adotadas

1. **Unidade da Evapotranspiração (`ET`) no BR-DWGD:** na coleção `BR-DWGD_Monthly`, a banda `ET` representa a média diária (`mm/dia`), enquanto `pr` representa o acumulado mensal (`mm/mês`). *Solução:* multiplicou-se `ET` por `days_in_month` no ETL para obter `mm/mês` reais e evitar balanço hídrico artificialmente positivo.
2. **Viés de amostragem de altura/volume entre campanhas (`2006–2010` vs `2011–2012`):** nos anos iniciais, apenas ~25–30% das árvores possuíam `AltObs` e `Volume (m³)` medidos; em `2011–2012`, mais de 92% possuíam. *Solução:* na agregação por parcela para ML, o volume total foi expandido proporcionalmente à área basal medida (`vol_medido * (ba_total / ba_com_vol)`).
3. **Heterogeneidade das planilhas anuais:** datas em três formatos distintos (`Timestamp`, serial Excel e texto `DD/MM/YYYY`), abas extras em `2008` (`Plan2`, `Coefs`), erros `#NUM!` em árvores com `DAP=0` e `ESPACAMENTO` presente apenas em 2012. *Solução:* parser unificado (`parse_data_mista`), leitura explícita da aba `Plan1`, coerção segura de numéricos e propagação relacional de espaçamento por `(PROJETO, COD_TALHAO)`.
4. **Variação de `Area`/`AreaCorr` e grafia de projetos entre anos:** pequenas diferenças cadastrais impediam a junção direta na `Dim_Localizacao` e no cruzamento com `Fato_Clima`. *Solução:* normalização canônica de `PROJETO` (`Água Branca`, `Homéro`, `J. Leôncio`) e agregação determinística da dimensão por `(PROJETO, COD_TALHAO, TALHAO, Parcela)`.
"""
    caminho_relatorio.parent.mkdir(parents=True, exist_ok=True)
    caminho_relatorio.write_text(conteudo, encoding="utf-8")


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Etapa Load: popula Data Warehouse DuckDB.")
    parser.add_argument(
        "--staging-dir", type=Path, default=raiz / "dados" / "processados" / "staging"
    )
    parser.add_argument("--schema-sql", type=Path, default=raiz / "dw" / "schema.sql")
    parser.add_argument("--db-path", type=Path, default=raiz / "dw" / "bi_florestal.duckdb")
    parser.add_argument(
        "--ml-csv",
        type=Path,
        default=raiz / "dados" / "processados" / "dataset_ml_prognose.csv",
    )
    parser.add_argument("--report-md", type=Path, default=raiz / "docs" / "relatorio_etl.md")
    args = parser.parse_args()

    df_inv = pd.read_pickle(args.staging_dir / "ifc_tratado.pkl")
    df_clima = pd.read_pickle(args.staging_dir / "clima_tratado.pkl")
    resumo_ifc = json.loads((args.staging_dir / "resumo_ifc.json").read_text(encoding="utf-8"))
    resumo_ml = json.loads((args.staging_dir / "resumo_ml.json").read_text(encoding="utf-8"))
    df_ml = pd.read_csv(args.ml_csv)

    print("[Load] 1/2 Construindo e populando Data Warehouse DuckDB...")
    contagens_dw = construir_e_carregar_dw(df_inv, df_clima, args.schema_sql, args.db_path)

    print("[Load] 2/2 Gerando relatório de auditoria do ETL...")
    gerar_relatorio_etl(resumo_ifc, contagens_dw, df_ml, resumo_ml, args.report_md)
    print(f"[Load] Concluído! DW salvo em {args.db_path}")


if __name__ == "__main__":
    main()
