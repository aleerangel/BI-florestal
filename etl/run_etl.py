"""Pipeline de ETL reexecutável (Etapa 2): IFC + BR-DWGD -> DuckDB (BI) + CSV (ML).

Etapas executadas:
1. Extração das planilhas brutas do Inventário Florestal Contínuo (2006–2012)
   e da série climática mensal BR-DWGD (extraída via Google Earth Engine).
2. Limpeza, padronização de datas/textos/tipos, deduplicação e cálculo de
   variáveis derivadas (idade, fase de desenvolvimento, área basal, volume/ha).
3. Carga no Data Warehouse DuckDB (dw/bi_florestal.duckdb) seguindo o modelo
   multidimensional em constelação de fatos (5 dimensões + 2 tabelas fato).
4. Pareamento de medições consecutivas (t1 -> t2) e integração climática no
   intervalo exato entre campanhas, exportando o dataset mestre para ML em
   dados/processados/dataset_ml_prognose.csv.
5. Geração de relatório auditável das transformações em docs/relatorio_etl.md.
"""

from __future__ import annotations

import argparse
import glob
import unicodedata
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# Mapeamento canônico para unificar grafias/acentos de PROJETO entre anos e bases
PROJETO_CANONICO: dict[str, str] = {
    "agua branca": "Água Branca",
    "homero": "Homéro",
    "j. leoncio": "J. Leôncio",
}


def normalizar_chave_texto(valor: object) -> str:
    """Remove acentos e normaliza espaços/caixa para comparação de nomes."""
    if pd.isna(valor):
        return ""
    texto = str(valor).strip()
    sem_acento = "".join(
        ch for ch in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(ch)
    )
    return " ".join(sem_acento.lower().split())


def padronizar_projeto(serie: pd.Series) -> pd.Series:
    """Padroniza nomes de projeto preservando grafia canônica."""

    def _fix(val: object) -> str:
        if pd.isna(val):
            return "Não informado"
        limpo = " ".join(str(val).strip().split())
        chave = normalizar_chave_texto(limpo)
        return PROJETO_CANONICO.get(chave, limpo)

    return serie.map(_fix)


def parse_data_mista(serie: pd.Series) -> pd.Series:
    """Converte coluna com datas mistas (datetime, serial Excel ou DD/MM/YYYY)."""
    resultado = pd.Series(pd.NaT, index=serie.index, dtype="datetime64[ns]")

    # 1. Já é datetime/Timestamp
    mask_dt = serie.map(lambda x: isinstance(x, (pd.Timestamp, datetime)))
    if mask_dt.any():
        resultado.loc[mask_dt] = pd.to_datetime(serie.loc[mask_dt], errors="coerce")

    # 2. Serial numérico do Excel (ex: 38393)
    restante = serie.loc[~mask_dt]
    num = pd.to_numeric(restante, errors="coerce")
    mask_num = num.notna()
    if mask_num.any():
        resultado.loc[num.index[mask_num]] = pd.to_datetime(
            num.loc[mask_num], unit="D", origin="1899-12-30", errors="coerce"
        )

    # 3. Strings (ex: '13/02/2005')
    mask_str = (~mask_dt) & (~serie.index.isin(num.index[mask_num]))
    if mask_str.any():
        resultado.loc[mask_str] = pd.to_datetime(
            serie.loc[mask_str].astype(str).str.strip(),
            dayfirst=True,
            errors="coerce",
        )

    return resultado


def classificar_fase(idade_meses: float) -> str:
    """Classifica a fase de desenvolvimento do povoamento pela idade em meses."""
    if pd.isna(idade_meses):
        return "Não informada"
    if idade_meses < 24:
        return "Inicial (0-2 anos)"
    if idade_meses < 48:
        return "Crescimento (2-4 anos)"
    if idade_meses < 72:
        return "Maturação (4-6 anos)"
    return "Corte (6+ anos)"


def extrair_e_padronizar_ifc(pasta_brutos: Path) -> tuple[pd.DataFrame, dict[str, object]]:
    """Lê e padroniza as planilhas anuais do IFC (2006–2012)."""
    arquivos = sorted(glob.glob(str(pasta_brutos / "*.xlsx")))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em {pasta_brutos}")

    frames: list[pd.DataFrame] = []
    stats_arquivos: list[dict[str, object]] = []

    for arq in arquivos:
        df = pd.read_excel(arq, sheet_name="Plan1", engine="openpyxl")
        n_bruto = len(df)
        stats_arquivos.append({"arquivo": Path(arq).name, "linhas": n_bruto, "colunas": df.shape[1]})
        frames.append(df)

    df_raw = pd.concat(frames, ignore_index=True)
    n_total_bruto = len(df_raw)

    df = df_raw.copy()
    df["PROJETO"] = padronizar_projeto(df["PROJETO"])
    df["COD_TALHAO"] = (
        df["COD_TALHAO"]
        .astype(str)
        .str.strip()
        .map(lambda s: s.zfill(3) if s.isdigit() else s)
    )
    df["TALHAO"] = df["TALHAO"].astype(str).str.strip()

    # Datas mistas
    df["DATA_PLANTIO"] = parse_data_mista(df["DATA_PLANTIO"])
    df["Data_Medicao"] = parse_data_mista(df["MedInicio"])

    # Numéricos e códigos
    df["ANO_IFC"] = pd.to_numeric(df["ANO IFC"], errors="coerce").astype("Int64")
    df["ROT"] = pd.to_numeric(df["ROT"], errors="coerce").astype("Int64")
    df["CICLO"] = pd.to_numeric(df["CICLO"], errors="coerce").fillna(1).astype("Int64")
    df["Parcela"] = pd.to_numeric(df["Parcela"], errors="coerce").astype("Int64")
    df["Area"] = pd.to_numeric(df["Area"], errors="coerce")
    df["AreaCorr"] = pd.to_numeric(df["AreaCorr"], errors="coerce")
    df["Cova"] = pd.to_numeric(df["Cova"], errors="coerce").fillna(0).astype("Int64")
    df["Fuste"] = pd.to_numeric(df["Fuste"], errors="coerce").fillna(0).astype("Int64")
    df["Fila"] = pd.to_numeric(df["Fila"], errors="coerce").fillna(0).astype("Int64")

    df["DAP"] = pd.to_numeric(df["DAP"], errors="coerce")
    df["AltObs"] = pd.to_numeric(df["AltObs"], errors="coerce")
    df["Volume_m3"] = pd.to_numeric(df["Volume (m³)"], errors="coerce")

    # Idade recalculada quando ausente ou inválida
    idade_num = pd.to_numeric(df["IDADE"], errors="coerce")
    idade_calc = ((df["Data_Medicao"] - df["DATA_PLANTIO"]).dt.days / 365.25) * 12.0
    df["IDADE"] = idade_num.fillna(idade_calc).round(2)
    df["Idade_Anos"] = (df["IDADE"] // 12).astype("Int64")
    df["Fase_Desenvolvimento"] = df["IDADE"].map(classificar_fase)

    df["MG"] = df["MG"].astype(str).str.strip()
    df["Codigos"] = df["Codigos"].fillna("N").astype(str).str.strip()

    # Espaçamento (presente em 2012): propaga por (PROJETO, COD_TALHAO) quando disponível
    if "ESPACAMENTO" not in df.columns:
        df["ESPACAMENTO"] = pd.NA
    df["ESPACAMENTO"] = df["ESPACAMENTO"].astype("string").str.strip()
    mapa_esp = (
        df.dropna(subset=["ESPACAMENTO"])
        .groupby(["PROJETO", "COD_TALHAO"])["ESPACAMENTO"]
        .first()
    )
    df = df.join(mapa_esp.rename("ESPACAMENTO_MAP"), on=["PROJETO", "COD_TALHAO"])
    df["ESPACAMENTO"] = df["ESPACAMENTO"].fillna(df["ESPACAMENTO_MAP"]).fillna("Não informado")
    df = df.drop(columns=["ESPACAMENTO_MAP"])

    # Remove linhas sem data de medição ou duplicatas exatas
    n_sem_data = int(df["Data_Medicao"].isna().sum())
    df = df.dropna(subset=["Data_Medicao"]).copy()

    n_antes_dedup = len(df)
    df = df.drop_duplicates().copy()
    n_duplicatas = n_antes_dedup - len(df)

    df["Ano"] = df["Data_Medicao"].dt.year.astype("Int64")
    df["Mes"] = df["Data_Medicao"].dt.month.astype("Int64")

    resumo = {
        "arquivos": stats_arquivos,
        "linhas_brutas": n_total_bruto,
        "linhas_sem_data": n_sem_data,
        "duplicatas_removidas": n_duplicatas,
        "linhas_finais": len(df),
        "nulos_volume_apos_parse": int(df["Volume_m3"].isna().sum()),
        "projetos_unicos": int(df["PROJETO"].nunique()),
    }
    return df, resumo


def carregar_clima(caminho_clima: Path) -> pd.DataFrame:
    """Lê a série mensal BR-DWGD, padroniza projeto e converte ET para mm/mês."""
    if not caminho_clima.exists():
        raise FileNotFoundError(f"Arquivo climático não encontrado: {caminho_clima}")
    df_clima = pd.read_csv(caminho_clima)
    df_clima["PROJETO"] = padronizar_projeto(df_clima["PROJETO"])
    df_clima["Data_Medicao"] = pd.to_datetime(df_clima["Data"])
    df_clima["Ano"] = df_clima["Ano"].astype(int)
    df_clima["Mes"] = df_clima["Mes"].astype(int)
    # Na coleção BR-DWGD_Monthly, ET representa média diária (mm/dia);
    # multiplicamos pelos dias do mês para alinhar com pr (mm/mês).
    df_clima["ET"] = pd.to_numeric(df_clima["ET"], errors="coerce") * df_clima["Data_Medicao"].dt.days_in_month
    # Caso dois nomes de projeto tenham sido unificados (ex: Agua Branca / Água Branca),
    # mantém um registro por (PROJETO, Ano, Mes).
    df_clima = (
        df_clima.groupby(["PROJETO", "Data_Medicao", "Ano", "Mes"], as_index=False)[
            ["pr", "ET", "RH", "u2", "Rs", "Tmax", "Tmin"]
        ]
        .mean()
    )
    return df_clima


def construir_e_carregar_dw(
    df_inv: pd.DataFrame,
    df_clima: pd.DataFrame,
    caminho_schema: Path,
    caminho_db: Path,
) -> dict[str, int]:
    """Cria as dimensões, popula o DuckDB e valida integridade referencial."""
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

    # Carga no DuckDB
    for nome_tabela, df_carga in [
        ("Dim_Tempo", dim_tempo),
        ("Dim_Idade", dim_idade),
        ("Dim_Localizacao", dim_loc),
        ("Dim_Silvicultura", dim_silv),
        ("Dim_Fuste", dim_fuste),
        ("Fato_Inventario_Fuste", fato_inv),
        ("Fato_Clima", fato_clima),
    ]:
        con.register("tmp_df", df_carga)
        con.execute(f"INSERT INTO {nome_tabela} SELECT * FROM tmp_df")
        con.unregister("tmp_df")

    contagens = {
        t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        for t in [
            "Dim_Tempo",
            "Dim_Idade",
            "Dim_Localizacao",
            "Dim_Silvicultura",
            "Dim_Fuste",
            "Fato_Inventario_Fuste",
            "Fato_Clima",
        ]
    }
    con.close()
    return contagens


def gerar_dataset_ml(
    df_inv: pd.DataFrame,
    df_clima: pd.DataFrame,
    caminho_saida_ml: Path,
) -> pd.DataFrame:
    """Agrega parcelas, pareia medições (t1 -> t2) e integra clima para ML."""
    df_vivos = df_inv[(df_inv["DAP"] > 0) & (df_inv["AreaCorr"] > 0)].copy()
    df_vivos["g_fuste"] = np.pi * ((df_vivos["DAP"] / 100.0) ** 2) / 4.0

    # Agregação por campanha/parcela
    chaves_parcela = ["PROJETO", "COD_TALHAO", "TALHAO", "Parcela", "Data_Medicao", "ANO_IFC"]
    agreg = (
        df_vivos.groupby(chaves_parcela, as_index=False)
        .agg(
            IDADE=("IDADE", "mean"),
            ROT=("ROT", "first"),
            CICLO=("CICLO", "first"),
            MG=("MG", "first"),
            ESPACAMENTO=("ESPACAMENTO", "first"),
            AreaCorr=("AreaCorr", "first"),
            n_fustes=("DAP", "count"),
            dap_medio=("DAP", "mean"),
            dap_std=("DAP", "std"),
            dap_max=("DAP", "max"),
            alt_media=("AltObs", lambda s: s[s > 0].mean() if (s > 0).any() else np.nan),
            hd=("AltObs", lambda s: s[s > 0].quantile(0.8) if (s > 0).any() else np.nan),
            ba_parcela=("g_fuste", "sum"),
            vol_parcela=("Volume_m3", "sum"),
            n_vol_medido=("Volume_m3", lambda s: s.notna().sum()),
        )
    )

    fator_ha = 10000.0 / agreg["AreaCorr"]
    agreg["volumeha"] = agreg["vol_parcela"] * fator_ha
    agreg["ba"] = agreg["ba_parcela"] * fator_ha
    agreg["nha"] = agreg["n_fustes"] * fator_ha
    agreg["dap_std"] = agreg["dap_std"].fillna(0.0)

    # Mantém apenas parcelas com volume medido válido e ordena cronologicamente
    agreg = agreg[(agreg["n_vol_medido"] > 0) & (agreg["volumeha"] > 0)].sort_values(
        ["PROJETO", "TALHAO", "Parcela", "Data_Medicao"]
    )

    # Pareamento consecutivo t1 -> t2 dentro da mesma parcela e rotação
    pares: list[dict[str, object]] = []
    for (_, _, _), grupo in agreg.groupby(["PROJETO", "TALHAO", "Parcela"]):
        if len(grupo) < 2:
            continue
        grupo = grupo.reset_index(drop=True)
        for i in range(len(grupo) - 1):
            r1 = grupo.iloc[i]
            r2 = grupo.iloc[i + 1]
            dt_meses = float(r2["IDADE"] - r1["IDADE"])
            if dt_meses <= 0 or r2["ROT"] != r1["ROT"]:
                continue

            # Clima agregado exatamente entre Data_Medicao_1 e Data_Medicao_2
            mask_cli = (
                (df_clima["PROJETO"] == r1["PROJETO"])
                & (df_clima["Data_Medicao"] >= r1["Data_Medicao"].replace(day=1))
                & (df_clima["Data_Medicao"] <= r2["Data_Medicao"].replace(day=1))
            )
            cli_sub = df_clima.loc[mask_cli]
            pr_acum = float(cli_sub["pr"].sum()) if not cli_sub.empty else np.nan
            et_acum = float(cli_sub["ET"].sum()) if not cli_sub.empty else np.nan

            ima1 = float(r1["volumeha"] / (r1["IDADE"] / 12.0)) if r1["IDADE"] > 0 else np.nan
            ima2 = float(r2["volumeha"] / (r2["IDADE"] / 12.0)) if r2["IDADE"] > 0 else np.nan
            ica = float((r2["volumeha"] - r1["volumeha"]) / (dt_meses / 12.0))
            sobrevivencia = float((r2["nha"] / r1["nha"]) * 100.0) if r1["nha"] > 0 else np.nan

            pares.append(
                {
                    "PROJETO": r1["PROJETO"],
                    "COD_TALHAO": r1["COD_TALHAO"],
                    "TALHAO": r1["TALHAO"],
                    "Parcela": int(r1["Parcela"]),
                    "grupo_parcela": f"{r1['PROJETO']}_{r1['TALHAO']}_{int(r1['Parcela'])}",
                    "ROT": int(r1["ROT"]),
                    "CICLO": int(r1["CICLO"]),
                    "MG": r1["MG"],
                    "ESPACAMENTO": r1["ESPACAMENTO"],
                    "ano1": int(r1["ANO_IFC"]),
                    "ano2": int(r2["ANO_IFC"]),
                    "idade1": round(float(r1["IDADE"]), 2),
                    "idade2": round(float(r2["IDADE"]), 2),
                    "dt_meses": round(dt_meses, 2),
                    "dap_medio1": round(float(r1["dap_medio"]), 4),
                    "dap_std1": round(float(r1["dap_std"]), 4),
                    "dap_max1": round(float(r1["dap_max"]), 4),
                    "alt_media1": round(float(r1["alt_media"]), 4) if pd.notna(r1["alt_media"]) else np.nan,
                    "hd1": round(float(r1["hd"]), 4) if pd.notna(r1["hd"]) else np.nan,
                    "ba1": round(float(r1["ba"]), 4),
                    "nha1": round(float(r1["nha"]), 2),
                    "volumeha1": round(float(r1["volumeha"]), 4),
                    "ima1": round(ima1, 4),
                    "pr_acum": round(pr_acum, 2) if pd.notna(pr_acum) else np.nan,
                    "et_acum": round(et_acum, 2) if pd.notna(et_acum) else np.nan,
                    "balanco_hidrico": round(pr_acum - et_acum, 2)
                    if pd.notna(pr_acum) and pd.notna(et_acum)
                    else np.nan,
                    "tmax_media": round(float(cli_sub["Tmax"].mean()), 2) if not cli_sub.empty else np.nan,
                    "tmin_media": round(float(cli_sub["Tmin"].mean()), 2) if not cli_sub.empty else np.nan,
                    "rh_media": round(float(cli_sub["RH"].mean()), 2) if not cli_sub.empty else np.nan,
                    "rs_media": round(float(cli_sub["Rs"].mean()), 2) if not cli_sub.empty else np.nan,
                    "u2_media": round(float(cli_sub["u2"].mean()), 2) if not cli_sub.empty else np.nan,
                    "volumeha2": round(float(r2["volumeha"]), 4),
                    "ima2": round(ima2, 4),
                    "ica": round(ica, 4),
                    "sobrevivencia_pct": round(sobrevivencia, 2),
                }
            )

    df_ml = pd.DataFrame(pares)
    caminho_saida_ml.parent.mkdir(parents=True, exist_ok=True)
    df_ml.to_csv(caminho_saida_ml, index=False)
    return df_ml


def gerar_relatorio_etl(
    resumo_ifc: dict[str, object],
    contagens_dw: dict[str, int],
    df_ml: pd.DataFrame,
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

- **Total de pares de medição (`t1 -> t2`):** {len(df_ml):,}
- **Projetos distintos nos pares:** {df_ml['PROJETO'].nunique() if not df_ml.empty else 0}
- **Variáveis climáticas integradas no intervalo exato `[t1, t2]`:** `pr_acum`, `et_acum`, `balanco_hidrico`, `tmax_media`, `tmin_media`, `rh_media`, `rs_media`, `u2_media`.
"""
    caminho_relatorio.parent.mkdir(parents=True, exist_ok=True)
    caminho_relatorio.write_text(conteudo, encoding="utf-8")


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Executa o pipeline completo de ETL.")
    parser.add_argument("--brutos-dir", type=Path, default=raiz / "dados_florestais_brutos")
    parser.add_argument("--clima-csv", type=Path, default=raiz / "dados" / "publicos" / "br_dwgd_mensal.csv")
    parser.add_argument("--schema-sql", type=Path, default=raiz / "dw" / "schema.sql")
    parser.add_argument("--db-path", type=Path, default=raiz / "dw" / "bi_florestal.duckdb")
    parser.add_argument(
        "--ml-csv",
        type=Path,
        default=raiz / "dados" / "processados" / "dataset_ml_prognose.csv",
    )
    parser.add_argument("--report-md", type=Path, default=raiz / "docs" / "relatorio_etl.md")
    args = parser.parse_args()

    print("1/4 Extraindo e padronizando planilhas IFC...")
    df_inv, resumo_ifc = extrair_e_padronizar_ifc(args.brutos_dir)

    print("2/4 Carregando série climática BR-DWGD...")
    df_clima = carregar_clima(args.clima_csv)

    print("3/4 Construindo e populando Data Warehouse DuckDB...")
    contagens_dw = construir_e_carregar_dw(df_inv, df_clima, args.schema_sql, args.db_path)

    print("4/4 Gerando pares de medição e dataset mestre para ML...")
    df_ml = gerar_dataset_ml(df_inv, df_clima, args.ml_csv)

    gerar_relatorio_etl(resumo_ifc, contagens_dw, df_ml, args.report_md)
    print(f"ETL concluído! DW: {args.db_path} | ML CSV: {args.ml_csv} ({len(df_ml)} pares)")


if __name__ == "__main__":
    main()
