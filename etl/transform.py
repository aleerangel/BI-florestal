"""Etapa 2 do ETL (Transform): limpeza, padronização, integração e enriquecimento."""

from __future__ import annotations

import argparse
import json
import unicodedata
from datetime import datetime
from pathlib import Path

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

    mask_dt = serie.map(lambda x: isinstance(x, (pd.Timestamp, datetime)))
    if mask_dt.any():
        resultado.loc[mask_dt] = pd.to_datetime(serie.loc[mask_dt], errors="coerce")

    restante = serie.loc[~mask_dt]
    num = pd.to_numeric(restante, errors="coerce")
    mask_num = num.notna()
    if mask_num.any():
        resultado.loc[num.index[mask_num]] = pd.to_datetime(
            num.loc[mask_num], unit="D", origin="1899-12-30", errors="coerce"
        )

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


def transformar_ifc(
    df_raw: pd.DataFrame, resumo_extract: dict[str, object]
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Padroniza tipos, datas, textos, remove duplicatas e calcula atributos derivados do IFC."""
    df = df_raw.copy()
    df["PROJETO"] = padronizar_projeto(df["PROJETO"])
    df["COD_TALHAO"] = (
        df["COD_TALHAO"]
        .astype(str)
        .str.strip()
        .map(lambda s: s.zfill(3) if s.isdigit() else s)
    )
    df["TALHAO"] = df["TALHAO"].astype(str).str.strip()

    df["DATA_PLANTIO"] = parse_data_mista(df["DATA_PLANTIO"])
    df["Data_Medicao"] = parse_data_mista(df["MedInicio"])

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

    idade_num = pd.to_numeric(df["IDADE"], errors="coerce")
    idade_calc = ((df["Data_Medicao"] - df["DATA_PLANTIO"]).dt.days / 365.25) * 12.0
    df["IDADE"] = idade_num.fillna(idade_calc).round(2)
    df["Idade_Anos"] = (df["IDADE"] // 12).astype("Int64")
    df["Fase_Desenvolvimento"] = df["IDADE"].map(classificar_fase)

    df["MG"] = df["MG"].astype(str).str.strip()
    df["Codigos"] = df["Codigos"].fillna("N").astype(str).str.strip()

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

    n_sem_data = int(df["Data_Medicao"].isna().sum())
    df = df.dropna(subset=["Data_Medicao"]).copy()

    n_antes_dedup = len(df)
    df = df.drop_duplicates().copy()
    n_duplicatas = n_antes_dedup - len(df)

    df["Ano"] = df["Data_Medicao"].dt.year.astype("Int64")
    df["Mes"] = df["Data_Medicao"].dt.month.astype("Int64")

    resumo = {
        **resumo_extract,
        "linhas_sem_data": n_sem_data,
        "duplicatas_removidas": n_duplicatas,
        "linhas_finais": len(df),
        "nulos_volume_apos_parse": int(df["Volume_m3"].isna().sum()),
        "projetos_unicos": int(df["PROJETO"].nunique()),
    }
    return df, resumo


def transformar_clima(df_clima_raw: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de projeto, converte ET de mm/dia para mm/mês e consolida grão mensal."""
    df_clima = df_clima_raw.copy()
    df_clima["PROJETO"] = padronizar_projeto(df_clima["PROJETO"])
    df_clima["Data_Medicao"] = pd.to_datetime(df_clima["Data"])
    df_clima["Ano"] = df_clima["Ano"].astype(int)
    df_clima["Mes"] = df_clima["Mes"].astype(int)
    # Na coleção BR-DWGD_Monthly, ET representa média diária (mm/dia);
    # multiplicamos pelos dias do mês para alinhar com pr (mm/mês).
    df_clima["ET"] = (
        pd.to_numeric(df_clima["ET"], errors="coerce") * df_clima["Data_Medicao"].dt.days_in_month
    )
    return (
        df_clima.groupby(["PROJETO", "Data_Medicao", "Ano", "Mes"], as_index=False)[
            ["pr", "ET", "RH", "u2", "Rs", "Tmax", "Tmin"]
        ]
        .mean()
    )


def _pressao_saturacao_tetens(temp_c: float) -> float:
    """Calcula pressão de saturação de vapor (kPa) pela equação de Tetens (FAO-56)."""
    return float(0.6108 * np.exp((17.27 * temp_c) / (temp_c + 237.3)))


def _resumir_bloco_climatico(cli_sub: pd.DataFrame) -> dict[str, float]:
    """Agrega variáveis climáticas de um recorte e deriva índices agroclimáticos."""
    chaves = (
        "pr",
        "ET",
        "RH",
        "u2",
        "Rs",
        "Tmax",
        "Tmin",
        "balanco_hidrico",
        "indice_umidade",
        "tmed",
        "dtr",
        "vpd",
        "razao_agua_radiacao",
    )
    if cli_sub.empty:
        return {k: np.nan for k in chaves}

    pr = float(cli_sub["pr"].sum())
    et = float(cli_sub["ET"].sum())
    rh = float(cli_sub["RH"].mean())
    u2 = float(cli_sub["u2"].mean())
    rs = float(cli_sub["Rs"].mean())
    tmax = float(cli_sub["Tmax"].mean())
    tmin = float(cli_sub["Tmin"].mean())

    tmed = (tmax + tmin) / 2.0
    dtr = tmax - tmin
    es_medio = (_pressao_saturacao_tetens(tmax) + _pressao_saturacao_tetens(tmin)) / 2.0
    vpd = es_medio * (1.0 - (rh / 100.0))

    return {
        "pr": pr,
        "ET": et,
        "RH": rh,
        "u2": u2,
        "Rs": rs,
        "Tmax": tmax,
        "Tmin": tmin,
        "balanco_hidrico": pr - et,
        "indice_umidade": (pr / et) if et > 0 else np.nan,
        "tmed": tmed,
        "dtr": dtr,
        "vpd": vpd,
        "razao_agua_radiacao": (pr / rs) if rs > 0 else np.nan,
    }


def gerar_dataset_ml(
    df_inv: pd.DataFrame,
    df_clima: pd.DataFrame,
    caminho_saida_ml: Path,
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Agrega parcelas, corrige viés de volume, pareia (t1->t2) e enriquece com índices."""
    df_vivos = df_inv[(df_inv["DAP"] > 0) & (df_inv["AreaCorr"] > 0)].copy()
    df_vivos["g_fuste"] = np.pi * ((df_vivos["DAP"] / 100.0) ** 2) / 4.0
    df_vivos["g_com_vol"] = np.where(df_vivos["Volume_m3"].notna(), df_vivos["g_fuste"], 0.0)

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
            dap_min=("DAP", "min"),
            dap_max=("DAP", "max"),
            alt_media=("AltObs", lambda s: s[s > 0].mean() if (s > 0).any() else np.nan),
            ht_min=("AltObs", lambda s: s[s > 0].min() if (s > 0).any() else np.nan),
            ht_max=("AltObs", lambda s: s[s > 0].max() if (s > 0).any() else np.nan),
            hd=("AltObs", lambda s: s[s > 0].quantile(0.8) if (s > 0).any() else np.nan),
            ba_parcela=("g_fuste", "sum"),
            ba_com_vol=("g_com_vol", "sum"),
            vol_parcela_medido=("Volume_m3", "sum"),
            n_vol_medido=("Volume_m3", lambda s: s.notna().sum()),
        )
    )

    agreg["vol_parcela"] = np.where(
        agreg["ba_com_vol"] > 0,
        agreg["vol_parcela_medido"] * (agreg["ba_parcela"] / agreg["ba_com_vol"]),
        np.nan,
    )

    fator_ha = 10000.0 / agreg["AreaCorr"]
    agreg["volumeha"] = agreg["vol_parcela"] * fator_ha
    agreg["ba"] = agreg["ba_parcela"] * fator_ha
    agreg["nha"] = agreg["n_fustes"] * fator_ha
    agreg["dap_std"] = agreg["dap_std"].fillna(0.0)

    agreg = agreg[(agreg["n_vol_medido"] > 0) & (agreg["volumeha"] > 0)].sort_values(
        ["PROJETO", "TALHAO", "Parcela", "Data_Medicao"]
    )

    clima_por_projeto = {proj: grp for proj, grp in df_clima.groupby("PROJETO")}

    pares: list[dict[str, object]] = []
    pares_filtrados_outlier = 0
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

            sobrevivencia = float((r2["nha"] / r1["nha"]) * 100.0) if r1["nha"] > 0 else np.nan
            if not (6.0 <= dt_meses <= 24.0) or (pd.notna(sobrevivencia) and sobrevivencia > 120.0):
                pares_filtrados_outlier += 1
                continue

            cli_proj = clima_por_projeto.get(r1["PROJETO"], df_clima.iloc[0:0])
            ano1, ano2 = int(r1["ANO_IFC"]), int(r2["ANO_IFC"])

            mask_int = (cli_proj["Data_Medicao"] >= r1["Data_Medicao"].replace(day=1)) & (
                cli_proj["Data_Medicao"] <= r2["Data_Medicao"].replace(day=1)
            )
            cli_int = _resumir_bloco_climatico(cli_proj.loc[mask_int])
            cli_a1 = _resumir_bloco_climatico(cli_proj[cli_proj["Ano"] == ano1])
            cli_a2 = _resumir_bloco_climatico(cli_proj[cli_proj["Ano"] == ano2])

            ima1 = float(r1["volumeha"] / (r1["IDADE"] / 12.0)) if r1["IDADE"] > 0 else np.nan
            ima2 = float(r2["volumeha"] / (r2["IDADE"] / 12.0)) if r2["IDADE"] > 0 else np.nan
            ica = float((r2["volumeha"] - r1["volumeha"]) / (dt_meses / 12.0))

            reg: dict[str, object] = {
                "PROJETO": r1["PROJETO"],
                "COD_TALHAO": r1["COD_TALHAO"],
                "TALHAO": r1["TALHAO"],
                "Parcela": int(r1["Parcela"]),
                "grupo_parcela": f"{r1['PROJETO']}_{r1['TALHAO']}_{int(r1['Parcela'])}",
                "ROT": int(r1["ROT"]),
                "CICLO": int(r1["CICLO"]),
                "MG": r1["MG"],
                "ESPACAMENTO": r1["ESPACAMENTO"],
                "ano1": ano1,
                "ano2": ano2,
                "idade1": round(float(r1["IDADE"]), 2),
                "idade2": round(float(r2["IDADE"]), 2),
                "dt_meses": round(dt_meses, 2),
                "invid2": round(1.0 / float(r2["IDADE"]), 6) if r2["IDADE"] > 0 else np.nan,
                "razao_idade": round(float(r1["IDADE"] / r2["IDADE"]), 6) if r2["IDADE"] > 0 else np.nan,
                "dap_medio1": round(float(r1["dap_medio"]), 4),
                "dap_std1": round(float(r1["dap_std"]), 4),
                "dap_min1": round(float(r1["dap_min"]), 4),
                "dap_max1": round(float(r1["dap_max"]), 4),
                "alt_media1": round(float(r1["alt_media"]), 4) if pd.notna(r1["alt_media"]) else np.nan,
                "ht_min1": round(float(r1["ht_min"]), 4) if pd.notna(r1["ht_min"]) else np.nan,
                "ht_max1": round(float(r1["ht_max"]), 4) if pd.notna(r1["ht_max"]) else np.nan,
                "hd1": round(float(r1["hd"]), 4) if pd.notna(r1["hd"]) else np.nan,
                "ba1": round(float(r1["ba"]), 4),
                "ln_ba1": round(float(np.log(r1["ba"])), 6) if r1["ba"] > 0 else np.nan,
                "nha1": round(float(r1["nha"]), 2),
                "volumeha1": round(float(r1["volumeha"]), 4),
                "ln_volumeha1": round(float(np.log(r1["volumeha"])), 6) if r1["volumeha"] > 0 else np.nan,
                "ima1": round(ima1, 4),
                "pr_acum": round(cli_int["pr"], 2) if pd.notna(cli_int["pr"]) else np.nan,
                "et_acum": round(cli_int["ET"], 2) if pd.notna(cli_int["ET"]) else np.nan,
                "balanco_hidrico": round(cli_int["balanco_hidrico"], 2)
                if pd.notna(cli_int["balanco_hidrico"])
                else np.nan,
                "indice_umidade": round(cli_int["indice_umidade"], 4)
                if pd.notna(cli_int["indice_umidade"])
                else np.nan,
                "tmax_media": round(cli_int["Tmax"], 2) if pd.notna(cli_int["Tmax"]) else np.nan,
                "tmin_media": round(cli_int["Tmin"], 2) if pd.notna(cli_int["Tmin"]) else np.nan,
                "tmed_media": round(cli_int["tmed"], 2) if pd.notna(cli_int["tmed"]) else np.nan,
                "dtr_media": round(cli_int["dtr"], 2) if pd.notna(cli_int["dtr"]) else np.nan,
                "rh_media": round(cli_int["RH"], 2) if pd.notna(cli_int["RH"]) else np.nan,
                "vpd_media": round(cli_int["vpd"], 4) if pd.notna(cli_int["vpd"]) else np.nan,
                "rs_media": round(cli_int["Rs"], 2) if pd.notna(cli_int["Rs"]) else np.nan,
                "u2_media": round(cli_int["u2"], 2) if pd.notna(cli_int["u2"]) else np.nan,
                "razao_agua_radiacao": round(cli_int["razao_agua_radiacao"], 2)
                if pd.notna(cli_int["razao_agua_radiacao"])
                else np.nan,
            }

            for var in (
                "pr",
                "ET",
                "RH",
                "u2",
                "Rs",
                "Tmax",
                "Tmin",
                "balanco_hidrico",
                "indice_umidade",
                "tmed",
                "dtr",
                "vpd",
                "razao_agua_radiacao",
            ):
                v1, v2 = cli_a1[var], cli_a2[var]
                reg[f"{var}_1"] = round(v1, 4) if pd.notna(v1) else np.nan
                reg[f"{var}_2"] = round(v2, 4) if pd.notna(v2) else np.nan
                reg[f"delta_{var}"] = round(v2 - v1, 4) if pd.notna(v1) and pd.notna(v2) else np.nan
                reg[f"media_{var}"] = (
                    round((v1 + v2) / 2.0, 4) if pd.notna(v1) and pd.notna(v2) else np.nan
                )

            reg["volumeha2"] = round(float(r2["volumeha"]), 4)
            reg["ln_volumeha2"] = round(float(np.log(r2["volumeha"])), 6) if r2["volumeha"] > 0 else np.nan
            reg["ima2"] = round(ima2, 4)
            reg["ica"] = round(ica, 4)
            reg["sobrevivencia_pct"] = round(sobrevivencia, 2)
            pares.append(reg)

    df_ml = pd.DataFrame(pares)
    caminho_saida_ml.parent.mkdir(parents=True, exist_ok=True)
    df_ml.to_csv(caminho_saida_ml, index=False)
    resumo_ml = {
        "pares_validos": len(df_ml),
        "pares_filtrados_outlier": pares_filtrados_outlier,
    }
    return df_ml, resumo_ml


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Etapa Transform: limpa, integra e enriquece dados.")
    parser.add_argument(
        "--staging-dir", type=Path, default=raiz / "dados" / "processados" / "staging"
    )
    parser.add_argument(
        "--ml-csv",
        type=Path,
        default=raiz / "dados" / "processados" / "dataset_ml_prognose.csv",
    )
    args = parser.parse_args()

    df_ifc_raw = pd.read_pickle(args.staging_dir / "ifc_bruto.pkl")
    df_clima_raw = pd.read_pickle(args.staging_dir / "clima_bruto.pkl")
    resumo_extract = json.loads((args.staging_dir / "resumo_extract.json").read_text(encoding="utf-8"))

    print("[Transform] 1/3 Padronizando inventário florestal (IFC)...")
    df_inv, resumo_ifc = transformar_ifc(df_ifc_raw, resumo_extract)

    print("[Transform] 2/3 Padronizando série climática BR-DWGD (ET mm/dia -> mm/mês)...")
    df_clima = transformar_clima(df_clima_raw)

    print("[Transform] 3/3 Gerando pares t1->t2 e enriquecendo dataset de prognose...")
    df_ml, resumo_ml = gerar_dataset_ml(df_inv, df_clima, args.ml_csv)

    df_inv.to_pickle(args.staging_dir / "ifc_tratado.pkl")
    df_clima.to_pickle(args.staging_dir / "clima_tratado.pkl")
    (args.staging_dir / "resumo_ifc.json").write_text(
        json.dumps(resumo_ifc, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (args.staging_dir / "resumo_ml.json").write_text(
        json.dumps(resumo_ml, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        f"[Transform] Concluído! IFC tratado: {len(df_inv):,} fustes | "
        f"Clima: {len(df_clima):,} meses | ML: {len(df_ml):,} pares ({df_ml.shape[1]} colunas)"
    )


if __name__ == "__main__":
    main()
