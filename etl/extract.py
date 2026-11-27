"""Etapa 1 do ETL (Extract): extração das planilhas brutas IFC e leitura do BR-DWGD."""

from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import pandas as pd


def extrair_ifc_bruto(pasta_brutos: Path) -> tuple[pd.DataFrame, dict[str, object]]:
    """Lê as planilhas anuais do IFC (2006–2012) preservando os dados brutos."""
    arquivos = sorted(glob.glob(str(pasta_brutos / "*.xlsx")))
    if not arquivos:
        raise FileNotFoundError(f"Nenhum arquivo .xlsx encontrado em {pasta_brutos}")

    frames: list[pd.DataFrame] = []
    stats_arquivos: list[dict[str, object]] = []

    for arq in arquivos:
        df = pd.read_excel(arq, sheet_name="Plan1", engine="openpyxl")
        stats_arquivos.append(
            {"arquivo": Path(arq).name, "linhas": len(df), "colunas": df.shape[1]}
        )
        frames.append(df)

    df_raw = pd.concat(frames, ignore_index=True)
    resumo: dict[str, object] = {
        "arquivos": stats_arquivos,
        "linhas_brutas": len(df_raw),
    }
    return df_raw, resumo


def extrair_clima_bruto(caminho_clima: Path) -> pd.DataFrame:
    """Lê a série mensal BR-DWGD previamente extraída do Google Earth Engine."""
    if not caminho_clima.exists():
        raise FileNotFoundError(f"Arquivo climático não encontrado: {caminho_clima}")
    return pd.read_csv(caminho_clima)


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Etapa Extract: extrai IFC e BR-DWGD para staging.")
    parser.add_argument("--brutos-dir", type=Path, default=raiz / "dados_florestais_brutos")
    parser.add_argument(
        "--clima-csv", type=Path, default=raiz / "dados" / "publicos" / "br_dwgd_mensal.csv"
    )
    parser.add_argument(
        "--staging-dir", type=Path, default=raiz / "dados" / "processados" / "staging"
    )
    args = parser.parse_args()

    print("[Extract] 1/2 Lendo planilhas brutas do IFC...")
    df_ifc_raw, resumo_extract = extrair_ifc_bruto(args.brutos_dir)

    print("[Extract] 2/2 Lendo série climática BR-DWGD...")
    df_clima_raw = extrair_clima_bruto(args.clima_csv)

    args.staging_dir.mkdir(parents=True, exist_ok=True)
    df_ifc_raw.to_pickle(args.staging_dir / "ifc_bruto.pkl")
    df_clima_raw.to_pickle(args.staging_dir / "clima_bruto.pkl")
    (args.staging_dir / "resumo_extract.json").write_text(
        json.dumps(resumo_extract, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        f"[Extract] Concluído! IFC bruto: {len(df_ifc_raw):,} linhas | "
        f"Clima bruto: {len(df_clima_raw):,} linhas -> {args.staging_dir}"
    )


if __name__ == "__main__":
    main()
