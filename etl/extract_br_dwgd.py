"""Extração de variáveis climáticas mensais da base BR-DWGD via Google Earth Engine.

Fonte: ImageCollection 'projects/ee-alexandrexavier/assets/BR-DWGD_Monthly'
(Brazilian Daily Weather Gridded Data - Prof. Alexandre Cândido Xavier / UFES).

Salva a série mensal por projeto em dados/publicos/br_dwgd_mensal.csv para
alimentar tanto a tabela Fato_Clima (DuckDB) quanto o cálculo de indicadores
climáticos entre campanhas de inventário (ETL / ML).
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import ee
import pandas as pd

# Parâmetros de reescalonamento da coleção BR-DWGD_Monthly no GEE
PARAMETROS_VAR: dict[str, dict[str, float | str]] = {
    "pr": {"offset": 750.0, "scale": 0.022888, "agregacao": "sum"},
    "ET": {"offset": -0.0511811, "scale": 0.05118110, "agregacao": "sum"},
    "RH": {"offset": -0.393701, "scale": 0.39370079, "agregacao": "mean"},
    "u2": {"offset": -0.059055, "scale": 0.05905512, "agregacao": "mean"},
    "Rs": {"offset": -0.057087, "scale": 0.15708661, "agregacao": "mean"},
    "Tmax": {"offset": 15.0, "scale": 0.00106815, "agregacao": "mean"},
    "Tmin": {"offset": 15.0, "scale": 0.00106815, "agregacao": "mean"},
}


def inicializar_gee(project: str | None = None) -> None:
    """Inicializa a sessão do Google Earth Engine, autenticando se necessário."""
    ee_project = project or os.getenv("GEE_PROJECT", "kunumi-508220")
    try:
        ee.Initialize(project=ee_project)
    except Exception:
        ee.Authenticate()
        ee.Initialize(project=ee_project)


def extrair_variavel_mensal(
    var_name: str,
    config: dict[str, float | str],
    point_geom: ee.Geometry.Point,
    start_date: str,
    end_date: str,
) -> pd.Series:
    """Extrai e reescalona a série mensal de uma banda para um ponto no GEE."""

    def scale_band(img: ee.Image) -> ee.Image:
        return (
            img.multiply(float(config["scale"]))
            .add(float(config["offset"]))
            .copyProperties(img, img.propertyNames())
        )

    colecao = (
        ee.ImageCollection("projects/ee-alexandrexavier/assets/BR-DWGD_Monthly")
        .filterDate(start_date, end_date)
        .select(var_name)
        .map(scale_band)
    )

    dados_regiao = colecao.getRegion(point_geom, scale=10000).getInfo()
    if not dados_regiao or len(dados_regiao) <= 1:
        return pd.Series(dtype=float, name=var_name)

    df_var = pd.DataFrame(dados_regiao[1:], columns=dados_regiao[0])
    df_var["Data"] = pd.to_datetime(df_var["id"].str[-8:], format="%Y%m%d")
    df_var[var_name] = pd.to_numeric(df_var[var_name], errors="coerce")

    # Garante um registro por mês caso haja sobreposição de imagens na grade
    serie_mensal = df_var.groupby("Data")[var_name].mean().sort_index()
    serie_mensal.name = var_name
    return serie_mensal


def extrair_br_dwgd(
    caminho_coords: Path,
    caminho_saida: Path,
    start_date: str = "2006-01-01",
    end_date: str = "2013-01-01",
    project: str | None = None,
) -> pd.DataFrame:
    """Extrai todas as variáveis BR-DWGD mensais para os pontos informados."""
    if not caminho_coords.exists():
        raise FileNotFoundError(f"Arquivo de coordenadas não encontrado: {caminho_coords}")

    inicializar_gee(project=project)

    df_pontos = pd.read_csv(caminho_coords)
    colunas_esperadas = {"PROJETO", "LATITUDE", "LONGITUDE"}
    if not colunas_esperadas.issubset(df_pontos.columns):
        raise ValueError(
            f"CSV de coordenadas deve conter as colunas {sorted(colunas_esperadas)}. "
            f"Encontradas: {list(df_pontos.columns)}"
        )

    registros_finais: list[pd.DataFrame] = []
    cache_coords: dict[tuple[float, float], pd.DataFrame] = {}
    total = len(df_pontos)

    for idx, row in df_pontos.iterrows():
        projeto = str(row["PROJETO"]).strip()
        lat = float(row["LATITUDE"])
        lon = float(row["LONGITUDE"])
        chave_coord = (lat, lon)

        if chave_coord in cache_coords:
            print(f"Reaproveitando cache [{idx + 1}/{total}]: {projeto} ({lat}, {lon})")
            df_ponto = cache_coords[chave_coord].copy()
        else:
            print(f"Consultando GEE [{idx + 1}/{total}]: {projeto} ({lat}, {lon})...")
            ponto = ee.Geometry.Point([lon, lat])
            dados_ponto = {
                var_name: extrair_variavel_mensal(var_name, config, ponto, start_date, end_date)
                for var_name, config in PARAMETROS_VAR.items()
            }
            df_ponto = pd.DataFrame(dados_ponto).reset_index()
            df_ponto["Ano"] = df_ponto["Data"].dt.year
            df_ponto["Mes"] = df_ponto["Data"].dt.month
            df_ponto["Data"] = df_ponto["Data"].dt.strftime("%Y-%m-%d")
            cache_coords[chave_coord] = df_ponto.copy()

        df_ponto.insert(0, "PROJETO", projeto)
        colunas_ordenadas = ["PROJETO", "Data", "Ano", "Mes", *PARAMETROS_VAR.keys()]
        registros_finais.append(df_ponto[colunas_ordenadas])

    df_final = pd.concat(registros_finais, ignore_index=True)
    caminho_saida.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_csv(caminho_saida, index=False)
    print(f"Extração concluída: {len(df_final)} registros salvos em {caminho_saida}")
    return df_final


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Extração mensal BR-DWGD via GEE")
    parser.add_argument(
        "--coords",
        type=Path,
        default=raiz / "dados" / "publicos" / "projetos_coordenadas.csv",
        help="Caminho para o CSV com PROJETO, LATITUDE, LONGITUDE.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=raiz / "dados" / "publicos" / "br_dwgd_mensal.csv",
        help="Caminho de saída do CSV mensal extraído.",
    )
    parser.add_argument("--start-date", default="2006-01-01")
    parser.add_argument("--end-date", default="2013-01-01")
    parser.add_argument("--project", default=None, help="ID do projeto no Google Earth Engine.")
    args = parser.parse_args()

    extrair_br_dwgd(
        caminho_coords=args.coords,
        caminho_saida=args.output,
        start_date=args.start_date,
        end_date=args.end_date,
        project=args.project,
    )


if __name__ == "__main__":
    main()
