"""Etapa 3 do ETL (Load): carga do Data Warehouse DuckDB."""

from __future__ import annotations

import argparse
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


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Etapa Load: popula Data Warehouse DuckDB.")
    parser.add_argument(
        "--staging-dir", type=Path, default=raiz / "dados" / "processados" / "staging"
    )
    parser.add_argument("--schema-sql", type=Path, default=raiz / "dw" / "schema.sql")
    parser.add_argument("--db-path", type=Path, default=raiz / "dw" / "bi_florestal.duckdb")
    args = parser.parse_args()

    df_inv = pd.read_pickle(args.staging_dir / "ifc_tratado.pkl")
    df_clima = pd.read_pickle(args.staging_dir / "clima_tratado.pkl")

    print("[Load] Construindo e populando Data Warehouse DuckDB...")
    contagens_dw = construir_e_carregar_dw(df_inv, df_clima, args.schema_sql, args.db_path)
    resumo_tabelas = ", ".join(f"{k}: {v:,}" for k, v in contagens_dw.items())
    print(f"[Load] Concluído! DW salvo em {args.db_path} ({resumo_tabelas})")


if __name__ == "__main__":
    main()
