"""Orquestrador do pipeline de ETL (Etapa 2): Extract -> Transform -> Load.

Permite executar todo o fluxo em um único comando ou rodar cada etapa
isoladamente pelos módulos:
  - etl/extract.py
  - etl/transform.py
  - etl/load.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Garante importação do pacote `etl` tanto via `python etl/run_etl.py` quanto `python -m etl.run_etl`
_RAIZ = Path(__file__).resolve().parent.parent
if str(_RAIZ) not in sys.path:
    sys.path.insert(0, str(_RAIZ))

from etl.extract import extrair_clima_bruto, extrair_ifc_bruto
from etl.load import construir_e_carregar_dw
from etl.transform import gerar_dataset_ml, transformar_clima, transformar_ifc


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa o pipeline completo de ETL (E -> T -> L).")
    parser.add_argument("--brutos-dir", type=Path, default=_RAIZ / "dados_florestais_brutos")
    parser.add_argument(
        "--clima-csv", type=Path, default=_RAIZ / "dados" / "publicos" / "br_dwgd_mensal.csv"
    )
    parser.add_argument("--schema-sql", type=Path, default=_RAIZ / "dw" / "schema.sql")
    parser.add_argument("--db-path", type=Path, default=_RAIZ / "dw" / "bi_florestal.duckdb")
    parser.add_argument(
        "--ml-csv",
        type=Path,
        default=_RAIZ / "dados" / "processados" / "dataset_ml_prognose.csv",
    )
    args = parser.parse_args()

    print("1/3 [Extract] Extraindo planilhas IFC e série climática BR-DWGD...")
    df_ifc_raw, resumo_extract = extrair_ifc_bruto(args.brutos_dir)
    df_clima_raw = extrair_clima_bruto(args.clima_csv)

    print("2/3 [Transform] Padronizando, integrando e enriquecendo dados...")
    df_inv, _ = transformar_ifc(df_ifc_raw, resumo_extract)
    df_clima = transformar_clima(df_clima_raw)
    df_ml, _ = gerar_dataset_ml(df_inv, df_clima, args.ml_csv)

    print("3/3 [Load] Construindo e populando Data Warehouse DuckDB...")
    contagens_dw = construir_e_carregar_dw(df_inv, df_clima, args.schema_sql, args.db_path)

    print(
        f"ETL concluído! DW: {args.db_path} ({contagens_dw['Fato_Inventario_Fuste']:,} fustes, "
        f"{contagens_dw['Fato_Clima']:,} meses climáticos) | "
        f"ML CSV: {args.ml_csv} ({len(df_ml):,} pares, {df_ml.shape[1]} colunas)"
    )


if __name__ == "__main__":
    main()
