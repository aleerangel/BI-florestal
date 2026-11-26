"""Validação do Data Warehouse DuckDB (integridade referencial e KPIs base)."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def validar_dw(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Banco DuckDB não encontrado: {db_path}")

    con = duckdb.connect(str(db_path), read_only=True)

    print("=== 1. Contagem de Registros ===")
    tabelas = [
        "Dim_Tempo",
        "Dim_Idade",
        "Dim_Localizacao",
        "Dim_Silvicultura",
        "Dim_Fuste",
        "Fato_Inventario_Fuste",
        "Fato_Clima",
    ]
    for t in tabelas:
        qtd = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t:25s}: {qtd:>10,d}")

    print("\n=== 2. Integridade Referencial (Chaves Órfãs) ===")
    checks = [
        ("Fato_Inventario_Fuste -> Dim_Tempo", "SELECT COUNT(*) FROM Fato_Inventario_Fuste f LEFT JOIN Dim_Tempo d ON f.SK_Tempo = d.SK_Tempo WHERE d.SK_Tempo IS NULL"),
        ("Fato_Inventario_Fuste -> Dim_Idade", "SELECT COUNT(*) FROM Fato_Inventario_Fuste f LEFT JOIN Dim_Idade d ON f.SK_Idade = d.SK_Idade WHERE d.SK_Idade IS NULL"),
        ("Fato_Inventario_Fuste -> Dim_Localizacao", "SELECT COUNT(*) FROM Fato_Inventario_Fuste f LEFT JOIN Dim_Localizacao d ON f.SK_Localizacao = d.SK_Localizacao WHERE d.SK_Localizacao IS NULL"),
        ("Fato_Inventario_Fuste -> Dim_Silvicultura", "SELECT COUNT(*) FROM Fato_Inventario_Fuste f LEFT JOIN Dim_Silvicultura d ON f.SK_Silvicultura = d.SK_Silvicultura WHERE d.SK_Silvicultura IS NULL"),
        ("Fato_Inventario_Fuste -> Dim_Fuste", "SELECT COUNT(*) FROM Fato_Inventario_Fuste f LEFT JOIN Dim_Fuste d ON f.SK_Fuste = d.SK_Fuste WHERE d.SK_Fuste IS NULL"),
        ("Fato_Clima -> Dim_Tempo", "SELECT COUNT(*) FROM Fato_Clima f LEFT JOIN Dim_Tempo d ON f.SK_Tempo = d.SK_Tempo WHERE d.SK_Tempo IS NULL"),
        ("Fato_Clima -> Dim_Localizacao", "SELECT COUNT(*) FROM Fato_Clima f LEFT JOIN Dim_Localizacao d ON f.SK_Localizacao = d.SK_Localizacao WHERE d.SK_Localizacao IS NULL"),
    ]
    for nome, sql in checks:
        orfaos = con.execute(sql).fetchone()[0]
        status = "OK" if orfaos == 0 else f"ERRO ({orfaos} órfãos)"
        print(f"  {nome:42s}: {status}")
        if orfaos > 0:
            raise AssertionError(f"Integridade violada em {nome}: {orfaos} órfãos")

    print("\n=== 3. Conferência de KPIs por Ano IFC ===")
    kpis_ano = con.execute(
        """
        SELECT
            t.Ano_IFC,
            COUNT(*) AS n_fustes,
            ROUND(AVG(f.DAP), 2) AS dap_medio,
            ROUND(AVG(CASE WHEN f.AltObs > 0 THEN f.AltObs END), 2) AS alt_media,
            ROUND(SUM(COALESCE(f.Volume_m3, 0)), 2) AS vol_medido_total
        FROM Fato_Inventario_Fuste f
        JOIN Dim_Tempo t ON f.SK_Tempo = t.SK_Tempo
        GROUP BY t.Ano_IFC
        ORDER BY t.Ano_IFC
        """
    ).fetchdf()
    print(kpis_ano.to_string(index=False))

    print("\n=== 4. Balanço Hídrico Médio Anual (BR-DWGD) ===")
    clima_ano = con.execute(
        """
        WITH proj_ano AS (
            SELECT
                l.PROJETO,
                t.Ano,
                SUM(c.pr) AS pr_anual,
                SUM(c.ET) AS et_anual,
                SUM(c.pr - c.ET) AS balanco_hidrico,
                AVG(c.Tmax) AS tmax_media,
                AVG(c.Tmin) AS tmin_media
            FROM Fato_Clima c
            JOIN Dim_Tempo t ON c.SK_Tempo = t.SK_Tempo
            JOIN Dim_Localizacao l ON c.SK_Localizacao = l.SK_Localizacao
            GROUP BY l.PROJETO, t.Ano
        )
        SELECT
            Ano,
            COUNT(*) AS n_projetos,
            ROUND(AVG(pr_anual), 1) AS pr_media_mm,
            ROUND(AVG(et_anual), 1) AS et_media_mm,
            ROUND(AVG(balanco_hidrico), 1) AS balanco_medio_mm,
            ROUND(AVG(tmax_media), 2) AS tmax_media,
            ROUND(AVG(tmin_media), 2) AS tmin_media
        FROM proj_ano
        GROUP BY Ano
        ORDER BY Ano
        """
    ).fetchdf()
    print(clima_ano.to_string(index=False))
    con.close()


def main() -> None:
    raiz = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Valida o Data Warehouse DuckDB.")
    parser.add_argument("--db-path", type=Path, default=raiz / "dw" / "bi_florestal.duckdb")
    args = parser.parse_args()
    validar_dw(args.db_path)


if __name__ == "__main__":
    main()
