
-- 1. Auditoria de Carga: contagem de registros em todas as dimensões e fatos
SELECT 'Dim_Tempo' AS tabela, COUNT(*) AS registros FROM Dim_Tempo
UNION ALL SELECT 'Dim_Idade', COUNT(*) FROM Dim_Idade
UNION ALL SELECT 'Dim_Localizacao', COUNT(*) FROM Dim_Localizacao
UNION ALL SELECT 'Dim_Silvicultura', COUNT(*) FROM Dim_Silvicultura
UNION ALL SELECT 'Dim_Fuste', COUNT(*) FROM Dim_Fuste
UNION ALL SELECT 'Fato_Inventario_Fuste', COUNT(*) FROM Fato_Inventario_Fuste
UNION ALL SELECT 'Fato_Clima', COUNT(*) FROM Fato_Clima;


-- 2. Evolução Anual do Inventário Florestal (2006–2012)
SELECT
    t.Ano_IFC,
    COUNT(DISTINCT l.PROJETO) AS n_projetos,
    COUNT(*) AS n_fustes,
    ROUND(AVG(CASE WHEN f.DAP > 0 THEN f.DAP END), 2) AS dap_medio_cm,
    ROUND(AVG(CASE WHEN f.AltObs > 0 THEN f.AltObs END), 2) AS alt_media_m,
    ROUND(SUM(COALESCE(f.Volume_m3, 0)), 2) AS volume_medido_m3
FROM Fato_Inventario_Fuste f
JOIN Dim_Tempo t ON f.SK_Tempo = t.SK_Tempo
JOIN Dim_Localizacao l ON f.SK_Localizacao = l.SK_Localizacao
GROUP BY t.Ano_IFC
ORDER BY t.Ano_IFC;


-- 3. Desempenho por Fase de Desenvolvimento e Rotação
SELECT
    i.ROT AS rotacao,
    i.Fase_Desenvolvimento,
    COUNT(*) AS n_fustes,
    ROUND(AVG(i.IDADE), 1) AS idade_media_meses,
    ROUND(AVG(CASE WHEN f.DAP > 0 THEN f.DAP END), 2) AS dap_medio_cm,
    ROUND(AVG(CASE WHEN f.AltObs > 0 THEN f.AltObs END), 2) AS alt_media_m
FROM Fato_Inventario_Fuste f
JOIN Dim_Idade i ON f.SK_Idade = i.SK_Idade
GROUP BY i.ROT, i.Fase_Desenvolvimento
ORDER BY i.ROT, idade_media_meses;


-- 4. Top 10 Materiais Genéticos (Clones) por Volume Médio por Fuste
SELECT
    s.MG AS material_genetico,
    s.ESPACAMENTO,
    COUNT(*) AS n_fustes,
    ROUND(AVG(i.IDADE), 1) AS idade_media_meses,
    ROUND(AVG(CASE WHEN f.DAP > 0 THEN f.DAP END), 2) AS dap_medio_cm,
    ROUND(AVG(f.Volume_m3), 4) AS volume_medio_fuste_m3
FROM Fato_Inventario_Fuste f
JOIN Dim_Silvicultura s ON f.SK_Silvicultura = s.SK_Silvicultura
JOIN Dim_Idade i ON f.SK_Idade = i.SK_Idade
WHERE f.Volume_m3 IS NOT NULL
GROUP BY s.MG, s.ESPACAMENTO
HAVING COUNT(*) >= 500
ORDER BY volume_medio_fuste_m3 DESC
LIMIT 10;


-- 5. Balanço Hídrico e Clima Médio Anual (BR-DWGD, 2006–2012)
WITH clima_proj_ano AS (
    SELECT
        l.PROJETO,
        t.Ano,
        SUM(c.pr) AS pr_anual,
        SUM(c.ET) AS et_anual,
        SUM(c.pr - c.ET) AS balanco_hidrico,
        AVG(c.Tmax) AS tmax,
        AVG(c.Tmin) AS tmin,
        AVG(c.RH) AS rh
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
    ROUND(AVG(balanco_hidrico), 1) AS balanco_hidrico_mm,
    ROUND(AVG(tmax), 2) AS tmax_media_c,
    ROUND(AVG(tmin), 2) AS tmin_media_c,
    ROUND(AVG(rh), 1) AS umidade_rel_pct
FROM clima_proj_ano
GROUP BY Ano
ORDER BY Ano;


-- 6. Consulta em Constelação: Inventário + Clima integrados por Projeto e Ano
WITH inv_proj_ano AS (
    SELECT
        l.PROJETO,
        t.Ano_IFC AS Ano,
        COUNT(*) AS n_fustes,
        ROUND(AVG(i.IDADE), 1) AS idade_media_meses,
        ROUND(AVG(CASE WHEN f.DAP > 0 THEN f.DAP END), 2) AS dap_medio_cm
    FROM Fato_Inventario_Fuste f
    JOIN Dim_Localizacao l ON f.SK_Localizacao = l.SK_Localizacao
    JOIN Dim_Tempo t ON f.SK_Tempo = t.SK_Tempo
    JOIN Dim_Idade i ON f.SK_Idade = i.SK_Idade
    GROUP BY l.PROJETO, t.Ano_IFC
),
clima_proj_ano AS (
    SELECT
        l.PROJETO,
        t.Ano,
        ROUND(SUM(c.pr), 1) AS pr_anual_mm,
        ROUND(SUM(c.ET), 1) AS et_anual_mm,
        ROUND(SUM(c.pr - c.ET), 1) AS balanco_hidrico_mm,
        ROUND(AVG((c.Tmax + c.Tmin) / 2.0), 2) AS tmed_c
    FROM Fato_Clima c
    JOIN Dim_Localizacao l ON c.SK_Localizacao = l.SK_Localizacao
    JOIN Dim_Tempo t ON c.SK_Tempo = t.SK_Tempo
    GROUP BY l.PROJETO, t.Ano
)
SELECT
    i.PROJETO,
    i.Ano,
    i.n_fustes,
    i.idade_media_meses,
    i.dap_medio_cm,
    c.pr_anual_mm,
    c.et_anual_mm,
    c.balanco_hidrico_mm,
    c.tmed_c
FROM inv_proj_ano i
JOIN clima_proj_ano c
  ON i.PROJETO = c.PROJETO
 AND i.Ano = c.Ano
ORDER BY i.PROJETO, i.Ano
LIMIT 20;
