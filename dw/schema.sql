-- Data Warehouse Florestal & Climático (Constelação de Fatos - DuckDB)
-- Baseado em modelo_estrela.pdf e Proposta_BI.pdf

-- ============================================================================
-- DIMENSÕES CONFORMADAS
-- ============================================================================

CREATE TABLE IF NOT EXISTS Dim_Tempo (
    SK_Tempo INTEGER PRIMARY KEY,
    Data_Medicao DATE NOT NULL,
    Ano_IFC INTEGER,
    Ano INTEGER NOT NULL,
    Mes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS Dim_Idade (
    SK_Idade INTEGER PRIMARY KEY,
    IDADE DOUBLE NOT NULL,
    Idade_Anos INTEGER NOT NULL,
    Fase_Desenvolvimento VARCHAR NOT NULL,
    ROT INTEGER,
    CICLO INTEGER
);

CREATE TABLE IF NOT EXISTS Dim_Localizacao (
    SK_Localizacao INTEGER PRIMARY KEY,
    PROJETO VARCHAR NOT NULL,
    COD_TALHAO VARCHAR,
    TALHAO VARCHAR,
    Parcela INTEGER,
    Area DOUBLE,
    AreaCorr DOUBLE
);

CREATE TABLE IF NOT EXISTS Dim_Silvicultura (
    SK_Silvicultura INTEGER PRIMARY KEY,
    MG VARCHAR NOT NULL,
    ESPACAMENTO VARCHAR
);

CREATE TABLE IF NOT EXISTS Dim_Fuste (
    SK_Fuste INTEGER PRIMARY KEY,
    Cova INTEGER NOT NULL,
    Fuste INTEGER NOT NULL,
    Fila INTEGER NOT NULL,
    Codigos VARCHAR
);

-- ============================================================================
-- TABELAS FATO (CONSTELAÇÃO DE FATOS)
-- ============================================================================

CREATE TABLE IF NOT EXISTS Fato_Inventario_Fuste (
    SK_Tempo INTEGER NOT NULL REFERENCES Dim_Tempo(SK_Tempo),
    SK_Idade INTEGER NOT NULL REFERENCES Dim_Idade(SK_Idade),
    SK_Localizacao INTEGER NOT NULL REFERENCES Dim_Localizacao(SK_Localizacao),
    SK_Silvicultura INTEGER NOT NULL REFERENCES Dim_Silvicultura(SK_Silvicultura),
    SK_Fuste INTEGER NOT NULL REFERENCES Dim_Fuste(SK_Fuste),
    DAP DOUBLE,
    AltObs DOUBLE,
    Volume_m3 DOUBLE
);

CREATE TABLE IF NOT EXISTS Fato_Clima (
    SK_Tempo INTEGER NOT NULL REFERENCES Dim_Tempo(SK_Tempo),
    SK_Localizacao INTEGER NOT NULL REFERENCES Dim_Localizacao(SK_Localizacao),
    pr DOUBLE,
    ET DOUBLE,
    RH DOUBLE,
    u2 DOUBLE,
    Rs DOUBLE,
    Tmax DOUBLE,
    Tmin DOUBLE
);

-- ============================================================================
-- ÍNDICES ANALÍTICOS PARA OTIMIZAÇÃO DE JOINS E AGREGAÇÕES
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_inv_tempo ON Fato_Inventario_Fuste (SK_Tempo);
CREATE INDEX IF NOT EXISTS idx_inv_loc ON Fato_Inventario_Fuste (SK_Localizacao);
CREATE INDEX IF NOT EXISTS idx_inv_idade ON Fato_Inventario_Fuste (SK_Idade);
CREATE INDEX IF NOT EXISTS idx_inv_silv ON Fato_Inventario_Fuste (SK_Silvicultura);
CREATE INDEX IF NOT EXISTS idx_inv_fuste ON Fato_Inventario_Fuste (SK_Fuste);

CREATE INDEX IF NOT EXISTS idx_clima_tempo ON Fato_Clima (SK_Tempo);
CREATE INDEX IF NOT EXISTS idx_clima_loc ON Fato_Clima (SK_Localizacao);
