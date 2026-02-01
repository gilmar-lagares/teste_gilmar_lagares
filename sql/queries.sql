-- ============================================================================
-- 3. TESTE DE BANCO DE DADOS E ANÁLISE
-- Compatibilidade: PostgreSQL 10+ (Recomendado) ou MySQL 8.0
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 3.2. DDL - Estrutura das Tabelas
-- ----------------------------------------------------------------------------

-- Justificativa de Normalização (Opção B):
-- Optou-se por separar 'operadoras' de 'despesas_consolidadas'.
-- Motivo: Redução de redundância. O nome da operadora, CNPJ e UF se repetem
-- milhares de vezes. Separar economiza armazenamento e facilita a atualização cadastral.

CREATE TABLE IF NOT EXISTS operadoras (
    registro_ans VARCHAR(20) PRIMARY KEY,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    modalidade VARCHAR(100), -- Pedido no teste 2.2
    uf CHAR(2)
);

CREATE TABLE IF NOT EXISTS despesas_consolidadas (
    id SERIAL PRIMARY KEY, -- Ou AUTO_INCREMENT no MySQL
    registro_ans VARCHAR(20),
    data_evento DATE, -- Usaremos a data para identificar o trimestre
    valor DECIMAL(15,2), -- DECIMAL evita erros de arredondamento do FLOAT
    descricao_conta VARCHAR(255),
    CONSTRAINT fk_operadora FOREIGN KEY (registro_ans) REFERENCES operadoras(registro_ans)
);

CREATE TABLE IF NOT EXISTS despesas_agregadas (
    razao_social VARCHAR(255),
    uf CHAR(2),
    total_despesas DECIMAL(20,2),
    media_trimestral DECIMAL(15,2),
    desvio_padrao DECIMAL(15,2),
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Índices para performance nas queries analíticas
CREATE INDEX idx_despesas_data ON despesas_consolidadas(data_evento);
CREATE INDEX idx_despesas_operadora ON despesas_consolidadas(registro_ans);

-- ----------------------------------------------------------------------------
-- 3.3. Queries de Importação (Exemplos)
-- ----------------------------------------------------------------------------

-- Nota: Como o ETL (Python) já limpou os dados (tratou NULLs e formatos),
-- a importação é direta.
-- No PostgreSQL, use o comando COPY. No MySQL, use LOAD DATA INFILE.

/*
-- Exemplo PostgreSQL:
COPY operadoras(registro_ans, cnpj, razao_social, modalidade, uf) 
FROM '/path/to/data/operadoras_tratadas.csv' DELIMITER ',' CSV HEADER;

COPY despesas_consolidadas(registro_ans, data_evento, valor, descricao_conta) 
FROM '/path/to/data/despesas_limpas.csv' DELIMITER ',' CSV HEADER;
*/

-- ----------------------------------------------------------------------------
-- 3.4. Queries Analíticas
-- ----------------------------------------------------------------------------

-- QUERY 1: Top 5 operadoras com maior crescimento percentual (Início vs Fim)
-- Desafio: Tratamento de operadoras sem dados no início (Divisão por zero).
-- Solução: Filtramos apenas quem tem dados em AMBOS os períodos (> 0).

WITH limites_temporais AS (
    SELECT MIN(data_evento) as data_inicio, MAX(data_evento) as data_fim 
    FROM despesas_consolidadas
),
valores_inicio AS (
    SELECT registro_ans, SUM(valor) as total_ini
    FROM despesas_consolidadas, limites_temporais
    WHERE data_evento = limites_temporais.data_inicio
    GROUP BY registro_ans
),
valores_fim AS (
    SELECT registro_ans, SUM(valor) as total_fim
    FROM despesas_consolidadas, limites_temporais
    WHERE data_evento = limites_temporais.data_fim
    GROUP BY registro_ans
)
SELECT 
    o.razao_social,
    vi.total_ini,
    vf.total_fim,
    ROUND(((vf.total_fim - vi.total_ini) / vi.total_ini) * 100, 2) as crescimento_pct
FROM valores_inicio vi
JOIN valores_fim vf ON vi.registro_ans = vf.registro_ans
JOIN operadoras o ON vi.registro_ans = o.registro_ans
WHERE vi.total_ini > 0 -- Evita divisão por zero e crescimentos infinitos
ORDER BY crescimento_pct DESC
LIMIT 5;


-- QUERY 2: Distribuição por UF e Média por Operadora
-- Desafio Adicional: Média de despesas por operadora em cada UF.

SELECT 
    o.uf,
    SUM(d.valor) as total_despesas_estado,
    COUNT(DISTINCT d.registro_ans) as qtd_operadoras,
    ROUND(SUM(d.valor) / COUNT(DISTINCT d.registro_ans), 2) as media_por_operadora
FROM despesas_consolidadas d
JOIN operadoras o ON d.registro_ans = o.registro_ans
GROUP BY o.uf
ORDER BY total_despesas_estado DESC
LIMIT 5;


-- QUERY 3: Operadoras > Média Geral em pelo menos 2 trimestres
-- Trade-off: Uso de CTEs e Window Functions (AVG OVER) para legibilidade e performance
-- em vez de subqueries correlacionadas complexas.

WITH media_por_trimestre AS (
    -- 1. Calcula a média global de despesas de TODAS operadoras por trimestre
    SELECT 
        data_evento, 
        AVG(SUM(valor)) OVER (PARTITION BY data_evento) as media_global_trimestre
    FROM despesas_consolidadas
    GROUP BY data_evento, registro_ans -- Agrupa para ter a soma por operadora antes da média
),
despesas_operadora_trimestre AS (
    -- 2. Calcula total de cada operadora por trimestre
    SELECT 
        registro_ans,
        data_evento,
        SUM(valor) as total_operadora
    FROM despesas_consolidadas
    GROUP BY registro_ans, data_evento
),
performance_comparada AS (
    -- 3. Compara Operadora vs Média Global do Trimestre
    SELECT 
        dot.registro_ans,
        dot.data_evento,
        CASE WHEN dot.total_operadora > mpt.media_global_trimestre THEN 1 ELSE 0 END as acima_da_media
    FROM despesas_operadora_trimestre dot
    JOIN (SELECT DISTINCT data_evento, media_global_trimestre FROM media_por_trimestre) mpt 
      ON dot.data_evento = mpt.data_evento
)
-- 4. Filtra quem ficou acima da média >= 2 vezes
SELECT 
    o.razao_social,
    COUNT(*) as trimestres_acima_media
FROM performance_comparada pc
JOIN operadoras o ON pc.registro_ans = o.registro_ans
WHERE pc.acima_da_media = 1
GROUP BY o.razao_social
HAVING COUNT(*) >= 2
ORDER BY trimestres_acima_media DESC;