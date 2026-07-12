-- ============================================================================
-- SCRIPT DE CONFIGURACIÓN DE BASE DE DATOS - TIMESCALEDB (TFM)
-- ============================================================================




-- 1. Asegurar que la extensión de TimescaleDB está activa en la base de datos
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 2. Tabla principal de métricas(metricas)
-- 2.1. Creación de la tabla principal de métricas
-- Se utiliza el tipo DOUBLE PRECISION para garantizar la máxima precisión en valores métricos.
CREATE TABLE IF NOT EXISTS metricas (
    time            TIMESTAMPTZ NOT NULL,   
    execution_name  TEXT,                  -- Identificador único de cada simulación o ejecución del pipeline
    instance        TEXT,                  
    grupo           TEXT,                  
    job             TEXT,                  
    metric_name     TEXT,           
    metric_value    DOUBLE PRECISION,                
    tags            JSONB,                  
    label           TEXT           
);

-- 2.2. Transformar la tabla convencional en una Hypertable de TimescaleDB
-- Esto activa el particionado automático por tiempo en bloques optimizados de 1 día.
SELECT create_hypertable('metricas', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

-- 2.3. Creación de índices optimizados para Machine Learning e Ingesta masiva
-- Se incluye 'execution_name' en el índice compuesto para acelerar radicalmente las 
-- búsquedas cuando los scripts de Python filtren una simulación específica y un tipo 
-- de métrica ordenado cronológicamente para alimentar el modelo predictivo.
CREATE INDEX IF NOT EXISTS idx_metricas_exec_name_time 
ON metricas (execution_name, metric_name, time DESC);

-- 2.4. Índice opcional para facilitar la segmentación por etiquetas de anomalías
CREATE INDEX IF NOT EXISTS idx_metricas_label 
ON metricas (label) 
WHERE label IS NOT NULL;



-- 2.5. Configuración de la compresion de la tabla métricas, no será automática, habrá que lanzarla manualmente
ALTER TABLE metricas SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'execution_name, metric_name, instance',
    timescaledb.compress_orderby = 'time DESC'
);





-- 3. Tabla de métricas transformadas(interpoladas a muestro uniforme, eliminadas metricas redundantes y rellenados nulos)(metricas)
-- 3.1. Creación de la tabla principal de métricas
-- Se utiliza el tipo DOUBLE PRECISION para garantizar la máxima precisión en valores métricos.
CREATE TABLE IF NOT EXISTS metricas_transformed (
    time            TIMESTAMPTZ NOT NULL,   
    execution_name  TEXT,                  -- Identificador único de cada simulación o ejecución del pipeline
    instance        TEXT,                  
    grupo           TEXT,                  
    job             TEXT,                  
    metric_name     TEXT,           
    metric_value    DOUBLE PRECISION,                
    tags            JSONB,                  
    label           TEXT           
);

-- 3.2. Transformar la tabla convencional en una Hypertable de TimescaleDB
-- Esto activa el particionado automático por tiempo en bloques optimizados de 1 día.
SELECT create_hypertable('metricas_transformed', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

-- 3.3. Creación de índices optimizados para Machine Learning e Ingesta masiva
-- Se incluye 'execution_name' en el índice compuesto para acelerar radicalmente las 
-- búsquedas cuando los scripts de Python filtren una simulación específica y un tipo 
-- de métrica ordenado cronológicamente para alimentar el modelo predictivo.
CREATE INDEX IF NOT EXISTS idx_metricas_transformed_exec_name_time 
ON metricas_transformed (execution_name, metric_name, time DESC);

-- 3.4. Índice opcional para facilitar la segmentación por etiquetas de anomalías
CREATE INDEX IF NOT EXISTS idx_metricas_transformed_label 
ON metricas_transformed (label) 
WHERE label IS NOT NULL;



-- 3.5. Configuración de la compresion de la tabla métricas, no será automática, habrá que lanzarla manualmente
ALTER TABLE metricas_transformed SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'execution_name, metric_name, instance',
    timescaledb.compress_orderby = 'time DESC'
);


