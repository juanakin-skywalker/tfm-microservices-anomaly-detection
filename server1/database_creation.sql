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



-- 4. Tabla de escalado
CREATE TABLE t_escalado (
    tabla TEXT NOT NULL,
    execution_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    media DOUBLE PRECISION,
    desv_std DOUBLE PRECISION,
    PRIMARY KEY (tabla,execution_name, metric_name)
);

-- Índice para acelerar los JOINs entre tus métricas y esta tabla de escalado
CREATE INDEX idx_escalado_lookup ON t_escalado (tabla,execution_name, metric_name);



-- 4. Tabla de vectores de características para Machine Learning
CREATE TABLE vectores (
    vector_id BIGSERIAL PRIMARY KEY,
    execution_name TEXT NOT NULL,
    label TEXT NOT NULL,
    t_rel FLOAT8 NOT NULL,
    vector DOUBLE PRECISION[] NOT NULL
);

-- Índices optimizados para búsquedas frecuentes
CREATE INDEX idx_vectores_label ON vectores(label);
CREATE INDEX idx_vectores_execution ON vectores(execution_name);



CREATE TABLE vectores_split (
    vector_id BIGINT PRIMARY KEY,
    split_type TEXT NOT NULL CHECK (split_type IN ('train', 'test', 'val','unknown')),
    CONSTRAINT fk_vector 
        FOREIGN KEY (vector_id) 
        REFERENCES public.vectores(vector_id) 
        ON DELETE CASCADE
);




-- 5. tabla para logs de microsrervicios
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS microservicios_logs (
    time TIMESTAMP NOT NULL,
    execution_name TEXT,
    service_name TEXT,
    label TEXT,
    thread TEXT,
    trace_id TEXT,
    level TEXT,
    logger TEXT,
    message TEXT,
    exception_class TEXT
);

SELECT create_hypertable('microservicios_logs', 'time', if_not_exists => TRUE, chunk_time_interval => INTERVAL '1 day');

CREATE INDEX idx_microservicios_logs_label ON microservicios_logs (label) WHERE label IS NOT NULL;;
CREATE INDEX idx_microservicios_logs_execution_name ON microservicios_logs (execution_name);


ALTER TABLE microservicios_logs SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'execution_name',
    timescaledb.compress_orderby = 'time DESC'
);