-- ============================================================================
-- SCRIPT DE CONFIGURACIÓN DE BASE DE DATOS - TIMESCALEDB (TFM)
-- ============================================================================

-- 1. Asegurar que la extensión de TimescaleDB está activa en la base de datos
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 2. Creación de la tabla principal de métricas
-- Se utiliza el tipo REAL para optimizar el almacenamiento de los valores métricos.
CREATE TABLE IF NOT EXISTS metricas (
    time         TIMESTAMPTZ NOT NULL,   
    instance     TEXT,                  
    grupo        TEXT,                 
    job          TEXT,                   
    metric_name  TEXT,           
    metric_value DOUBLE PRECISION,                
    tags         JSONB,                  
    label        TEXT           
);

-- 3. Transformar la tabla convencional en una Hypertable de TimescaleDB
-- Esto activa el particionado automático por tiempo (por defecto en bloques de 7 días).
SELECT create_hypertable('metricas', 'time', if_not_exists => TRUE,chunk_time_interval => INTERVAL '1 day');

-- 4. Creación de índices optimizados para Machine Learning e Ingesta masiva
-- Este índice compuesto acelera radicalmente las búsquedas cuando tus scripts de Python 
-- filtren un tipo de métrica específico ordenado cronológicamente para entrenar los modelos.
CREATE INDEX IF NOT EXISTS idx_metricas_name_time 
ON metricas (metric_name, time DESC);

-- 5. Índice opcional para facilitar la segmentación por etiquetas de anomalías
CREATE INDEX IF NOT EXISTS idx_metricas_label 
ON metricas (label) 
WHERE label IS NOT NULL;