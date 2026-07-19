import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from dotenv import load_dotenv
import json
import time
from timescale import TimescaleDBManager
from sqlalchemy import create_engine
import pandas as pd
import os
from contextlib import contextmanager

@contextmanager
def medir_tiempo(nombre_proceso):
    """Gestor de contexto para medir el tiempo de ejecución de forma elegante."""
    print(f"⏱️ [INICIO] Ejecutando: '{nombre_proceso}'...")
    inicio = time.perf_counter()
    try:
        yield
    finally:
        fin = time.perf_counter()
        duracion = fin - inicio
        print(f"⏱️ [FIN] '{nombre_proceso}' completado en {duracion:.4f} segundos.\n")


def cargar_datos_en_timescaledb(lista_datos_run):
    user_env = os.getenv("USER_DB")
    pass_env = os.getenv("PASS_DB")
    host_env = os.getenv("HOST_DB", "localhost")
    port_env = os.getenv("DB_PORT_EXPOSED", "5432")
    db_env   = os.getenv("NAME_DB", "tfm_db")

    db = TimescaleDBManager(
        user=user_env,
        password=pass_env,
        host=host_env,
        port=port_env,
        dbname=db_env
    )
    
    db.conectar()

    with medir_tiempo("Carga de datos en metricas con COPY"):
        db.insertar_registros_copy(lista_datos_run)


def obtener_datos_ejecucion_metrica(execution_name, metric_name, engine):
    """
    Extrae todos los registros de una ejecución y métrica específica.
    """
    SQL = """
        SELECT time,instance,grupo,job,tags,label,metric_value
        FROM metricas 
        WHERE execution_name = %s 
          AND metric_name = %s
        ORDER BY time ASC
    """
    
    # Ejecutamos la consulta pasando los parámetros de forma segura
    df = pd.read_sql(SQL, engine, params=(execution_name, metric_name))
    
    # Convertimos la columna time a formato datetime
    df['time'] = pd.to_datetime(df['time'])
    df         = df.sort_values('time')


    inicio_global = df['time'].min()
    df['tiempo_relativo'] = (df['time'] - inicio_global).dt.total_seconds()
        
    return df


def interpolar(df_serie,INTERVALO_VALOR):
    
    # --- 1. CONFIGURACIÓN ---
    INTERVALO_VALOR = 5
    INTERVALO_MUESTREO = f'{INTERVALO_VALOR}s'
    
    # --- 2. PREPARACIÓN ---
    # Aseguramos que 'time' es datetime y ordenamos
    df_serie['time'] = pd.to_datetime(df_serie['time'])
    df_serie = df_serie.sort_values('time')
    
    # Filtramos solo las columnas que nos interesan
    cols_a_mantener = ['time', 'metric_value', 'grupo', 'instance', 'job', 'label','tags']
    cols_numericas = ['metric_value']  # Ajusta si tienes más columnas numéricas
    cols_texto = ['grupo', 'instance', 'job', 'label', 'tags']
    
    df_filtrado = df_serie[cols_a_mantener].copy()
    df_filtrado['metric_value'] = df_filtrado['metric_value'].astype(float)
    
    # --- 3. PROCESAMIENTO ---
    # Establecemos el índice temporal
    df_indexado = df_filtrado.set_index('time').sort_index()
    
    # AJUSTE: Calculamos el final redondeando al siguiente múltiplo de 5 segundos
    # Esto asegura que si tu último punto es 799, el rango llegue hasta 800
    inicio = df_indexado.index.min()
    ultimo_timestamp = df_indexado.index.max()
    # Calculamos cuánto falta para el siguiente múltiplo de 5s
    segundos_faltantes = INTERVALO_VALOR - (ultimo_timestamp.second % INTERVALO_VALOR)
    fin = df_indexado.index.max().ceil(INTERVALO_MUESTREO) + pd.Timedelta(seconds=INTERVALO_VALOR)
    
    
    rango_muestreo = pd.date_range(start=inicio, end=fin, freq=INTERVALO_MUESTREO)
    
    # Creamos un DataFrame destino con el índice regular
    df_target = pd.DataFrame(index=rango_muestreo)
    
    df_fusionado = df_target.join(df_indexado, how='outer')
    df_fusionado[cols_numericas] = df_fusionado[cols_numericas].interpolate(method='time')
    df_fusionado[cols_texto] = df_fusionado[cols_texto].ffill()
    df_final = df_fusionado.loc[df_target.index]
    inicio_global = df_final.index.min()
    df_final['tiempo_relativo'] = (df_final.index - inicio_global).total_seconds()
    return df_final

def get_lista_ejecuciones_sel(engine, FOLDER_SALIDA):
    SQL='select distinct(execution_name) from metricas'
    df_metric_name = pd.read_sql(SQL, engine)
    lista_execution_name=df_metric_name['execution_name'].to_list()

    ruta_archivo = f'{FOLDER_SALIDA}/lista_ejecuciones_descartadas.json'
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        lista_ejecuciones_descartadas = json.load(f)
        
    set_descartadas = set(lista_ejecuciones_descartadas)
    lista_ejecuciones_sel = [x for x in lista_execution_name if x not in set_descartadas]

    return lista_ejecuciones_sel



def configurar_logging(carpeta_logs="log"):
    """Configura dos loggers con el mismo nivel INFO pero distintos destinos."""
    os.makedirs(carpeta_logs, exist_ok=True)
    fecha_hoy = datetime.now().strftime("%Y%m%d")
    ruta_log = os.path.join(carpeta_logs, f"transform_data_{fecha_hoy}.log")
    
    formato = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # --- HANDLERS COMPARTIDOS ---
    file_handler = logging.FileHandler(ruta_log, encoding="utf-8")
    file_handler.setFormatter(formato)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formato)
    
    # 1. LOGGER PRINCIPAL: Va a Pantalla y a Disco
    log_pantalla_y_disco = logging.getLogger("transform_data")
    log_pantalla_y_disco.setLevel(logging.INFO)
    log_pantalla_y_disco.propagate = False # Evita duplicados en el logger raíz
    if not log_pantalla_y_disco.hasHandlers():
        log_pantalla_y_disco.addHandler(file_handler)
        log_pantalla_y_disco.addHandler(console_handler)
        
    # 2. LOGGER SILENCIOSO: Va SOLO a Disco
    log_solo_disco = logging.getLogger("transform_data.archivo")
    log_solo_disco.setLevel(logging.INFO)
    log_solo_disco.propagate = False # Evita que el mensaje "suba" a la consola
    if not log_solo_disco.hasHandlers():
        log_solo_disco.addHandler(file_handler) # Solo le añadimos el manejador de archivo

    logging.getLogger("transform_data").info("📝 Sistema de logging doble inicializado (INFO dual).")


def get_datos_calculados(engine, FOLDER_SALIDA):
    SQL='select distinct(execution_name) from metricas'
    df_metric_name = pd.read_sql(SQL, engine)
    lista_execution_name=df_metric_name['execution_name'].to_list()



    lista_ejecuciones_sel = get_lista_ejecuciones_sel(engine, FOLDER_SALIDA)

    df_analisis_metricas = pd.read_parquet(f'{FOLDER_SALIDA}/analisis_metricas.parquet')
    lista_metricas_sel=df_analisis_metricas[   df_analisis_metricas['metric_name_sel']!=''  ]['metric_name'].tolist()


    ruta_archivo = f'{FOLDER_SALIDA}/lista_ejecuciones_descartadas.json'
    with open(ruta_archivo, "r", encoding="utf-8") as f:
        lista_ejecuciones_descartadas = json.load(f)

    set_descartadas = set(lista_ejecuciones_descartadas)
    lista_ejecuciones_sel = [x for x in lista_execution_name if x not in set_descartadas]

    return lista_ejecuciones_sel, lista_metricas_sel, df_analisis_metricas
   
    


if __name__ == "__main__":
    load_dotenv()

    configurar_logging()
    log = logging.getLogger("transform_data")         
    log_solo_disco = logging.getLogger("transform_data.archivo")  


    INTERVALO_MUESTREO = 5
    load_dotenv()
    user_env        = os.getenv("USER_DB")
    pass_env        = os.getenv("PASS_DB")
    host_env        = os.getenv("HOST_DB", "localhost")
    port_env        = os.getenv("DB_PORT_EXPOSED", "5432")
    db_env          = os.getenv("NAME_DB", "tfm_db")
    FOLDER_SALIDA   = os.getenv("FOLDER_SALIDA")
    FOLDER_TMP      = os.getenv("FOLDER_TMP")

    SEED            = os.getenv("SEED")

    db = TimescaleDBManager(
        user=user_env,
        password=pass_env,
        host=host_env,
        port=port_env,
        dbname=db_env
    )
    db.conectar()
    conexion_postgresql = db.connection
    #esto es para usar una conexion postgrews para pandas que da un warning indicando que solo esta testeado para sqlalchemy
    engine = create_engine(f'postgresql+psycopg2://{user_env}:{pass_env}@{host_env}:{port_env}/{db_env}')

    lista_ejecuciones_sel, lista_metricas_sel, df_analisis_metricas =get_datos_calculados(engine, FOLDER_SALIDA)



    N_ejecucion=0
    for una_ejecucion in lista_ejecuciones_sel:
        N_ejecucion+=1

        lista_df_procesados = []
        for una_metrica in lista_metricas_sel:
            df_sel = df_analisis_metricas[df_analisis_metricas['metric_name'] == una_metrica]
            clasificacion = df_sel.iloc[0]['clasificacion']
            if clasificacion=='diff':
                nombre_metrica = una_metrica+'_diff'
            else:
                nombre_metrica    = una_metrica
            log_solo_disco.info(f'execution_name = {una_ejecucion}   |  metric_name = {una_metrica}')
            df_serie        = obtener_datos_ejecucion_metrica(una_ejecucion, una_metrica, engine)
            df_procesado = (
                interpolar(df_serie, INTERVALO_MUESTREO)
                .reset_index()
                .rename(columns={'index': 'time'})
                .drop(columns=['tiempo_relativo'], errors='ignore')
            )
            df_procesado['metric_name'] = nombre_metrica
            if clasificacion == 'diff':
                df_procesado['metric_value'] = df_procesado['metric_value'].diff().fillna(0)
            lista_df_procesados.append(df_procesado)
        
        
        #obtenemos esta metrica que se vio que prsenta muchos NaN en anomalias, asi que puede servir para detectarlas
        #crearemos una metrica _isnan que sera 1 si hay valor NaN
        #modificaremos la metrica original para sustituir los NaN por valores medios
        una_metrica = 'prometheus_target_sync_length_seconds'
        df_serie    = obtener_datos_ejecucion_metrica(una_ejecucion, una_metrica, engine)
        
        
        #creamos metrica _isnan
        df_serie2  = df_serie.copy()
        df_serie2['metric_value'] = df_serie2['metric_value'].isna().astype(int)
        df_procesado = (
            interpolar(df_serie2, INTERVALO_MUESTREO)
            .reset_index()
            .rename(columns={'index': 'time'})
            .drop(columns=['tiempo_relativo'], errors='ignore')
        )
        df_procesado['metric_name'] = nombre_metrica+'_isnan'
        lista_df_procesados.append(df_procesado)
        
        
        mediana = df_serie['metric_value'].median()
        
        # Rellenamos los NaN con ese valor
        df_serie['metric_value'] = df_serie['metric_value'].fillna(mediana)
        
        
        #creamos la metrica _mod rellenando con la media
        df_procesado = (
            interpolar(df_serie, INTERVALO_MUESTREO)
            .reset_index()
            .rename(columns={'index': 'time'})
            .drop(columns=['tiempo_relativo'], errors='ignore')
        )
        df_procesado['metric_name'] = nombre_metrica+'_mod'
        lista_df_procesados.append(df_procesado)
        df_final = pd.concat(lista_df_procesados, ignore_index=True)
        
        df_final = df_final.sort_values(by=['time', 'metric_name', 'instance', 'grupo'])
        df_final['execution_name'] = una_ejecucion

        #hay que renombrar la columna grupo a group

        fichero_parquet=f'{FOLDER_TMP}/df_transformado({una_ejecucion}).parquet'
        df_final.to_parquet(fichero_parquet, index=False,engine='fastparquet')
        log.info(f'Exportado datos a fichero ({fichero_parquet})    N_ejecucion = {N_ejecucion:4d}\r')
        lista_datos_run = df_final.to_dict('records')
        lista_datos_run.sort(key=lambda x: (
            x['time'],
            x['instance'],
            x['grupo'] if x['grupo'] is not None else '',  # Evita el TypeError con los nulos
            x['job'],
            x['metric_name'],
            x['label']
        ))

        #cargar_datos_en_timescaledb(lista_datos_run)
        db.insertar_registros_copy(lista_datos_run, tabla='metricas_transformed')
        log.info(f'Insertados datos en metricas_transformadas  {una_ejecucion}  N_ejecucion = {N_ejecucion:4d}\r')
        db.comprimir_chunks_antiguos('metricas_transformed')
        # if N_ejecucion >=3:
        #     break




    