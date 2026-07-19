
import logging
from logging.handlers import RotatingFileHandler
import csv
from datetime import datetime, timezone
from dotenv import load_dotenv
import tarfile
import zipfile
import sys
import csv
import io
import json
import traceback
import time
from timescale import TimescaleDBManager
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

def loguear_excepcion(contexto, excepcion):
    log = logging.getLogger("pipeline")
    log_oculto = logging.getLogger("pipeline.archivo")
    
    # 1. Extraer el tipo de error
    tipo_error = type(excepcion).__name__
    
    # 2. Desempaquetar el traceback para obtener las coordenadas del fallo
    _, _, exc_tb = sys.exc_info()
    tb_detallado = traceback.extract_tb(exc_tb)
    
    if tb_detallado:
        # Tomamos el último paso (donde rompió el código realmente)
        ultimo_paso = tb_detallado[-1]
        archivo = os.path.basename(ultimo_paso.filename)  # Limpia rutas de Windows
        linea = ultimo_paso.lineno
        funcion = ultimo_paso.name
        codigo_linea = ultimo_paso.line
    else:
        archivo, linea, funcion, codigo_linea = "Desconocido", "N/A", "Desconocida", "N/A"

    # 3. 📺 PANTALLA + 📁 DISCO: Alerta visual limpia para la consola
    log.error(
        f"❌ Fallo en [{contexto}] -> {tipo_error} "
        f"({archivo} | {funcion}() | Línea {linea})"
    )      
    
    # 4. 📁 SOLO DISCO: Auditoría forense completa en el archivo .log
    log_oculto.info("=" * 60)
    log_oculto.info(f"📋 DIAGNÓSTICO DE ERROR - CONTEXTO: {contexto.upper()}")
    log_oculto.info(f"• Tipo de Error: {tipo_error}")
    log_oculto.info(f"• Mensaje: {excepcion}")
    log_oculto.info(f"• Archivo: {archivo}")
    log_oculto.info(f"• Función: {funcion}()")
    log_oculto.info(f"• Línea: {linea}")
    log_oculto.info(f"• Código problemático: {codigo_linea}")
    log_oculto.info("=" * 60)



def configurar_logging(carpeta_logs="log"):
    """Configura dos loggers con el mismo nivel INFO pero distintos destinos."""
    os.makedirs(carpeta_logs, exist_ok=True)
    fecha_hoy = datetime.now().strftime("%Y%m%d")
    ruta_log = os.path.join(carpeta_logs, f"pipeline_carga_{fecha_hoy}.log")
    
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
    log_pantalla_y_disco = logging.getLogger("pipeline")
    log_pantalla_y_disco.setLevel(logging.INFO)
    log_pantalla_y_disco.propagate = False # Evita duplicados en el logger raíz
    if not log_pantalla_y_disco.hasHandlers():
        log_pantalla_y_disco.addHandler(file_handler)
        log_pantalla_y_disco.addHandler(console_handler)
        
    # 2. LOGGER SILENCIOSO: Va SOLO a Disco
    log_solo_disco = logging.getLogger("pipeline.archivo")
    log_solo_disco.setLevel(logging.INFO)
    log_solo_disco.propagate = False # Evita que el mensaje "suba" a la consola
    if not log_solo_disco.hasHandlers():
        log_solo_disco.addHandler(file_handler) # Solo le añadimos el manejador de archivo

    logging.getLogger("pipeline").info("📝 Sistema de logging doble inicializado (INFO dual).")



def cargar_labels_validos(fichero_labels):
    """
    Lee un fichero de etiquetas CSV con DictReader, descartando
    las líneas que comienzan con el prefijo '#'.
    """
    labels_validos = []
    
    with open(fichero_labels, mode='r', encoding='utf-8') as f:
        # Generador que descarta líneas vacías o que empiezan con '#'
        lineas_filtradas = (
            linea for linea in f 
            if linea.strip() and not linea.strip().startswith('#')
        )
        
        # Pasamos las líneas limpias al DictReader
        lector = csv.DictReader(lineas_filtradas)
        
        for fila in lector:
            # Aseguramos que la columna 'label' exista en la fila
            if 'label' in fila and fila['label']:
                labels_validos.append(fila['label'])
                
    return labels_validos




def filtrar_lineas_csv(ruta_csv):
    """
    Generador que abre un CSV y devuelve únicamente las líneas 
    que no son comentarios (#). Protege la cabecera del DictReader.
    """
    with open(ruta_csv, 'r', encoding='utf-8') as f:
        for linea in f:
            if not linea.strip().startswith('#'):
                yield linea



def get_label_and_datatype(path):
    # light-oauth2-data-1719594842/delete_token_404/light-oauth2-oauth2-service-1.log
    pos1      = path.find('/')
    pos2      = path.find('/',pos1+1)
    pos3      = path.rfind('/')
    label     = path[pos1+1:pos2]
    if path[-1]=='/':
        datatype=None
    else:
        filename  = path[pos3+1:]
        if filename.find('metric_')>=0:
            datatype='metric'
        elif filename.find('traces_')>=0:
            datatype='traces'    
        elif filename.find('.log')>=0: 
            datatype='log'
        else:                
            datatype='other'
    return label,datatype


def volcar_fichero_metricas(lista_datos_run):
    fichero_salida='a.jsonl'
    with open(fichero_salida, 'w', encoding='utf-8') as f:
        for reg in lista_datos_run:
            f.write(json.dumps(reg)+'\n')

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





def procesar_datos_json(contenido_json,label,execution_name):
    lista_result=[]
    lista_campos=('__name__','instance','group','job')
    reg0={'time':None,'execution_name':execution_name,'__name__':None,'instance':None,'group':None,'job':None,'metric_value':None,'label':label}
    if type(contenido_json)!=list:
        contenido_json=[contenido_json]
    for dato_json in contenido_json:
        reg=reg0.copy()
        tags_dict={}
        dato_json_metric=dato_json.get('metric')

        if dato_json_metric is None:
            continue
        for key in dato_json_metric.keys():
            if key in lista_campos:
                reg[key]      = dato_json_metric[key]
            else:
                tags_dict[key] = dato_json_metric[key] 
    reg['metric_name'] = reg.pop('__name__', '')
    reg['tags']        = tags_dict

    values_json=dato_json.get('values')

    if values_json is None:
        return lista_result
    elif type(values_json)!=list:
        return lista_result
    
    for value_data in values_json:
        un_reg=reg.copy()
        un_reg['time']          = value_data[0]
        un_reg['metric_value']  = value_data[1]
        lista_result.append(un_reg)


    return lista_result





def preparar_datos_para_bd(fichero_dataset, fichero_registro_runs, fichero_labels_filtro):

    labels_autorizados = cargar_labels_validos(fichero_labels_filtro)
    log.info("Etiquetas autorizadas para cargar: ")
    for label in labels_autorizados:
        log.info(f'   {label}')
    
    if not labels_autorizados:
        log.info("No hay etiquetas válidas para procesar.")
        return []

    # 2. Leemos el archivo de ejecuciones ignorando sus comentarios con el generador
    lineas_ejecuciones = filtrar_lineas_csv(fichero_registro_runs)
    lector_runs = csv.DictReader(lineas_ejecuciones)  



    #hay que revisar para crear la conexion solo una vez y luego reutilizarla
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
    SQL='select distinct execution_name from metricas;'
    resultados_db=db.ejecutar_select_generica(SQL)
    lista_ejecuciones_en_BBDD = [row['execution_name'] for row in resultados_db]
    #print(lista_ejecuciones_en_BBDD)


    datos_listos_para_bd = []

    # 3. Abrimos el fichero ZIP principal una sola vez para optimizar rendimiento

    try:
        with zipfile.ZipFile(fichero_dataset, 'r') as archivo_zip:
            log.info(f'Abierto fichero zip {fichero_dataset}')
            
            # 4. Iteramos por cada ejecución registrada en el CSV de ejecuciones
            for run in lector_runs:
                lista_datos_run=[]
                fichero_contenedor = run.get('file', '').strip().strip('"') # Archivo .tar

                if fichero_contenedor in lista_ejecuciones_en_BBDD:
                    #log.info(f'El fichero {fichero_contenedor} ya esta en BBDD, no lo subimos.')
                    continue
                if fichero_contenedor in archivo_zip.namelist():
                    log.info(f'El fichero {fichero_contenedor} esta en el zip')
                    # Leemos el contenido binario del TAR desde el ZIP sin extraerlo a disco
                    tar_bytes = archivo_zip.read(fichero_contenedor)
                    
                    # Convertimos los bytes en un flujo de datos para tarfile
                    with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode='r') as archivo_tar:
                        
                        # Examinamos cada elemento dentro del archivo TAR
                        for miembro in archivo_tar.getmembers():
                            label,datatype = get_label_and_datatype(miembro.name)
                            if datatype is None:
                                continue
                            if (label in labels_autorizados) and (datatype=='metric'):
                                log_solo_disco.info(f'Empezamos a leer {miembro.name}')
                                # print('===================')
                                # print(miembro.name)
                                # print(label)
                                # print(datatype)
                                fichero_objeto = archivo_tar.extractfile(miembro)
                                if fichero_objeto:
                                    try:
                                        contenido_json = json.loads(fichero_objeto.read().decode('utf-8'))
                                        lista_datos=procesar_datos_json(contenido_json,label,fichero_contenedor)
                                        lista_datos_run+=lista_datos

                                    except Exception as e:
                                        log.info(f'Error al decodificar fichero ({miembro})')

                                        loguear_excepcion(f"Carga de miembro: {miembro.name}", e)

                                        # print(f'Error al cargar datos de {miembro.name}')      
                                        # print(e)
                                else: 
                                    log.info(f'Error al cargar datos de {miembro.name}')  
                                log_solo_disco.info(f'Fin lectura      {miembro.name}')

                with medir_tiempo("Ordenar datos de la ejecución"):
                    lista_datos_run.sort(key=lambda x: (
                        x['time'],
                        x['instance'],
                        x['group'] if x['group'] is not None else '',  # Evita el TypeError con los nulos
                        x['job'],
                        x['metric_name'],
                        x['label']
                    ))
                    log.info('N_datos = %i'%(len(lista_datos_run)))


                cargar_datos_en_timescaledb(lista_datos_run)

                db.comprimir_chunks_antiguos('metricas')



                

                                        
    except zipfile.BadZipFile:
        print(f"Error: El archivo {fichero_dataset} no es un ZIP válido o está corrupto.")
        return []
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()

        # 2. Extraemos el último paso del traceback (donde ocurrió el error real)
        detalles = traceback.extract_tb(exc_traceback)[-1]

        fichero = detalles.filename
        linea = detalles.lineno
        funcion = detalles.name
        codigo_causante = detalles.line

        # 3. Ya lo tienes listo para meterlo en un diccionario y volcarlo con tu DictWriter
        registro_error = {
            "status": "ERROR",
            "error_tipo": exc_type.__name__,
            "mensaje": str(e),
            "fichero": fichero,
            "linea": linea,
            "funcion": funcion,
        }

        print("📊 Registro estructurado para tu base de datos o CSV:")
        print(registro_error)
        return []
                
    print(f"Preparación completada. Se generaron {len(datos_listos_para_bd)} registros listos para la Base de Datos.")
    return datos_listos_para_bd



if __name__ == "__main__":
    load_dotenv()

    configurar_logging()
    log = logging.getLogger("pipeline")         
    log_solo_disco = logging.getLogger("pipeline.archivo")  






    # Ahora sí, el mensaje inicial
    log.info("📝 Sistema de logging doble inicializado (Rotación: 10MB, Máx: 10 archivos).")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    FOLDER_CONFIG = os.getenv("FOLDER_CONFIG")
    FICHERO_RUNS          = os.path.join(BASE_DIR, f'{FOLDER_CONFIG}', "files_run.csv")
    FICHERO_FILTRO_LABELS = os.path.join(BASE_DIR, f'{FOLDER_CONFIG}', "labels.csv")
    RUTA_DATASET          = os.path.join(BASE_DIR, os.getenv("RUTA_DATASET"))
    

    registros_finales = preparar_datos_para_bd(RUTA_DATASET,FICHERO_RUNS, FICHERO_FILTRO_LABELS)


