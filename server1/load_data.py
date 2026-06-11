import csv
import os
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
    # cabeceras=['time', 'metric_name', 'instance', 'group', 'job', 'metric_value', 'label', 'tags']
    # fichero_salida='a.csv'
    # with open(fichero_salida, 'w', encoding='utf-8', newline='') as f:
    #     writer = csv.DictWriter(f, fieldnames=cabeceras)
    #     writer.writeheader()
    #     for reg in lista_datos_run:
    #         reg2=reg.copy()
    #         reg2['tags']=json.dumps(reg2.get('tags'))
    #         writer.writerows(reg2)

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

    t1=time.time()
    # N=0
    # for reg in lista_datos_run:
    #     db.insertar_registro(reg)
    #     N+=1
    #     if N>=100000:
    #         break
    #db.insertar_registros_masivos(lista_datos_run, batch_size=1024)
    db.insertar_registros_copy(lista_datos_run)
    t2=time.time()
    print('Carga realizada en %5.2f segundos'%(t2-t1))




def procesar_datos_json(contenido_json,label):
    lista_result=[]
    lista_campos=('__name__','instance','group','job')
    reg0={'time':None,'__name__':None,'instance':None,'group':None,'job':None,'metric_value':None,'label':label}
    if type(contenido_json)!=list:
        contenido_json=[contenido_json]
    for dato_json in contenido_json:
        reg=reg0.copy()
        tags_dict={}
        dato_json_metric=dato_json.get('metric')

        if dato_json_metric is None:
            continue
        for key in dato_json_metric.keys():
            # print(key)
            # print(dato_json_metric[key])
            if key in lista_campos:
                reg[key]      = dato_json_metric[key]
            else:
                tags_dict[key] = dato_json_metric[key] 
    reg['metric_name'] = reg.pop('__name__', '')
    reg['tags']        = tags_dict
    # print('reg=')
    # print(reg)
    # print('tags=')
    # print(tag_dict)

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
    # 1. Cargamos el filtro de etiquetas (ej: {'test_00_control', 'test_01_cpu'})
    labels_autorizados = cargar_labels_validos(fichero_labels_filtro)
    print("Etiquetas autorizadas para cargar: ")
    for label in labels_autorizados:
        print(f'   {label}')
    
    if not labels_autorizados:
        print("No hay etiquetas válidas para procesar.")
        return []

    # 2. Leemos el archivo de ejecuciones ignorando sus comentarios con el generador
    lineas_ejecuciones = filtrar_lineas_csv(fichero_registro_runs)
    lector_runs = csv.DictReader(lineas_ejecuciones)  
    datos_listos_para_bd = []

    # 3. Abrimos el fichero ZIP principal una sola vez para optimizar rendimiento
    try:
        with zipfile.ZipFile(fichero_dataset, 'r') as archivo_zip:
            
            # 4. Iteramos por cada ejecución registrada en el CSV de ejecuciones
            for run in lector_runs:
                lista_datos_run=[]
                #esto creo que no es necesario#label_ejecucion = run.get('label', '').strip().strip('"')
                fichero_contenedor = run.get('file', '').strip().strip('"') # Archivo .tar
                #esto creo que no es necesario#run_id = run.get('run', '').strip() # Identificador de la ejecución


                if fichero_contenedor in archivo_zip.namelist():
                    print(f'El fichero {fichero_contenedor} esta en el zip')
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
                                print(f'Empezamos a leer {miembro.name}')
                                # print('===================')
                                # print(miembro.name)
                                # print(label)
                                # print(datatype)
                                fichero_objeto = archivo_tar.extractfile(miembro)
                                if fichero_objeto:
                                    try:
                                        contenido_json = json.loads(fichero_objeto.read().decode('utf-8'))
                                        # print(f'Cargados datos de {miembro.name}')
                                        # print(contenido_json)
                                        lista_datos=procesar_datos_json(contenido_json,label)
                                        #print('---')
                                        # print(lista_datos)
                                        # for dato in lista_datos:
                                        #     print(dato)
                                        lista_datos_run+=lista_datos

                                    except Exception as e:
                                        print(f'Error al cargar datos de {miembro.name}')      
                                        print(e)
                                else: 
                                    print(f'Error al cargar datos de {miembro.name}')  
                                print(f'Fin lectura      {miembro.name}')


                t1=time.time()
                lista_datos_run.sort(key=lambda x: (
                    x['time'],
                    x['instance'],
                    x['group'] if x['group'] is not None else '',  # Evita el TypeError con los nulos
                    x['job'],
                    x['metric_name'],
                    x['label']
                ))
                t2=time.time()
                print('Tiempo de ordenacion  =   %5.3f'%(t2-t1))
                print('N_datos = %i'%(len(lista_datos_run)))
                # for dato in lista_datos_run:
                #     print(dato)
                #volcar_fichero_metricas(lista_datos_run)
                cargar_datos_en_timescaledb(lista_datos_run)



                

                                        
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
    RUTA_DATASET = os.getenv("RUTA_DATASET")
    FOLDER_CONFIG = os.getenv("FOLDER_CONFIG")
    label,datatype = get_label_and_datatype('light-oauth2-data-1719592986/access_token_authorization_form_401/metrics/')
    # Tus rutas de archivos
    FICHERO_RUNS = f'{FOLDER_CONFIG}/files_run.csv'
    FICHERO_FILTRO_LABELS = f'{FOLDER_CONFIG}/labels.csv'
    
    # Lanzamos el pipeline
    registros_finales = preparar_datos_para_bd(RUTA_DATASET,FICHERO_RUNS, FICHERO_FILTRO_LABELS)