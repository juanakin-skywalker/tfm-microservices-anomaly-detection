import os
import zipfile
from dotenv import load_dotenv
import csv
import tarfile
from datetime import datetime, timezone
import io



def listar_raiz_zip(ruta_zip):
    lista_ficheros_run=[]
    print(f"📦 Abriendo: {os.path.basename(ruta_zip)}")
    print("=" * 50)
    
    try:
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            # namelist() devuelve las rutas de todo lo que hay dentro
            for ruta_interna in z.namelist():
                # Al romper por la barra, contamos los elementos de la ruta
                partes = ruta_interna.strip('/').split('/')
                
                # Si solo tiene 1 parte, es un fic#hero o carpeta en la raíz del ZIP
                if len(partes) == 1:
                    es_carpeta = ruta_interna.endswith('/')
                    tipo = "📁 Carpeta:" if es_carpeta else "📄 Fichero:"
                    #print(f"{tipo} {partes[0]}")
                    lista_ficheros_run.append(partes[0])
                    
    except zipfile.BadZipFile:
        print("❌ El archivo ZIP está corrupto.")


    return lista_ficheros_run

def volcar_lista_ficheros_run(lista_ficheros_run, FICHERO_lista_ficheros_run):
    lista_ficheros_run.sort()
    cabeceras=['file','time','datetime']
    with open(FICHERO_lista_ficheros_run, 'w', encoding='utf-8',newline='') as f:
        writer = csv.DictWriter(f, fieldnames=cabeceras,quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for item in lista_ficheros_run:        
            #print(item)
            FICHERO=item
            pos2=item.rfind('.')
            pos1=item.rfind('-', 0, pos2)
            # print(item, pos1, pos2)
            if pos1>0 and pos2>0:
                time_fichero_run = item[pos1+1:pos2]
                dt = datetime.fromtimestamp(float(time_fichero_run), tz=timezone.utc)
                datetime_str = str(dt)

            else:
                time_fichero_run = ''
            reg={
                'file': FICHERO,
                'time': time_fichero_run,
                'datetime': datetime_str
            }
            writer.writerow(reg)

def volcar_lista_labels(lista_labels, FICHERO_lista_labels):
    cabeceras=['label']
    #lista_labels.sort()
    with open(FICHERO_lista_labels, 'w', encoding='utf-8',newline='') as f:
        writer = csv.DictWriter(f, fieldnames=cabeceras,quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for item in lista_labels:        
            reg={'label': item} 
            writer.writerow(reg)

def volcar_lista_tipo_ficheros(lista_tipo_ficheros, FICHERO_lista_tipo_ficheros):
    cabeceras=['file_type','data_type','name']
    with open(FICHERO_lista_tipo_ficheros, 'w', encoding='utf-8',newline='') as f:
        writer = csv.DictWriter(f, fieldnames=cabeceras,quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for item in lista_tipo_ficheros:
            if item.rfind('.log')>=0:
                tipo_dato='log'
                name=item.replace('.log','')
            elif item.find('metric_')>=0:
                tipo_dato='metric'
                name=item.replace('metric_','').replace('.json','')
                pos=item.rfind('.')
                if pos>0:
                    name=name[:pos]
            elif item.find('traces_')>=0:
                tipo_dato='traces'
                name=item.replace('traces_','')
                pos=item.rfind('.')
                if pos>0:
                    name=name[:pos]
            else:                
                tipo_dato='other'
                nane=''
            reg={
                'file_type': item,
                'data_type': tipo_dato,
                'name'     : name
            }   
            writer.writerow(reg)

def obtener_ficheros_del_tar_interno(ruta_zip, nombre_tar_interno):
    """
    Abre un archivo ZIP, localiza un archivo TAR en su raíz,
    y devuelve una lista ordenada con las rutas de todos sus ficheros internos.
    """
    lista_rutas_ficheros = []
    
    try:
        # 1. Abrimos el ZIP raíz
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            # 2. Extraemos los bytes del TAR interno en memoria
            with z.open(nombre_tar_interno) as tar_bytes:
                tar_stream = io.BytesIO(tar_bytes.read())
                
                # 3. Mapeamos el flujo de bytes como un archivo TAR
                with tarfile.open(fileobj=tar_stream, mode='r:*') as t:
                    # 4. Extraemos el nombre de cada archivo válido
                    for miembro in t.getmembers():
                        if miembro.isfile():
                            lista_rutas_ficheros.append(miembro.name)
                            
    except (zipfile.BadZipFile, tarfile.TarError, KeyError) as e:
        print(f"❌ Error al procesar los contenedores: {e}")
        return []  # Devuelve una lista vacía si algo falla para no romper el programa
        
    # Devolvemos la lista perfectamente ordenada
    lista_rutas_ficheros.sort()
    return lista_rutas_ficheros

def procesar_lista_ficheros(lista_ficheros):
  
    lista_labels=[]
    lista_tipo_ficheros=[]

    for item in lista_ficheros:
        #print(item)
        pos1=item.find('/')
        pos2=item.find('/',pos1+1)
        pos3=item.rfind('/')
        label=item[pos1+1:pos2]
        if label not in lista_labels:
            lista_labels.append(label)
        tipo_fichero=item[pos3+1:]
        if tipo_fichero not in lista_tipo_ficheros:
            lista_tipo_ficheros.append(tipo_fichero)
    return lista_labels, lista_tipo_ficheros
        


if __name__ == "__main__":
    load_dotenv()
    RUTA_DATASET = os.getenv("RUTA_DATASET")
    FOLDER_SALIDA = os.getenv("FOLDER_SALIDA", "result")
    FICHERO_lista_ficheros_run  = FOLDER_SALIDA + "/files_run.csv"
    FICHERO_lista_labels        = FOLDER_SALIDA + "/labels.csv"
    FICHERO_lista_tipo_ficheros = FOLDER_SALIDA + "/file_types.csv"  

    if RUTA_DATASET and os.path.exists(RUTA_DATASET):
        if True:
            lista_ficheros_run=listar_raiz_zip(RUTA_DATASET)
            volcar_lista_ficheros_run(lista_ficheros_run, FICHERO_lista_ficheros_run)

            #exploramos todos los ficheros dentro de una ejecucion(run)
            un_fichero_tar=lista_ficheros_run[0]
            lista_ficheros=obtener_ficheros_del_tar_interno(RUTA_DATASET, un_fichero_tar)
            lista_labels, lista_tipo_ficheros=procesar_lista_ficheros(lista_ficheros)
            volcar_lista_labels(lista_labels, FICHERO_lista_labels)
            volcar_lista_tipo_ficheros(lista_tipo_ficheros, FICHERO_lista_tipo_ficheros)


    else:
        print("❌ Revisa la ruta en tu archivo .env")