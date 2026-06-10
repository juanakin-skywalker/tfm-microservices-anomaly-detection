import os
import zipfile
import tarfile
import io
from dotenv import load_dotenv
import json

# 1. Cargamos las variables del archivo .env oculto
load_dotenv()
RUTA_DATASET = os.getenv("RUTA_DATASET")

# Extensiones de archivos finales que queremos auditar
EXTENSIONES_PERMITIDAS = ('.csv', '.json', '.log')



class RecopiladorResultados:
    """
    Clase para almacenar y mostrar los resultados de forma estructurada.
    Actualmente no se utiliza, pero se deja preparada para futuras mejoras.
    """
    def __init__(self,file_path):
        self.resultados = []
        self.f=open(file_path, 'w', encoding='utf-8')

    def agregar_resultado(self, ruta, tamaño_bytes, num_lineas):
        ruta_split  = ruta.split('/')
        run         = ruta_split[0]
        file_type   = ruta_split[-1]
        label       = ruta_split[1]

        reg={
            'run': run,
            'file_type': file_type,
            'label': label,
            'path': ruta,
            'size': tamaño_bytes,
            'lines': num_lineas
        }
        self.resultados.append(reg)

        cadena=json.dumps(reg)
        self.f.write(cadena+'\n')


    def mostrar_resultados(self):
        print("\n📊 Resultados recopilados:")
        for res in self.resultados:
            print(f"Ruta: {res['ruta']} | Tamaño: {res['tamaño_bytes']} Bytes | Líneas: {res['num_lineas']}")


def procesar_contenedor_recursivo(flujo_bytes, nombre_objeto, nivel=0, recopilador_resultados=None):
    """
    Función ÚNICA para explorar contenedores (ZIP/TAR) de forma recursiva.
    Detecta, mide en Bytes y cuenta las líneas de cualquier archivo .csv, .json o .log.
    """
    indentacion = "  " * nivel
    nombre_lower = nombre_objeto.lower()
    
    # --- CASO 1: EL CONTENEDOR ES UN ARCHIVO ZIP ---
    if nombre_lower.endswith('.zip'):
        print(f"{indentacion}📂 [Nivel {nivel}] Explorando ZIP: '{nombre_objeto}'...")
        try:
            with zipfile.ZipFile(flujo_bytes, 'r') as z:
                for info in z.infolist():
                    nombre_interno = info.filename
                    nombre_interno_lower = nombre_interno.lower()
                    
                    # A) Fichero final válido (.csv, .json, .log)
                    if nombre_interno_lower.endswith(EXTENSIONES_PERMITIDAS):
                        print(f"{indentacion}  ├── 📄 Fichero final detectado: '{nombre_interno}'")
                        try:
                            with z.open(nombre_interno, 'r') as f:
                                wrapper = io.TextIOWrapper(f, encoding='utf-8', errors='ignore')
                                num_lineas = sum(1 for _ in wrapper)
                            print(f"{indentacion}  │   └── 📊 [RESULTADO] Tamaño: {info.file_size} Bytes | Líneas: {num_lineas}")
                            input('....')
                            recopilador_resultados.agregar_resultado(nombre_interno, info.file_size, num_lineas)    
                        except Exception as e:
                            print(f"{indentacion}  │   └── ❌ Error al leer líneas: {e}")
                    
                    # B) Compresión anidada (ZIP o TAR dentro del ZIP)
                    elif nombre_interno_lower.endswith(('.zip', '.tar', '.tar.gz', '.tgz')):
                        print(f"{indentacion}  ├── 📦 Contenedor anidado: '{nombre_interno}' -> Bajando nivel...")
                        with z.open(nombre_interno) as sub_bytes:
                            sub_stream = io.BytesIO(sub_bytes.read())
                            procesar_contenedor_recursivo(sub_stream, nombre_interno, nivel + 1, recopilador_resultados)
        except zipfile.BadZipFile:
            print(f"{indentacion}❌ Error: ZIP inválido o corrupto: '{nombre_objeto}'")

    # --- CASO 2: EL CONTENEDOR ES UN ARCHIVO TAR (.tar, .tar.gz, .tgz) ---
    elif nombre_lower.endswith(('.tar', '.tar.gz', '.tgz')):
        print(f"{indentacion}📂 [Nivel {nivel}] Explorando TAR: '{nombre_objeto}'...")
        try:
            with tarfile.open(fileobj=flujo_bytes, mode='r:*') as t:
                for miembro in t.getmembers():
                    nombre_interno = miembro.name
                    nombre_interno_lower = nombre_interno.lower()
                    
                    # A) Fichero final válido (.csv, .json, .log)
                    if miembro.isfile() and nombre_interno_lower.endswith(EXTENSIONES_PERMITIDAS):
                        print(f"{indentacion}  ├── 📄 Fichero final detectado: '{nombre_interno}'")
                        fichero_interno = t.extractfile(miembro)
                        if fichero_interno is not None:
                            try:
                                wrapper = io.TextIOWrapper(fichero_interno, encoding='utf-8', errors='ignore')
                                num_lineas = sum(1 for _ in wrapper)
                                print(f"{indentacion}  │   └── 📊 [RESULTADO] Tamaño: {miembro.size} Bytes | Líneas: {num_lineas}")
                                recopilador_resultados.agregar_resultado(nombre_interno, miembro.size, num_lineas)
                            except Exception as e:
                                print(f"{indentacion}  │   └── ❌ Error al leer líneas: {e}")
                    
                    # B) Compresión anidada (ZIP o TAR dentro del TAR)
                    elif miembro.isfile() and nombre_interno_lower.endswith(('.zip', '.tar', '.tar.gz', '.tgz')):
                        print(f"{indentacion}  ├── 📦 Contenedor anidado: '{nombre_interno}' -> Bajando nivel...")
                        fichero_sub = t.extractfile(miembro)
                        if fichero_sub is not None:
                            sub_stream = io.BytesIO(fichero_sub.read())
                            procesar_contenedor_recursivo(sub_stream, nombre_interno, nivel + 1, recopilador_resultados)
        except tarfile.TarError:
            print(f"{indentacion}❌ Error: TAR inválido o corrupto: '{nombre_objeto}'")

def comenzar_escaneo_multinivel(ruta_base, recopilador_resultados):
    print(f"🚀 Iniciando escaneo unificado multiformato en: {ruta_base}")
    print(f"📋 Formatos auditados: {EXTENSIONES_PERMITIDAS}")
    print("=" * 80)
    
    if os.path.isfile(ruta_base):
        if ruta_base.lower().endswith(('.zip', '.tar', '.tar.gz', '.tgz')):
            print(f"📦 [Disco] Detectado contenedor raíz: '{os.path.basename(ruta_base)}'")
            if ruta_base.lower().endswith('.zip'):
                procesar_contenedor_recursivo(ruta_base, ruta_base, nivel=0, recopilador_resultados=recopilador_resultados)
            else:
                with open(ruta_base, 'rb') as f_raiz:
                    procesar_contenedor_recursivo(f_raiz, ruta_base, nivel=0, recopilador_resultados=recopilador_resultados)
    else:
        for raiz, subcarpetas, ficheros in os.walk(ruta_base):
            for fichero in ficheros:
                if fichero.lower().endswith(('.zip', '.tar', '.tar.gz', '.tgz')):
                    ruta_completa = os.path.join(raiz, fichero)
                    print(f"\n📦 [Disco] Detectado contenedor en carpeta: '{fichero}'")
                    if fichero.lower().endswith('.zip'):
                        procesar_contenedor_recursivo(ruta_completa, fichero, nivel=0, recopilador_resultados=recopilador_resultados)
                    else:
                        with open(ruta_completa, 'rb') as f_raiz:
                            procesar_contenedor_recursivo(f_raiz, fichero, nivel=0, recopilador_resultados=recopilador_resultados)
                    print("-" * 80)

if __name__ == "__main__":
    mi_RecopiladorResultados = RecopiladorResultados('./result/resultados_analisis.jsonl')
    if RUTA_DATASET and os.path.exists(RUTA_DATASET):
        comenzar_escaneo_multinivel(RUTA_DATASET, mi_RecopiladorResultados)
        mi_RecopiladorResultados.mostrar_resultados()
        print("\n🏁 ¡Análisis multiformato completado con éxito!")
    else:
        print("❌ Error: Ruta del .env no válida o no encontrada.")