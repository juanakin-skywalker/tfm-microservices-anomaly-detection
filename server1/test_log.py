import os
import sys
import logging
from logging.handlers import RotatingFileHandler

def configurar_logging(carpeta_logs="logs"):
    os.makedirs(carpeta_logs, exist_ok=True)
    ruta_log = os.path.join(carpeta_logs, "pipeline_carga.log")
    
    formato = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    file_handler = RotatingFileHandler(
        ruta_log, maxBytes=10 * 1024 * 1024, backupCount=10, encoding="utf-8"
    )
    file_handler.setFormatter(formato)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formato)
    
    # Configuramos el logger raíz por si acaso, garantizando salida inmediata
    root_log = logging.getLogger()
    root_log.setLevel(logging.INFO)
    root_log.handlers = [] # Limpieza de handlers previos
    root_log.addHandler(file_handler)
    root_log.addHandler(console_handler)
    
    # Configuramos tus canales específicos
    log_pantalla_y_disco = logging.getLogger("pipeline")
    log_pantalla_y_disco.setLevel(logging.INFO)
    log_pantalla_y_disco.propagate = True # Activamos propagación temporal para asegurar

    log_pantalla_y_disco.info("📝 PRUEBA: ¡El log funciona correctamente!")

# FORZAMOS LA EJECUCIÓN DIRECTA SIN IMPEDIMENTOS
print("--- Ejecutando inicialización manual ---")
configurar_logging()
print("--- Fin de la inicialización ---")