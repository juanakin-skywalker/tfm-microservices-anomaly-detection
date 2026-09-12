# TFM: Uso de AI/ML para detección de anomalías en microservicios
Autor: Juan Antonio Alcaraz del Pino<br>
Director: Óscar Gáribo Orts<br>
Curso: 2024/25, 2.ª convocatoria<br>
Repositorio: https://github.com/juanakin-skywalker/tfm-microservices-anomaly-detection<br>


El propósito de este TFM es explorar diferentes algoritmos de detección de anomalías en un entorno de microservicios, así como de clasificar diferentes tipos de anomalías, usando tanto métricas de sistema como logs de aplicaciones.<br>

Se ha usado como fuente de datos el dataset LO2 (https://zenodo.org/records/14938118). Este dataset tiene 1740 ejecuciones diferentes. En cada ejecución se dispone de:
* 494 ficheros de métricas cuantitativas independientes, los cuales siguen el patrón de nomenclatura metric_*.json.
* 8 ficheros de trazass distribuidas, identificados bajo el patrón traces_*.csv.
* 13 ficheros de log, que incluyen la actividad del generador de carga, bajo el patrón genérico *.log.

Cada ejecución tiene 60 (15 muestras) segundos de comportamento normal etiquetados como correct y 15 segundos (3 muestras) por cada anomalía.<br>

Se ha elegido este dataset por tener datos abundantes, etiquetados y disponer tanto de métricas como de logs.

## 📁 Estructura del proyecto

A continuación se detalla la organización de directorios de este repositorio.

```text
.
├── README.md              # Documentación principal del proyecto
├── .env_ejemplo           # Fichero .env de ejemplo
├── .gitignore             # Filtro de exclusión para archivos voluminosos o temporales
├── server1/               # Registros temporales y logs de ejecución
│   ├── backup/            # Directorio donde se hacen las copias de seguridad de la base de dataos PosgreSQL/TimescaleDB
│   ├── config/            # Directorio donde se hacen las copias de seguridad de la base de dataos PosgreSQL/TimescaleDB
│   │   ├── files_run.csv  # Fichero con el nombre de las ejecuciones y su instante inicial y final
│   │   └── labels.csv     # Fichero con las etiquetas de anomlías
│   ├── data/              # Directorio donde almacenar el fichero lo2-data.zip
│   ├── log/               # Directorio de logs de las ejecuciones de scripts y notebooks (no se actualiza el contenido en el repositorio)
│   ├── postgresql_data/   # Directorio de datos del contenedor posgresql/timescaleDB (no se actualiza el contenido en el repositorio)
│   ├── result/            # Directorio de resultados los scripts y notebooks
│   │   └── models/        # Directorio de modelos de detección y clasificación de anomalías (no se actualiza el contenido en el repositorio por tener mucho tamaño los modelos)
│   ├── tmp/               # Directorio temporal (no se actualiza el contenido en el repositorio)
│   └── venv_tf/           # Directorio del entorno virtual python usado para los scripts y notebooks(no se actualiza el contenido en el repositorio)



