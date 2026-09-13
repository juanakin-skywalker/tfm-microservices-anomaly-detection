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
├── README.md                                       # Documentación principal del proyecto
├── .env_ejemplo                                    # Fichero .env de ejemplo
├── .gitignore                                      # Filtro de exclusión para archivos voluminosos o temporales
├── server1/                                        # Registros temporales y logs de ejecución
│   ├── backup/                                     # Directorio donde se hacen las copias de seguridad de la base de dataos PosgreSQL/TimescaleDB
│   ├── config/                                     # Directorio donde se hacen las copias de seguridad de la base de dataos PosgreSQL/TimescaleDB
│   │   ├── files_run.csv                           # Fichero con el nombre de las ejecuciones y su instante inicial y final
│   │   └── labels.csv                              # Fichero con las etiquetas de anomlías
│   ├── data/                                       # Directorio donde almacenar el fichero lo2-data.zip
│   ├── log/                                        # Directorio de logs de las ejecuciones de scripts y notebooks (no se actualiza el contenido en el repositorio)
│   ├── postgresql_data/                            # Directorio de datos del contenedor posgresql/timescaleDB (no se actualiza el contenido en el repositorio)
│   ├── result/                                     # Directorio de resultados los scripts y notebooks
│   │   └── models/                                 # Directorio de modelos de detección y clasificación de anomalías (no se actualiza el contenido en el repositorio porque algunos modelos tienen un tamaño del orden de Gb)
│   ├── tmp/                                        # Directorio temporal (no se actualiza el contenido en el repositorio)
│   └── venv_tf/                                    # Directorio del entorno virtual python usado para los scripts y notebooks(no se actualiza el contenido en el repositorio)
├── analyze_dataset.py                              # Script que analiza el fichero lo2-data.zip y obtiene e listado de ejecuciones y labels.
├── calcular_escalado(metricas_logs).py             # Script que calcula los parámetros de escalado de series de las métricas obtenidas de los logs
├── calcular_escalado.py                            # Script que calcula los parámetros de escalado de las métricas originales
├── clasificacion_anomalias(metricas_logs).ipynb    # Clasificación de anomalías con las métricas generadas a partir de los logs
├── clasificacion_anomalias_AE.ipynb                # Clasificación de anomalias con métricas originales y Autoencoder
├── clasificacion_anomalias_IF.ipynb                # Clasificación de anomalias con métricas originales e IF
├── clasificacion_anomalias_LSTM.ipynb              # Clasificación de anomalias con métricas originales y LSTM AE
├── crear_venv_py3_11_con_tensorflow.bat            # Fichero para crear el entorno virtual de python
├── database_creation.sql                           # Fichero para crear y configurar las tablas de la base de datos PostgreSQL/TimescaleDB
├── deteccion_anomalias_AE.ipynb                    # Detección de anomalía con métricas originales y Autoencoder
├── deteccion_anomalias_IF.ipynb                    # Detección de anomalía con métricas originales y IF
├── deteccion_anomalias_LSTM.ipynb                  # Detección de anomalía con métricas originales y LSTM AE 
├── deteccion_anomalias_IF(metricas_logs).ipynb     # Detección de anomalía con métricas generadas a partir de logs e IF
├── docker-compose.yaml                             # fichero para la creación del contenedor TimescaleDB
├── EDA_datos_TFM(logs).ipynb                       # EDA de las métricas generadas a partir de logs
├── EDA_datos_TFM.ipynb                             # EDA de las métricas originales
├── load_data.py                                    # Script que realiza la carga de datos desde el fichero del dataset en la hipertabla metricas
├── load_data_log.py                                # Script que realiza la carga de datos desde el fichero del dataset en la hipertabla microservicios_logs
├── logs_preprocesado.ipynb                         # Notebook que entrena minero Drain3, calcula métricas a partir de logs usando conteo por patrones Drain3
├── requirements.txt                                # Fichero de los módulos necesarios para el entorno virtual python
├── split_data.py                                   # Script usado para separar los datos en en train, test y val
├── timescale.py                                    # Módulo que facilita el acceso a la base de datos y permite operaciones de backup y auditoría
├── transform_data.py                               # Notebook que realiza la transformación de la métricas originales (filtrado, interpolación y relleno de datos faltantes)
├── vectorizacion_datos.ipynb                       # Notebook que realiza la función de vectorizar metricas originales
└── vectorizacion_datos(metricas_logs).ipynb        # Notebook que realiza la función de vectorizar metricas generadas a partir de logs






