import os
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import RealDictCursor
from psycopg2 import extras
from dotenv import load_dotenv
from datetime import datetime, timezone
import json
import subprocess
import gzip
import shutil
import time
from contextlib import contextmanager

import io
import sys



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


class TimescaleDBManager:
    def __init__(self, user, password, host="localhost", port="5432", dbname="tfm_db"):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.connection = None

    def conectar(self):
        if self.connection is None or self.connection.closed != 0:
            try:
                self.connection = psycopg2.connect(
                    user=self.user,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    database=self.dbname
                )
                self.connection.autocommit = True
                print("[OK] Conexión establecida con TimescaleDB.")
            except OperationalError as e:
                print(f"[ERROR] No se pudo conectar a la base de datos: {e}")
                self.connection = None
        return self.connection

    def comprobar_conexion(self,echo=False):
        if not self.connection or self.connection.closed != 0:
            return self.conectar() is not None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()
            if echo:
                print("🚀 [OK] La conexión está activa y respondiendo correctamente.")
            return True
        except OperationalError:
            if echo:
                print("❌ [ERROR] La conexión se ha perdido.")
            return False

    def obtener_conteo_por_ejecucion_dict(self):
        SQL = "SELECT execution_name, COUNT(*) as N FROM metricas GROUP BY execution_name;"
        
        try:
            # Pasamos RealDictCursor al crear el cursor para mapear automáticamente los campos
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(SQL)
                resultados = cursor.fetchall()
                
                # 'resultados' ya es directamente una lista de diccionarios de Python:
                # Ejemplo: [{'execution_name': 'ejecucion_01', 'cantidad': 1500}, ...]
                return resultados
                
        except Exception as e:
            print(f"Error al obtener el conteo de registros por ejecución: {e}")
            if self.connection:
                self.connection.rollback()
            return []  # Devolvemos una lista vacía en caso de error para mantener la consistencia del tipo de dato
                
    def truncate_table(self, tabla):
        SQL = f"TRUNCATE TABLE {tabla};"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(SQL)
                self.connection.commit()

            return True

        except Exception as e:
            print(f"Error al hacer TRUNCATE de la tabla {tabla}: {e}")

            if self.connection:
                self.connection.rollback()

            return False





    def listar_tablas(self):
        print('>>>>>>>>>>>>>>>')
        if not self.comprobar_conexion():
            print("[ERROR] No se pueden listar las tablas sin una conexión activa.")
            return []

        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE';
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return [tabla[0] for tabla in cursor.fetchall()]
        except Exception as e:
            print(f"[ERROR] Error al obtener el listado de tablas: {e}")
            return []

    def cerrar_conexion(self):
        if self.connection and self.connection.closed == 0:
            self.connection.close()
            print("Conexión con la base de datos cerrada de forma segura.")

    def obtener_diagnostico_almacenamiento_reducido(self):
        if not self.comprobar_conexion():
            print("[ERROR] Sin conexión para realizar el diagnóstico.")
            return []

        # Listado base de tablas en el esquema público
        query_tablas = """
            SELECT 
                relname AS tabla,
                n_live_tup AS registros_estimados
            FROM pg_stat_user_tables
            WHERE schemaname = 'public';
        """
        
        diagnostico_reducido = []
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_tablas)
                tablas_info = cursor.fetchall()
                
                for tabla, registros_est in tablas_info:
                    # 1. Comprobar si la tabla actual es una hypertable de TimescaleDB
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables 
                            WHERE hypertable_name = %s
                        );
                    """, (tabla,))
                    es_hypertable = cursor.fetchone()[0]
                    
                    registros_reales = registros_est
                    
                    # 2. Si es hypertable, forzamos el COUNT(*) exacto para evitar el desfase estadístico
                    if es_hypertable:
                        try:
                            cursor.execute(f'SELECT COUNT(*) FROM "{tabla}";')
                            registros_reales = cursor.fetchone()[0]
                        except Exception:
                            if self.connection:
                                self.connection.rollback()
                    
                    # Guardamos solo las tres columnas que necesitas
                    diagnostico_reducido.append({
                        "TABLA": tabla,
                        "REGISTROS": registros_reales,
                        "ES_HYPERTABLE": "SÍ" if es_hypertable else "NO"
                    })
                    
            return diagnostico_reducido
            
        except Exception as e:
            print(f"[ERROR] Error al obtener el listado de tablas: {e}")
            if self.connection:
                self.connection.rollback()
            return []
        

    def obtener_diagnostico_almacenamiento(self):
        if not self.comprobar_conexion():
            print("[ERROR] Sin conexión para realizar el diagnóstico de almacenamiento.")
            return []

        # 1. Obtener todas las tablas del usuario en el esquema público
        query_tablas = """
            SELECT 
                relname AS tabla,
                n_live_tup AS registros
            FROM pg_stat_user_tables
            WHERE schemaname = 'public';
        """
        
        diagnostico = []
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_tablas)
                tablas_info = cursor.fetchall()
                
                for tabla, registros in tablas_info:
                    # Estructura base para cada tabla
                    info_tabla = {
                        "tabla": tabla,
                        "registros": registros,
                        "es_hypertable": False,
                        "chunks_activos": 0,
                        "tamaño_total": "0 kB",
                        "detalles_chunks": []
                    }
                    
                    # 2. Verificar si es una Hypertable de TimescaleDB
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM timescaledb_information.hypertables 
                            WHERE hypertable_name = %s
                        );
                    """, (tabla,))
                    info_tabla["es_hypertable"] = cursor.fetchone()[0]
                    
                    if info_tabla["es_hypertable"]:
                        # Tamaño total real de la hypertable (incluyendo todos sus chunks e índices)
                        cursor.execute("SELECT pg_size_pretty(pg_total_relation_size(%s::regclass));", (tabla,))
                        res_size = cursor.fetchone()
                        if res_size and res_size[0]:
                            info_tabla["tamaño_total"] = res_size[0]
                        
                        # 3. Obtener los detalles reales de los Chunks con funciones nativas de Postgres
                        cursor.execute("""
                            SELECT 
                                chunk_name,
                                pg_size_pretty(pg_relation_size(chunk_schema || '.' || chunk_name::text)) as tamaño_datos,
                                pg_size_pretty(pg_indexes_size(chunk_schema || '.' || chunk_name::text)) as tamaño_indices,
                                is_compressed
                            FROM timescaledb_information.chunks
                            WHERE hypertable_name = %s;
                        """, (tabla,))
                        chunks = cursor.fetchall()
                        info_tabla["chunks_activos"] = len(chunks)
                        
                        for ch_name, ch_size, idx_size, is_comp in chunks:
                            info_tabla["detalles_chunks"].append({
                                "chunk": ch_name,
                                "tamaño_datos": ch_size,
                                "tamaño_indices": idx_size,
                                "comprimido": is_comp
                            })
                    else:
                        # Si es una tabla estándar de Postgres
                        cursor.execute("SELECT pg_size_pretty(pg_total_relation_size(%s::regclass));", (tabla,))
                        res_size = cursor.fetchone()
                        if res_size and res_size[0]:
                            info_tabla["tamaño_total"] = res_size[0]
                    
                    diagnostico.append(info_tabla)
                    
            return diagnostico
            
        except Exception as e:
            print(f"[ERROR] Error al calcular el almacenamiento: {e}")
            return []

    def insertar_vectores_batch(self,lista_datos):
        """
        datos: Lista de tuplas (execution_name, label, t_rel, vector_lista)
        """
        SQL = """
            INSERT INTO vectores (execution_name, label, t_rel, vector)
            VALUES (%s, %s, %s, %s)
        """
        result=0
        
        lista_datos_transformados = [
            (d['execution_name'], d['label'], d['t_rel'],  d['vector'])
            for d in lista_datos
        ]

        with self.connection.cursor() as cur: 
            try:
                extras.execute_batch(cur, SQL, lista_datos_transformados, page_size=1000)
                self.connection.commit()
                result = len(lista_datos)

        
            except Exception as e:
                result=0
                if self.connection:
                    self.connection.rollback()
                    
        return result

    
    def insertar_datos_batch_generico(self, table_name, lista_diccionarios, page_size=1000):
        """Inserta de forma masiva una lista de diccionarios en cualquier tabla de PostgreSQL.
    
        :param table_name: Nombre de la tabla (ej. 'vectores_split')
        :param lista_diccionarios: Lista de diccionarios con las claves exactas de las columnas
        """
        if not lista_diccionarios:
            return 0
    
        # 1. Extraemos las columnas automáticamente del primer diccionario
        columnas = list(lista_diccionarios[0].keys())
        
        table = f"public.{table_name}" if "." not in table_name else table_name
        columnas_str = ", ".join(columnas)
        
        # 2. CAMBIO CLAVE: Usamos un único '%s' para que execute_values maneja los bloques
        sql = f"""
            INSERT INTO {table} ({columnas_str})
            VALUES %s
        """
    
        # 3. Transformamos la lista de diccionarios en tuplas
        lista_datos_transformados = [
            tuple(d[col] for col in columnas) 
            for d in lista_diccionarios
        ]
    
        result = 0
        with self.connection.cursor() as cur:
            try:
                # execute_values insertará el bloque usando el único '%s' definido arriba
                extras.execute_values(cur, sql, lista_datos_transformados, page_size=page_size)
                self.connection.commit()
                result = len(lista_diccionarios)
                print(f"Se insertaron correctamente {result} registros en {table_name}.")
    
            except Exception as e:
                result = 0
                if self.connection:
                    self.connection.rollback()
                print(f"Error al insertar en {table_name}: {e}")
                raise
    
        return result



    def insertar_registro(self, data):
        if not self.comprobar_conexion():
            print("❌ [ERROR] Sin conexión activa. No se puede insertar el registro.")
            return False

        # Usamos 'grupo' que es el nombre real de tu columna en la BD
        query = """
            INSERT INTO metricas (
                time, instance, grupo, job, metric_name, metric_value, tags, label
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            );
        """
        
        # --- 1. Conversión del tiempo (UNIX int/float a datetime) ---
        time_raw = data.get("time")
        if isinstance(time_raw, (int, float)):
            time_final = datetime.fromtimestamp(time_raw, tz=timezone.utc)
        else:
            time_final = time_raw

        # --- 2. Conversión del valor de la métrica a float ---
        metric_raw = data.get("metric_value")
        metric_final = float(metric_raw) if metric_raw is not None else None

        # --- 3. Conversión del diccionario tags a string JSON ---
        tags_raw = data.get("tags")
        tags_json = json.dumps(tags_raw) if isinstance(tags_raw, dict) else tags_raw

        # --- 4. Extracción de valores ---
        # Buscamos tanto 'grupo' como 'group' por si acaso viene de una forma u otra
        grupo_valor = data.get("grupo") if data.get("grupo") is not None else data.get("group")

        valores = (
            time_final,
            data.get("instance"),
            grupo_valor, # Va a la columna 'grupo'
            data.get("job"),
            data.get("metric_name"),
            metric_final,
            tags_json,
            data.get("label")
        )

        try:
            with self.connection.cursor() as cursor:   #al hacerlo con conextmanager(with), al salir hace el commit de forma implicita
                cursor.execute(query, valores)
            return True
        except Exception as e:
            print(f"[ERROR] Error al insertar el registro: {e}")
            return False
        



    def insertar_registros_masivos(self, lista_data, batch_size=100):
        from psycopg2.extras import execute_batch
        import json
        from datetime import datetime, timezone

        if not self.comprobar_conexion():
            print("[ERROR] Sin conexión activa.")
            return False

        query = """
            INSERT INTO metricas (
                time, instance, grupo, job, metric_name, metric_value, tags, label
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            );
        """

        # 1. Preparar y limpiar todos los datos en memoria antes de tocar la BD
        valores_procesados = []
        for data in lista_data:
            # Tiempo
            time_raw = data.get("time")
            time_final = datetime.fromtimestamp(time_raw, tz=timezone.utc) if isinstance(time_raw, (int, float)) else time_raw
            
            # Métricas y grupo
            metric_raw = data.get("metric_value")
            metric_final = float(metric_raw) if metric_raw is not None else None
            grupo_valor = data.get("grupo") if data.get("grupo") is not None else data.get("group")
            
            # Tags JSON
            tags_raw = data.get("tags")
            tags_json = json.dumps(tags_raw) if isinstance(tags_raw, dict) else tags_raw

            valores_procesados.append((
                time_final, data.get("instance"), grupo_valor, data.get("job"),
                data.get("metric_name"), metric_final, tags_json, data.get("label")
            ))

        # 2. Ejecutar la inserción por lotes dentro de una única transacción
        try:
            with self.connection:
                with self.connection.cursor() as cursor:
                    execute_batch(cursor, query, valores_procesados, page_size=batch_size)
            print(f"[OK] Insertados {len(lista_data)} registros en bloques de {batch_size}.")
            return True
        except Exception as e:
            print(f"[ERROR] Fallo en la inserción masiva: {e}")
            self.connection.rollback()
            return False
            

    def insertar_registros_copy(self, lista_data, tabla ="metricas"):

        if not self.comprobar_conexion():
            print("[ERROR] Sin conexión activa.")
            return False

        # Creamos un archivo de texto virtual en la memoria RAM
        fichero_virtual = io.StringIO()

        for data in lista_data:
            # 1. Limpieza y preparación de datos (Igual que antes)
            time_raw = data.get("time")
            time_final = datetime.fromtimestamp(time_raw, tz=timezone.utc) if isinstance(time_raw, (int, float)) else time_raw
            # Asegurar formato ISO string para el COPY
            time_str = time_final.isoformat() if isinstance(time_final, datetime) else str(time_final)

            execution_name_valor = data.get("execution_name") if data.get("execution_name") is not None else data.get("execution_name")
            execution_name_str = execution_name_valor if execution_name_valor is not None else "\\N"

            metric_raw = data.get("metric_value")
            metric_str = str(float(metric_raw)) if metric_raw is not None else "\\N" # \\N significa NULL en COPY
            
            grupo_valor = data.get("grupo") if data.get("grupo") is not None else data.get("group")
            grupo_str = grupo_valor if grupo_valor is not None else "\\N"
            
            instance_str = data.get("instance") if data.get("instance") is not None else "\\N"
            job_str = data.get("job") if data.get("job") is not None else "\\N"
            metric_name_str = data.get("metric_name") if data.get("metric_name") is not None else "\\N"
            label_str = data.get("label") if data.get("label") is not None else "\\N"

            tags_raw = data.get("tags")
            tags_json = json.dumps(tags_raw) if isinstance(tags_raw, dict) else (tags_raw if tags_raw is not None else "{}")

            # 2. Creamos una línea delimitada por tabuladores (\t) limpia
            # Es crítico que el orden coincida exactamente con las columnas que diremos en el COPY
            linea = f"{time_str}\t{execution_name_str}\t{instance_str}\t{grupo_str}\t{job_str}\t{metric_name_str}\t{metric_str}\t{tags_json}\t{label_str}\n"
            fichero_virtual.write(linea)

        # Volvemos al principio del fichero virtual para que Postgres pueda leerlo desde el inicio
        fichero_virtual.seek(0)

        # 3. Lanzamos el comando COPY directo al motor
        query = f"""
            COPY {tabla} (time, execution_name,instance, grupo, job, metric_name, metric_value, tags, label) 
            FROM STDIN WITH DELIMITER AS '\t' NULL AS '\\N';
        """

        try:
            with self.connection:
                with self.connection.cursor() as cursor:
                    cursor.copy_expert(sql=query, file=fichero_virtual)
            print(f"[COPY OK] Volcados {len(lista_data)} registros por flujo directo a TimescaleDB.")
            return True
        except Exception as e:
            print(f"[ERROR] Fallo en el volcado COPY: {e}")
            self.connection.rollback()
            return False
        finally:
            fichero_virtual.close()

    def realizar_backup(self, ruta_destino="backup_tfm.sql.gz", usar_docker=True, contenedor_name="timescaledb"):
        # Asegurar que la extensión refleje que es un archivo comprimido
        if not ruta_destino.endswith(".gz"):
            ruta_destino += ".gz"

        print(f"[BACKUP WINDOWS] Iniciando copia de seguridad de '{self.dbname}'...")

        # Configurar la contraseña en las variables de entorno efímeras
        os.environ["PGPASSWORD"] = str(self.password)
        
        if usar_docker:
            # Forzamos formato plano (-F p) para comprimir el texto SQL directamente con Python
            comando_dump = [
                "docker", "exec", "-e", f"PGPASSWORD={self.password}", contenedor_name,
                "pg_dump", "-U", self.user, "-d", self.dbname, "-F", "p",
                "--clean",          # <--- AÑADE ESTO: Incluye comandos DROP TABLE antes de CREATE
                "--if-exists"       # <--- AÑADE ESTO: Evita errores si la tabla no existía antes
            ]
        else:
            comando_dump = [
                "pg_dump", "-h", self.host, "-p", str(self.port), "-U", self.user, "-d", self.dbname, "-F", "p",
                "--clean",          # <--- AÑADE ESTO: Incluye comandos DROP TABLE antes de CREATE
                "--if-exists"       # <--- AÑADE ESTO: Evita errores si la tabla no existía antes
            ]

        try:
            # 1. Iniciamos el proceso pg_dump redirigiendo su salida a un pipe
            # En Windows, incluimos creationflags para evitar que se abran ventanas de consola molestas
            kwargs = {}
            if os.name == 'nt': # Si es Windows
                kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW

            proc_dump = subprocess.Popen(
                comando_dump, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                **kwargs
            )
            
            # 2. Abrimos el archivo de destino con la librería gzip nativa de Python (Máxima compresión: compresslevel=9)
            with gzip.open(ruta_destino, "wb", compresslevel=9) as fichero_comprimido:
                # shutil.copyfileobj lee el flujo de pg_dump en bloques de memoria y los escribe comprimidos al vuelo
                if proc_dump.stdout:
                    shutil.copyfileobj(proc_dump.stdout, fichero_comprimido)
            
            # Esperamos a que termine el proceso y capturamos posibles errores
            _, stderr_dump = proc_dump.communicate()

            # Limpieza de la variable de entorno por seguridad
            os.environ.pop("PGPASSWORD", None)

            # Validar si pg_dump falló internamente (por ejemplo, contraseña errónea o contenedor apagado)
            if proc_dump.returncode != 0:
                print(f"[BACKUP ERROR] Error en pg_dump: {stderr_dump.decode('utf-8', errors='ignore').strip()}")
                # Si falló, borramos el archivo .gz residual que se haya podido crear vacío
                if os.path.exists(ruta_destino):
                    os.remove(ruta_destino)
                return False

            # Comprobar el archivo resultante en tu máquina Windows
            if os.path.exists(ruta_destino) and os.path.getsize(ruta_destino) > 0:
                tamaño_mb = os.path.getsize(ruta_destino) / (1024 * 1024)
                print(f"[BACKUP OK] Copia comprimida con éxito en Windows: '{ruta_destino}' ({tamaño_mb:.2f} MB)")
                return True
            else:
                print("[BACKUP ERROR] El archivo comprimido se generó vacío.")
                return False

        except Exception as e:
            print(f"[BACKUP ERROR] Error inesperado en el entorno Windows: {e}")
            os.environ.pop("PGPASSWORD", None)
            return False

    def realizar_restore(self, ruta_origen="backup_tfm.sql.gz", usar_docker=True, contenedor_name="timescaledb"):
        print(f"[RESTORE WINDOWS] Iniciando restauración de '{self.dbname}' desde '{ruta_origen}'...")

        if not os.path.exists(ruta_origen):
            print(f"[RESTORE ERROR] El archivo '{ruta_origen}' no existe.")
            return False

        # Configurar la contraseña
        os.environ["PGPASSWORD"] = str(self.password)

        comando_limpieza = f"TRUNCATE TABLE metricas, metricas_transformed RESTART IDENTITY;"

        # Ejecutar el truncate antes de abrir el gzip
        subprocess.run([
            "docker", "exec", "-i", contenedor_name, "psql", "-U", self.user, "-d", self.dbname, "-c", comando_limpieza
        ], check=False)



        if usar_docker:
            # -f - lee desde stdin
            comando_restore = [
                "docker", "exec", "-i", "-e", f"PGPASSWORD={self.password}", contenedor_name,
                "psql", "-U", self.user, "-d", self.dbname
            ]
        else:
            # Para ejecución directa en local
            comando_restore = [
                "psql", "-h", self.host, "-p", str(self.port), "-U", self.user, "-d", self.dbname
            ]

        try:
            kwargs = {}
            if os.name == 'nt':
                kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW

            # 1. Iniciamos psql esperando datos por stdin
            proc_restore = subprocess.Popen(
                comando_restore,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                **kwargs
            )

            # 2. Leemos el archivo comprimido y lo enviamos al proceso psql
            with gzip.open(ruta_origen, "rb") as fichero_comprimido:
                # Escribimos el contenido descomprimido al stdin del proceso
                shutil.copyfileobj(fichero_comprimido, proc_restore.stdin)
                # IMPORTANTE: Debemos cerrar el stdin para que psql sepa que ya terminamos de recibir datos
                proc_restore.stdin.close()

            # Esperar a que termine y capturar resultados
            stdout_res, stderr_res = proc_restore.communicate()
            os.environ.pop("PGPASSWORD", None)

            if proc_restore.returncode == 0:
                print(f"[RESTORE OK] Base de datos '{self.dbname}' restaurada correctamente.")
                return True
            else:
                error_msg = stderr_res.decode('utf-8', errors='ignore').strip()
                print(f"[RESTORE ERROR] Fallo en la restauración: {error_msg}")
                return False

        except Exception as e:
            print(f"[RESTORE ERROR] Error inesperado: {e}")
            os.environ.pop("PGPASSWORD", None)
            return False

    def obtener_analisis_chunks_hipertabla(self,tabla):
        # Tu consulta exacta adaptada para pasar parámetros de forma segura
        query_analitica = f"""
            SELECT 
                c.chunk_name AS nombre_chunk,
                c.range_start AS inicio_rango,
                c.is_compressed AS comprimido,
                CASE 
                    WHEN c.is_compressed = true THEN pg_size_pretty(stats.after_compression_total_bytes)
                    ELSE pg_size_pretty(pg_total_relation_size(c.chunk_schema || '.' || c.chunk_name::text))
                END AS total_ocupacion_real,
                CASE 
                    WHEN c.is_compressed = true THEN pg_size_pretty(stats.before_compression_total_bytes)
                    ELSE pg_size_pretty(pg_total_relation_size(c.chunk_schema || '.' || c.chunk_name::text))
                END AS total_descomprimido
            FROM timescaledb_information.chunks c
            LEFT JOIN LATERAL chunk_compression_stats('metricas'::regclass) stats 
                 ON c.chunk_name = stats.chunk_name
            WHERE c.hypertable_name = '{tabla}'
            ORDER BY c.range_start DESC;
        """
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query_analitica)
                filas = cursor.fetchall()
                
                desglose_chunks = []
                for nombre, inicio, comprimido, real, descomp in filas:
                    # Formateamos la fecha de inicio del rango para que no sature la tabla en consola
                    fecha_legible = inicio.strftime('%Y-%m-%d') if inicio else "N/A"
                    
                    desglose_chunks.append({
                        "CHUNK": nombre,
                        "INICIO RANGO": fecha_legible,
                        "COMPRIMIDO": "SÍ" if comprimido else "NO",
                        "OCUPACIÓN REAL": real if real else "0 bytes",
                        "DESCOMPRIMIDO": descomp if descomp else "0 bytes"
                    })
                    
                return desglose_chunks
                
        except Exception as e:
            print(f"[ERROR] Error al calcular el desglose analítico de chunks: {e}")
            if self.connection:
                self.connection.rollback()
            return []

    def comprimir_chunks_antiguos(self, tabla):
        if not self.comprobar_conexion():
            print(f"[ERROR] Sin conexión para ejecutar la compresión de '{tabla}'.")
            return False

        # 1. Consulta analítica para listar los chunks ordenados por rango descendente
        # El primer registro devuelto siempre será el más reciente debido al ORDER BY c.range_start DESC
        query_listar_chunks = """
            SELECT 
                c.chunk_name,
                c.is_compressed
            FROM timescaledb_information.chunks c
            WHERE c.hypertable_name = %s
            ORDER BY c.range_start DESC;
        """
        
        try:
            with self.connection.cursor() as cursor:
                # Comprobamos primero si existen chunks para la tabla
                cursor.execute(query_listar_chunks, (tabla,))
                chunks = cursor.fetchall()
                
                if not chunks:
                    print(f"[INFO] No se encontraron chunks para la tabla '{tabla}'.")
                    return True
                
                if len(chunks) == 1:
                    print(f"[INFO] La tabla '{tabla}' solo tiene 1 chunk (el actual). No se requiere comprimir nada.")
                    return True
                
                # El primer chunk es el más reciente (debido al ordenamiento descendente)
                chunk_reciente_name = chunks[0][0]
                chunks_a_procesar = chunks[1:] # Excluimos el primero, nos quedamos con los antiguos
                
                print(f"Iniciando compresión selectiva para la tabla '{tabla}'...")
                print(f"Chunk protegido (datos recientes): {chunk_reciente_name}")
                
                contador_comprimidos = 0
                
                # 2. Iteramos por los chunks antiguos para comprimirlos si no lo están ya
                for chunk_name, is_compressed in chunks_a_procesar:
                    if is_compressed:
                        # Si ya está comprimido en base de datos, lo saltamos silenciosamente
                        continue
                        
                    try:
                        print(f"Comprimiendo chunk antiguo: {chunk_name}...")
                        # Invocamos la función nativa de TimescaleDB para empaquetar el chunk
                        cursor.execute("SELECT compress_chunk(format('%%I.%%I', chunk_schema, chunk_name)::regclass) FROM timescaledb_information.chunks WHERE chunk_name = %s LIMIT 1;", (chunk_name,))
                        cursor.fetchone()
                        contador_comprimidos += 1
                    except Exception as error_chunk:
                        # Si falla un chunk individual, hacemos rollback de esa operación para no bloquear el bucle
                        if self.connection:
                            self.connection.rollback()
                        print(f"[Aviso] No se pudo comprimir el chunk {chunk_name}: {error_chunk}")
                
                print(f"[OK] Proceso finalizado. Se han comprimido {contador_comprimidos} chunks antiguos en '{tabla}'.")
                return True
                
        except Exception as e:
            print(f"[ERROR] Error general durante la ejecución de la compresión: {e}")
            if self.connection:
                self.connection.rollback()
            return False

    def ejecutar_select_generica(self, query, parametros=None):
        try:
            with self.connection.cursor() as cursor:
                # Ejecutamos la consulta pasándole parámetros si existen
                cursor.execute(query, parametros)
                
                # Verificamos si la consulta devuelve filas (tiene descripción de columnas)
                if cursor.description is None:
                    return []
                
                # Extraemos los nombres de las columnas para construir el diccionario
                columnas = [desc[0] for desc in cursor.description]
                filas = cursor.fetchall()
                
                resultado_estructurado = []
                for fila in filas:
                    # Creamos un diccionario asociando el nombre de la columna con su valor
                    registro = dict(zip(columnas, fila))
                    resultado_estructurado.append(registro)
                    
                return resultado_estructurado
                
        except Exception as e:
            print(f"[ERROR] Error al ejecutar la consulta genérica: {e}")
            if self.connection:
                self.connection.rollback()
            return []

def mostrar_menu(opciones):
    print("\n" + "="*35)
    print("      GESTOR TIMESCALEDB - MENU")
    print("="*35)
    for opc in opciones:
        print(f"{opc['comando']}. {opc['descripcion']}")
    print("="*35)

def conectar_db():
    db_manager=None
    user_env = os.getenv("USER_DB")
    pass_env = os.getenv("PASS_DB")
    host_env = os.getenv("HOST_DB", "localhost")
    port_env = os.getenv("DB_PORT_EXPOSED", "5432")
    db_env   = os.getenv("NAME_DB", "tfm_db")
    
    # Validación rápida de que el archivo .env está siendo leído
    if not user_env or not pass_env:
        print("[ALERTA] No se han detectado variables para iniciar la conexión con la base de datos.")
        print("Asegúrate de tener un archivo .env válido en la raíz de la ejecución.\n")
        return db_manager

    db = TimescaleDBManager(
        user=user_env,
        password=pass_env,
        host=host_env,
        port=port_env,
        dbname=db_env
    )

    db.conectar()
    result_conexion=db.comprobar_conexion(echo=True)
    if not result_conexion:
        print('La verificación de la conexión ha fallado')
        result_db=None
 

    db_manager=db
    return db_manager

def imprimir_datos(datos):

    # Caso 0: Por seguridad, si viene vacío o es None
    if not datos:
        print("+--------------------------+")
        print("| El listado está vacío.   |")
        print("+--------------------------+")
        return

    # Si nos pasan un diccionario suelto, lo metemos en una lista para unificar
    if isinstance(datos, dict):
        datos = [datos]

    # Caso 1: Si es una lista y su primer elemento es una cadena de texto (str)
    if isinstance(datos, list) and isinstance(datos[0], str):
        print(f"\n--- Listado de Elementos ({len(datos)} encontrados) ---")
        for elemento in datos:
            print(f"- {elemento}")
        print("-" * 40 + "\n")
        return

    # Caso 2: Si es una lista de diccionarios (Formato Tabla)
    if isinstance(datos, list) and isinstance(datos[0], dict):
        # Extraemos las columnas (las llaves del primer diccionario)
        columnas = list(datos[0].keys())
        
        # Calculamos el ancho óptimo de cada columna dinámicamente
        anchos = {}
        for col in columnas:
            max_longitud_valor = max(len(str(item.get(col, ''))) for item in datos)
            anchos[col] = max(len(col), max_longitud_valor)

        # Construimos las líneas decorativas horizontales (+----+-------+)
        linea_separadora = "+" + "+".join("-" * (anchos[col] + 2) for col in columnas) + "+"

        # Imprimir borde superior y cabecera
        print("\n" + linea_separadora)
        linea_cabecera = "| " + " | ".join(f"{col.upper():<{anchos[col]}}" for col in columnas) + " |"
        print(linea_cabecera)
        print(linea_separadora)

        # Imprimir cada una de las filas con sus datos
        for fila in datos:
            linea_fila = "| " + " | ".join(f"{str(fila.get(col, '')):<{anchos[col]}}" for col in columnas) + " |"
            print(linea_fila)
            
        # Imprimir borde inferior de la tabla
        print(linea_separadora + "\n")


def obtener_diagnostico_almacenamiento_reducido(db_manager):
    diagnostico=db_manager.obtener_diagnostico_almacenamiento_reducido()
    return diagnostico

def obtener_diagnostico_almacenamiento(db_manager):
    diagnostico=db_manager.obtener_diagnostico_almacenamiento()
    return diagnostico

def listar_tablas(db_manager):
    tablas_existentes = db_manager.listar_tablas()
    return tablas_existentes

def obtener_analisis_chunks_hipertabla_metricas(db_manager):
    TABLA='metricas'
    resultado=db_manager.obtener_analisis_chunks_hipertabla(TABLA)
    return resultado

def comprimir_chunks_antiguos_tabla_metricas(db_manager):
    TABLA='metricas'
    resultado=db_manager.comprimir_chunks_antiguos(TABLA)
    return resultado

def resumen_execution_name_tabla_metricas(db_manager):
    SQL='select execution_name,min(time) as MIN_TIME,max(time) as MAX_TIME,count(*) as N from metricas group by execution_name;'
    resultado=db_manager.ejecutar_select_generica(SQL)
    return resultado


def realizar_backup(db_manager):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_con_timestamp = f"backup/{timestamp}_backup_tfm_db.sql.gz"
    db_manager.realizar_backup(
        ruta_destino=ruta_con_timestamp,
        usar_docker=True,
        contenedor_name=os.getenv('TIMESCALE_CONTAINER_NAME')
    )

def realizar_restore(db_manager, ruta_fichero_backup):
    db_manager.realizar_restore(
        ruta_origen=ruta_fichero_backup,
        usar_docker=True,
        contenedor_name=os.getenv('TIMESCALE_CONTAINER_NAME')
    )

def analisis_EDA(db_manager):
    SQL = """
    SELECT 
        COUNT(*) AS total_registros,
        COUNT(*) - COUNT(metric_value) AS nulos_en_valores,
        COUNT(*) - COUNT(label) AS nulos_en_etiquetas,
        COUNT(*) - COUNT(time)  AS nulos_en_tiempo,
        COUNT(*) - COUNT(job)   AS nulos_en_job,
        COUNT(*) - COUNT(grupo) AS nulos_en_grupo
    FROM metricas;
    """
    result=db_manager.ejecutar_select_generica(SQL)
    print(result)



# =====================================================================
# BLOQUE DE EJECUCIÓN PRINCIPAL (Prueba y Diagnóstico Local)
# =====================================================================
if __name__ == "__main__":
    load_dotenv()

    db_manager= conectar_db()

    if db_manager is None:
        sys.exit()
    
    opciones_menu = [
        {"comando": "1", "descripcion": "Diagnostico de almacenamiento", "accion": obtener_diagnostico_almacenamiento_reducido,'volcar_resultado':True},
        {"comando": "2", "descripcion": "Realizar backup", "accion": realizar_backup,'volcar_resultado':False},
        {"comando": "3", "descripcion": "Restaurar backup", "accion": realizar_restore,'volcar_resultado':False},
        {"comando": "4", "descripcion": "Listado de tablas", "accion": listar_tablas,'volcar_resultado':True},
        {"comando": "5", "descripcion": "Analisis de chunks de tabla metricas", "accion": obtener_analisis_chunks_hipertabla_metricas,'volcar_resultado':True},     
        {"comando": "6", "descripcion": "Forzar compresión chunks no activos tabla(metricas)", "accion": comprimir_chunks_antiguos_tabla_metricas,'volcar_resultado':True},  
        {"comando": "7", "descripcion": "Resumen de execution_name tabla metricas", "accion": resumen_execution_name_tabla_metricas,'volcar_resultado':True},  
        {"comando": "8", "descripcion": "EDA", "accion": analisis_EDA,'volcar_resultado':False},  
        {"comando": "0", "descripcion": "Salir de la aplicación", "accion": None}
    ]  

    while True:
        mostrar_menu(opciones_menu)
        seleccion = input("Selecciona una opción: ").strip()
        opcion_elegida = next((item for item in opciones_menu if item["comando"] == seleccion), None)

        if opcion_elegida:
            if opcion_elegida["comando"] == "0":
                print("\nCerrando conexiones y saliendo del programa. ¡Adiós!")
                break
            if opcion_elegida["descripcion"] == "Restaurar backup":
                ruta_fichero_backup = input("Introduce la ruta del fichero de backup: ").strip()
                resultado = opcion_elegida["accion"](db_manager, ruta_fichero_backup)
            else:
                resultado = opcion_elegida["accion"](db_manager)
            if opcion_elegida.get('volcar_resultado',False):
                imprimir_datos(resultado)
        else:
            print(f"\nOpción '{seleccion}' no válida. Inténtalo de nuevo.")

        input("\nPresiona Enter para continuar...")


    db_manager.cerrar_conexion()



