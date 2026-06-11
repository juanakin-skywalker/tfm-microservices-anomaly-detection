import os
import psycopg2
from psycopg2 import OperationalError
from dotenv import load_dotenv
from datetime import datetime, timezone
import json

class TimescaleDBManager:
    def __init__(self, user, password, host="localhost", port="5432", dbname="tfm_db"):
        """
        Constructor de la clase. Recibe los parámetros de conexión de forma explícita,
        lo que permite desacoplar la lógica de la base de datos de la configuración del entorno.
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.dbname = dbname
        self.connection = None

    def conectar(self):
        """Establece la conexión con la base de datos TimescaleDB"""
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
                print("🔌 [OK] Conexión establecida con TimescaleDB.")
            except OperationalError as e:
                print(f"❌ [ERROR] No se pudo conectar a la base de datos: {e}")
                self.connection = None
        return self.connection

    def comprobar_conexion(self):
        """Verifica si la conexión sigue activa ejecutando una consulta rápida"""
        if not self.connection or self.connection.closed != 0:
            return self.conectar() is not None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1;")
                cursor.fetchone()
            print("🚀 [OK] La conexión está activa y respondiendo correctamente.")
            return True
        except OperationalError:
            print("❌ [ERROR] La conexión se ha perdido.")
            return False

    def listar_tablas(self):
        """Devuelve un listado con los nombres de todas las tablas en el esquema 'public'"""
        if not self.comprobar_conexion():
            print("❌ [ERROR] No se pueden listar las tablas sin una conexión activa.")
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
            print(f"❌ [ERROR] Error al obtener el listado de tablas: {e}")
            return []

    def cerrar_conexion(self):
        """Cierra la conexión de forma segura si está abierta"""
        if self.connection and self.connection.closed == 0:
            self.connection.close()
            print("🔒 Conexión con la base de datos cerrada de forma segura.")

            
    def obtener_diagnostico_almacenamiento(self):
        """
        Analiza el almacenamiento del esquema público. Devuelve estadísticas de registros,
        tamaño total, y si la tabla es una Hypertable de TimescaleDB, detalla sus chunks
        y su estado de compresión.
        """
        if not self.comprobar_conexion():
            print("❌ [ERROR] Sin conexión para realizar el diagnóstico de almacenamiento.")
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
            print(f"❌ [ERROR] Error al calcular el almacenamiento: {e}")
            return []

    def insertar_registro(self, data):
        """
        Inserta un único registro de métrica en la hypertable 'metricas'.
        Mapea automáticamente la clave 'group' del JSON a la columna 'grupo' de la BD.
        """


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
            print(f"❌ [ERROR] Error al insertar el registro: {e}")
            return False
        



    def insertar_registros_masivos(self, lista_data, batch_size=100):
            """
            Inserta una lista de diccionarios en bloques (batches) de forma ultra rápida.
            """
            from psycopg2.extras import execute_batch
            import json
            from datetime import datetime, timezone

            if not self.comprobar_conexion():
                print("❌ [ERROR] Sin conexión activa.")
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
                print(f"🚀 [OK] Insertados {len(lista_data)} registros en bloques de {batch_size}.")
                return True
            except Exception as e:
                print(f"❌ [ERROR] Fallo en la inserción masiva: {e}")
                self.connection.rollback()
                return False
                

    def insertar_registros_copy(self, lista_data):
            """
            Inserta registros utilizando el comando COPY de PostgreSQL a través de un búfer de memoria.
            Es la forma más rápida absoluta de cargar datos.
            """
            import io
            import json
            from datetime import datetime, timezone

            if not self.comprobar_conexion():
                print("❌ [ERROR] Sin conexión activa.")
                return False

            # Creamos un archivo de texto virtual en la memoria RAM
            fichero_virtual = io.StringIO()

            for data in lista_data:
                # 1. Limpieza y preparación de datos (Igual que antes)
                time_raw = data.get("time")
                time_final = datetime.fromtimestamp(time_raw, tz=timezone.utc) if isinstance(time_raw, (int, float)) else time_raw
                # Asegurar formato ISO string para el COPY
                time_str = time_final.isoformat() if isinstance(time_final, datetime) else str(time_final)

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
                linea = f"{time_str}\t{instance_str}\t{grupo_str}\t{job_str}\t{metric_name_str}\t{metric_str}\t{tags_json}\t{label_str}\n"
                fichero_virtual.write(linea)

            # Volvemos al principio del fichero virtual para que Postgres pueda leerlo desde el inicio
            fichero_virtual.seek(0)

            # 3. Lanzamos el comando COPY directo al motor
            query = """
                COPY metricas (time, instance, grupo, job, metric_name, metric_value, tags, label) 
                FROM STDIN WITH DELIMITER AS '\t' NULL AS '\\N';
            """

            try:
                with self.connection:
                    with self.connection.cursor() as cursor:
                        cursor.copy_expert(sql=query, file=fichero_virtual)
                print(f"⚡ [COPY OK] Volcados {len(lista_data)} registros por flujo directo a TimescaleDB.")
                return True
            except Exception as e:
                print(f"❌ [ERROR] Fallo en el volcado COPY: {e}")
                self.connection.rollback()
                return False
            finally:
                fichero_virtual.close()
# =====================================================================
# BLOQUE DE EJECUCIÓN PRINCIPAL (Prueba y Diagnóstico Local)
# =====================================================================
if __name__ == "__main__":
    print("\n=== [DIAGNÓSTICO] Iniciando pruebas del módulo de Base de Datos ===")
    
    # 1. Cargar el entorno local (.env) únicamente para esta prueba scriptada
    load_dotenv()
    
    user_env = os.getenv("USER_DB")
    pass_env = os.getenv("PASS_DB")
    host_env = os.getenv("HOST_DB", "localhost")
    port_env = os.getenv("DB_PORT_EXPOSED", "5432")
    db_env   = os.getenv("NAME_DB", "tfm_db")
    
    # Validación rápida de que el archivo .env está siendo leído
    if not user_env or not pass_env:
        print("⚠️ [ALERTA] No se han detectado variables para iniciar la conexión con la base de datos.")
        print("Asegúrate de tener un archivo .env válido en la raíz de la ejecución.\n")
    
    # 2. Instanciar la clase inyectando los parámetros del constructor
    print(f"⚙️ Configurando gestor para [{user_env}@{host_env}:{port_env}/{db_env}]...")
    db = TimescaleDBManager(
        user=user_env,
        password=pass_env,
        host=host_env,
        port=port_env,
        dbname=db_env
    )
    
    # 3. Ejecutar flujo de pruebas
    print("\n[Paso 1] Intentando abrir conexión...")
    db.conectar()
    
    print("\n[Paso 2] Verificando estado de la línea...")
    db.comprobar_conexion()
    
    print("\n[Paso 3] Solicitando catálogo de tablas...")
    tablas_existentes = db.listar_tablas()

    print("\n[Paso 4] Diagnostico de almacenamiento...")
    diagnostico=db.obtener_diagnostico_almacenamiento()
    print(diagnostico)
    print()
    
    if tablas_existentes:
        print(f"📋 Éxito. Tablas mapeadas en el esquema público ({len(tablas_existentes)}):")
        for tabla in tablas_existentes:
            print(f"   🔹 {tabla}")
    else:
        print("📭 Conectado, pero el esquema público no contiene ninguna tabla base.")
        
    # 4. Finalizar de forma limpia
    print("\n[Paso 5] Cerrando canales...")
    db.cerrar_conexion()
    print("=== [DIAGNÓSTICO] Pruebas finalizadas con éxito ===\n")