from timescale import TimescaleDBManager
from dotenv import load_dotenv
from sqlalchemy import create_engine
import pandas as pd
import json
import os
import io
import sys


def cargar_tabla_escalado(db, lista_data):
    tabla='t_escalado'
    lista_campos=['tabla','execution_name','metric_name','media','desv_std']

    if not db.comprobar_conexion():
        print("❌ [ERROR] Sin conexión activa.")
        return False

    # Creamos un archivo de texto virtual en la memoria RAM
    fichero_virtual = io.StringIO()

    
    for data in lista_data:
        data_tmp=[]
        for campo in lista_campos:
            valor =data.get(campo,'')
            data_tmp.append(valor)
        linea = '\t'.join(str(valor) for valor in data_tmp) + '\n'


        fichero_virtual.write(linea)
        #break

    # Volvemos al principio del fichero virtual para que Postgres pueda leerlo desde el inicio
    fichero_virtual.seek(0)

    # 3. Lanzamos el comando COPY directo al motor
    campos_str = ', '.join(lista_campos)
    SQL = f"""
        COPY {tabla} ({campos_str}) 
        FROM STDIN WITH DELIMITER AS '\t' NULL AS '\\N';
    """

    try:
        with db.connection:
            with db.connection.cursor() as cursor:
                cursor.copy_expert(sql=SQL, file=fichero_virtual)
        print(f"⚡ [COPY OK] Volcados {len(lista_data)} registros por flujo directo a TimescaleDB.")
        return True
    except Exception as e:
        print(f"❌ [ERROR] Fallo en el volcado COPY: {e}")
        db.connection.rollback()
        print(lista_campos)
        print(data)
        print(data_tmp)
        print(linea)
        sys.exit(1)
        return False
   
    finally:
        fichero_virtual.close()

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


if __name__ == "__main__":
    load_dotenv()
    user_env        = os.getenv("USER_DB")
    pass_env        = os.getenv("PASS_DB")
    host_env        = os.getenv("HOST_DB", "localhost")
    port_env        = os.getenv("DB_PORT_EXPOSED", "5432")
    db_env          = os.getenv("NAME_DB", "tfm_db")
    FOLDER_SALIDA   = os.getenv("FOLDER_SALIDA")
    #FOLDER_TMP      = os.getenv("FOLDER_TMP")
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

    lista_ejecuciones_sel = get_lista_ejecuciones_sel(engine, FOLDER_SALIDA)
    lista_resultados = []
    N_ejecucion=0
    tabla='metricas_transformed'
    db.truncate_table('t_escalado')

    for una_ejecucion in lista_ejecuciones_sel:
        N_ejecucion+=1
        print(una_ejecucion)

        SQL = f"""
            WITH datos AS (
                SELECT
                    execution_name,
                    metric_name,
                    metric_value,
                    AVG(metric_value) OVER (
                        PARTITION BY execution_name, metric_name
                    ) AS media
                FROM {tabla}
                WHERE label = 'correct'
                AND execution_name = '{una_ejecucion}'
            )
            SELECT
                '{tabla}' AS tabla,
                execution_name,
                metric_name,
                AVG(metric_value) AS media,
                SQRT(
                    SUM(POWER(metric_value - media, 2))
                    / (COUNT(*) - 1)
                ) AS desv_std,
                COUNT(*) AS n
            FROM datos
            GROUP BY execution_name, metric_name
            HAVING COUNT(*) > 1;
        """


        resultados_db=db.ejecutar_select_generica(SQL)
        for reg in resultados_db:
            if reg['desv_std']==0.0:
                reg['desv_std']=1.0 

        lista_resultados.extend(resultados_db)
        cargar_tabla_escalado(db, resultados_db)
        # if N_ejecucion >=2:
        #     break

    df_escalado_metricas_transformed = pd.DataFrame(lista_resultados)
    df_escalado_metricas_transformed.to_parquet(f'{FOLDER_SALIDA}/df_escalado_metricas_transformed.parquet', index=False,engine='fastparquet')






    