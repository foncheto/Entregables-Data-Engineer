# Importo librerias
# pip install psycopg2 pandas python-dotenv
import pandas as pd
import psycopg2
import os
import requests
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()

# Se cargan las credenciales, a partir de un archivo .env, de la base de datos y se realiza la conexión.

CODER_REDSHIFT_HOST = os.environ.get('CODER_REDSHIFT_HOST')
CODER_REDSHIFT_DB = os.environ.get('CODER_REDSHIFT_DB')
CODER_REDSHIFT_USER = os.environ.get('CODER_REDSHIFT_USER')
CODER_REDSHIFT_PASS = os.environ.get('CODER_REDSHIFT_PASS')
CODER_REDSHIFT_PORT = os.environ.get('CODER_REDSHIFT_PORT')

# Nos conectamos a la base de datos
try:
    conn = psycopg2.connect(
        host=CODER_REDSHIFT_HOST,
        dbname=CODER_REDSHIFT_DB,
        user=CODER_REDSHIFT_USER,
        password=CODER_REDSHIFT_PASS,
        port=CODER_REDSHIFT_PORT,

    )
    print("Connected to Redshift successfully!")
    
except Exception as e:
    print("Unable to connect to Redshift.")
    print(e)

# Se realiza la consulta a la API de Alphavantage para obtener los datos de las acciones de Apple (AAPL) y Amazon (AMZN) en el año 2020.
# La llave es gratuita y se puede obtener en https://www.alphavantage.co/support/#api-key
# En este caso se almacena en el archivo .env para mayor seguridad, bajo el nombre ALPHAVANTAGE_API_KEY
alphavantage_api_key = os.environ.get('ALPHAVANTAGE_API_KEY')

def get_json(symbol):
    # Se realiza la petición a la API de Alpha Vantage con el símbolo de la acción y la llave de la API
    # La llave de la API se encuentra en el archivo .env y es gratuita, se puede obtener en https://www.alphavantage.co/support/#api-key.
    # AL no tener opciones de pago, la API solo permite 5 peticiones por minuto y 500 peticiones por día.
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY_ADJUSTED&symbol={symbol}&interval=5min&apikey={alphavantage_api_key}"
    r = requests.get(url)
    print(r)
    return r.json()
def json_a_diccionario(json, symbol):
    # Se crea un diccionario vacío
    diccionario = {}
    # Se itera sobre las llaves del json
    for llave in json.keys():
        diccionario[llave] = json[llave]

    dictionaryList = json['Monthly Adjusted Time Series']

    dictWithAllRecords = {}

    for date in dictionaryList:
        open = dictionaryList[date]['1. open']
        high = dictionaryList[date]['2. high']
        low = dictionaryList[date]['3. low']
        close = dictionaryList[date]['4. close']
        adjusted_close = dictionaryList[date]['5. adjusted close']
        volume = dictionaryList[date]['6. volume']
        dividend_amount = dictionaryList[date]['7. dividend amount']
        symbol = symbol
        record = {
            'open': open,
            'high': high,
            'low': low,
            'close': close,
            'adjusted_close': adjusted_close,
            'volume': volume,
            'dividend_amount': dividend_amount,
            'symbol': symbol,
            'date': date,
        }
        dictWithAllRecords[date] = record
    return dictWithAllRecords
def format_json(json, symbol): 
    # Se crea un DataFrame en Pandas a partir del json y se transpone para que las columnas sean los datos y las filas los días
    df = pd.DataFrame(json['Monthly Adjusted Time Series']).T
    # Se cambian los nombres de las columnas para que no tengan enumeración
    df.rename(columns=lambda x: x[3:], inplace=True)
    # Agregar columna index
    df['date'] = df.index
    df.reset_index(drop=True, inplace=True)
    # Se definen los tipos de datos de las columnas ya que naturalmente son todos strings VARCHAR
    df['date'] = pd.to_datetime(df['date'])
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['adjusted close'] = pd.to_numeric(df['adjusted close'])
    # Cambiar volumen a millones ya que es un número muy grande y no se puede almacenar en un INT
    df['volume'] = pd.to_numeric(df['volume'])
    df['volume'] = round(df['volume'] / 1000000)
    df['volume'] = df['volume'].astype(int)
    df['dividend amount'] = pd.to_numeric(df['dividend amount'])
    #Se agrega la columna symbol con el símbolo de la acción
    df['symbol'] = symbol
    df['symbol'] = df['symbol'].astype(str)
    #Se devuelve el DataFrame ya transformado
    return df

# Conexio a API y posterior transformacion a lista de diccionarios con nombres de futurascolumnas
data = get_json('AAPL')
dict = json_a_diccionario(data, 'AAPL')
print(f"Ejemplo de una entrada del diccionario: {dict['2000-04-28']}")

df_aapl = format_json(data, 'AAPL')

# Amazon
data = get_json('AMZN')
df_amzn = format_json(data, 'AMZN')
data = get_json('GOOG')
df_goog = format_json(data, 'GOOG')
data = get_json('MSFT')
df_msft = format_json(data, 'MSFT')
data = get_json('IBM')
df_ibm = format_json(data, 'IBM')

print(df_aapl.dtypes)

def crear_tabla_redshift(nombre_tabla):
    try:
        # Se crea la tabla en Redshift con el nombre de la acción
        cursor = conn.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {nombre_tabla};")
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {nombre_tabla} ("open" FLOAT, "high" FLOAT, "low" FLOAT, "close" FLOAT, "adjusted close" FLOAT, "volume" INT, "dividend amount" FLOAT, "date" TIMESTAMP, "symbol" VARCHAR(255));
        """)
        conn.commit()
        cursor.close()
        print("Tabla creada exitosamente")
    except Exception as e:
        print("Error creating table")
        print(e)

# Se crea la tabla en Redshift con el nombre de la acción para futuros insert
crear_tabla_redshift('AAPL')

def cargar_en_redshift(conn, table_name, dataframe):
    # Funcion para cargar un dataframe en una tabla de redshift, creando la tabla si no existe
    # Definir formato tipos de datos SQL
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    print(cols)
    tipos = list(dtypes.values)
    type_map = {
        'float64': 'FLOAT',
        'int32': 'INT',
        'datetime64[ns]': 'TIMESTAMP',
        'object': 'VARCHAR(255)'
    }
    # Definir formato TIPO_DATO revisando el tipo de dato de cada columna del dataframe
    sql_dtypes = [type_map.get(str(dtype), 'VARCHAR(255)') for dtype in tipos]

    # Definir formato COLUMNA TIPO_DATO
    column_defs = [f'"{name}" {data_type}' for name, data_type in zip(cols, sql_dtypes)]

    # Combina las columnas y los tipos de datos en una sola cadena de SQL para crear la tabla con todas la columnas necesarias
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)});
        """
    print(table_schema)

    # Crear la tabla
    cur = conn.cursor()
    try:
        # Se ejecuta el comando para crear la tabla creado anteriormente
        cur.execute(table_schema)

        # Generar los valores a insertar
        values = [tuple(x) for x in dataframe.values]

        # Definir el INSERT con las columnas a insertar
        insert_sql = f"INSERT INTO {table_name} (\"open\", \"high\", \"low\", \"close\", \"adjusted close\", \"volume\", \"dividend amount\", \"date\", \"symbol\") VALUES %s"

        # Execute the transaction to insert the data
        cur.execute("BEGIN")
        execute_values(cur, insert_sql, values)
        cur.execute("COMMIT")
        print('Proceso terminado')
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()  # Rollback the transaction on error

def drop_table(conn, table_name):
    # Funcion para eliminar una tabla y asi poder volver a crearla sin problemas de sobreescritura
    cur = conn.cursor()
    try:
        cur.execute(f"DROP TABLE IF EXISTS {table_name}")
        conn.commit()
        print('Proceso terminado')
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()  # Rollback the transaction on error

# Se elimina la tabla si existe
drop_table(conn=conn, table_name='apple_example')

# Prueba de la función de carga de datos con la tabla de solo el simbolo AAPL
cargar_en_redshift(conn=conn, table_name='apple_example', dataframe=df_aapl)

# Se concatenan los DataFrames de cada acción en uno solo con distintos símbolos
result_df = pd.concat([df_aapl, df_amzn, df_goog, df_msft, df_ibm], ignore_index=True)
# Se ordena el DataFrame por fecha de forma descendente
result_df = result_df.sort_values(by=['date'], ascending=False)
result_df = result_df.reset_index(drop=True)
result_df.head(10)

# Se elimina la tabla si existe para evitar problemas de sobreescritura
drop_table(conn=conn, table_name='monthly_stocks_over_time')

cargar_en_redshift(conn=conn, table_name='monthly_stocks_over_time', dataframe=result_df)