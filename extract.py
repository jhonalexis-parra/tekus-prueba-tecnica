import pandas as pd
import numpy as np
import os
import glob2
import pymssql


def data_csv():
    ruta_completa = os.getcwd()
    ruta_completa = ruta_completa + '\data'

    list_data = []

    for dirpath, dirnames, filenames in os.walk(ruta_completa):
        csv_files = glob2.glob(dirpath + '\*.csv')
        for filename in csv_files:
            data = pd.read_csv(filename)
            data = data[['Duration', 'MovementDuration', 'MovementInteractions', 'HardwareInteractions','Key', 'Date']]
            list_data.append(data)

    df = pd.concat(list_data,ignore_index=True)
    return df

def data_mysql():

    # Datos de logging
    server_tekus = "proyectos.tekus.co"
    port_tekus = "1433"
    user_tekus = "datatest"
    password_tekus = "9cUQ*48AAX8Q"
    data_base_tekus = "DataTest"


    #conexión con servidor
    conn = pymssql.connect(server = server_tekus, user = user_tekus, password = password_tekus, port = port_tekus, database = data_base_tekus)

    #Extraer tabla ciudades
    cursor_cities = conn.cursor()
    cursor_cities.execute("SELECT * FROM Cities")
    cities = cursor_cities.fetchall()

    #Extraer tabla ollas
    cursor_pots = conn.cursor()
    cursor_pots.execute("SELECT * FROM Pots")
    pots = cursor_pots.fetchall()


    #Conversión a DataFrame
    pots = pd.DataFrame(pots, columns = ['Key', 'Serial', 'CityId'])
    cities = pd.DataFrame(cities, columns = ['CityId', 'Name'])

    #Unificación de tablas y selección de ciudades
    pots_cities = pots.merge(cities, on='CityId', how = 'inner')
    pots_cities = pots_cities.drop(['Serial', 'CityId'], axis= 1)   
    return pots_cities
    

def run():
    df_datos_csv = data_csv()

    df_datos_mysql = data_mysql()

    df_final = df_datos_csv.merge(df_datos_mysql, how= 'inner', on='Key')

    print(type(df_final))


    df_final.to_csv('data_to_transform/df.csv', index = False)

    print('Extracted Data')


if __name__ == '__main__':
    run()