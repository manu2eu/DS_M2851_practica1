# Importar modulos
from datetime import timedelta, date
import pandas as pd
import requests
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import numpy as np

num_registros = 12  # numero de registros que vamos a recuperar.
path = 'C:/Users/usuario/'


# path = 'C:\Users\elena\'

def get_city_url(city_name: str) -> str:
    """Get the url where to make the api request"""
    print(f"Getting info for {city_name} city")
    return "https://www.meteosat.com/tiempo/" + city_name + "/tiempo-" + city_name + ".html"


def make_request(url: str):
    """Make http request to provided url"""
    print("make_request")

    return requests.get(url)


def _extract_info_from_dataframe(tabla_datos, soap):
    horas = 0  # horas
    temperatures = 0  # temp
    wind = 0  # viento
    wind_speed = 0  # Velocidad viento
    precipatations = 0  # precipitaciones

    horaList = list()
    tempList = list()
    dirVientoList = list()
    velVientoList = list()
    precipitacionesList = list()

    ###get tag locality
    locality = soap.find("meta", {"name": "locality"})['content']
    locality = locality.split()[0]  # We keep only the name of the city (removed ", Province Spain")
    locality = locality.replace(',', '')  # Comma remmoved
    # print("locality:::", locality)  # d.find("meta content", {"name":"locality"}))
    cityList = list(
        [locality] * (num_registros + 1))  # create a list , with the same "locality" and lenght "num_registros"
    print("cityList:::", cityList)

    # recorremos el objeto forecast
    for d in soap.find_all(id='forecast'):

        # Recorremos  los objetos de la clase "col2" en el que almacena la hora
        for col2 in d.find_all('th', class_='col2'):
            if 'colspan' not in col2.attrs:  # Se observa que existen registros que contienen el atributo colspan, que no se quieren recurperar
                horaList.append(col2.text)  # vamos incluyendo los valores en una lista
                horas += 1
                if (horas > num_registros):  # cuando superamos el numero de registros deseados salimos del loop
                    break

        # Recorremos  los objetos de la clase "pluss col4" que almacena la temperatura
        for plusCol4 in d.find_all('td', class_='pluss col4'):

            tempList.append(plusCol4.text)  # vamos incluyendo los valores en una lista

            temperatures += 1
            if (temperatures > num_registros):  # cuando superamos el numero de registros deseados salimos del loop
                break

        # Recorremos  los objetos de la clase "v col5" que almacena la dirección del viento
        for col5 in d.find_all('td', class_='v col5'):

            dirVientoList.append(col5.text)  # vamos incluyendo los valores en una lista
            wind += 1

            if (wind > num_registros):  # cuando superamos el numero de registros deseados salimos del loop
                break

        # Recorremos  los objetos de la clase "col6" que almacena la velocidad del viento
        for col6 in d.find_all('td', class_='col6'):

            velVientoList.append(col6.text)  # vamos incluyendo los valores en una lista
            wind_speed += 1
            if (wind_speed > num_registros):  # cuando superamos el numero de registros deseados salimos del loop
                break

        city = d.find('th', class_='v col1').text
        # print("city:::", d.find('th', class_='v col1').text)

        # Recorremos  los objetos de la clase "col7" que almacena la cantidad de precipitaciones
        for col7 in d.find_all('td', class_='col7'):

            precipitacionesList.append(col7.text)  # vamos incluyendo los valores en una lista
            precipatations += 1
            if (precipatations > num_registros):  # cuando superamos el numero de registros deseados salimos del loop
                break

    fechaList = list()  # lista para las fechas
    diaAux = date.today()  # variable para fechas.

    # recorremos la lista de horas para informar la lista de fechas
    # cuando llegamos a la hora 00 , a�adimos un dia
    for h in horaList:
        print(h)
        if '00' in h:
            # print ("Son las doce cambiamos de dia")
            diaAux = diaAux + timedelta(days=1)

        fechaList.append(diaAux.strftime("%d/%m/%Y"))  # vamos incluyendo los valores en una lista

    # generamos un dataframe con las listas de valores recuperadas.
    df = pd.DataFrame({
        'locality': cityList,
        'fecha': fechaList,
        'hora': horaList,
        'temperatura': tempList,
        'dirViento': dirVientoList,
        'velViento': velVientoList,
        'precipitaciones': precipitacionesList
    })

    return df


def parse_data(response):
    """"""
    # Analizar sintacticamente el archivo HTML de BeautifulSoup del texto fuente
    soap = BeautifulSoup(response.text, 'html.parser')
    ##print ("parse_data:: prueba soap" ,soap)



    tabla_datos = soap.find_all(id='forecast')
    print ("long de tabla : tabla_datos :forecast", tabla_datos)
    print("long dforecast", len(tabla_datos))
    if len(tabla_datos) > 0:
        df = _extract_info_from_dataframe(tabla_datos, soap)
        # print ("parse_data::::", df)

    else:
        print("No traemos datos")
        df = pd.DataFrame()


    return df


def write_dataframe_to_csv(nameFile,df: pd.DataFrame) -> None:
    """Write Pandas DataFrame into disk"""
    df.to_csv(path+nameFile , index=False, encoding='utf-16', sep=';')


def get_CitiesAvailable ():
    url = "https://www.meteosat.com/"
    response = requests.get(url)
    soap = BeautifulSoup(response.text, 'html.parser')
    linksCitiesAvailable = list()
    listCitiesAvailable = list()

    for d in soap.find_all('div', attrs={'class': 'clearfix'}):
        # print (datosSeleccioneProvincia)
        for li in d.find_all('li'):
            print(li.text)
            listCitiesAvailable.append(li.text)
            # print(li)
            if li.find('a'):
                a = li.find('a')
                print(a['href'])
                linksCitiesAvailable.append(a['href'])

    df_CitiesAvailable = pd.DataFrame({
        'ciudad': listCitiesAvailable,
        'link': linksCitiesAvailable
    })
    print (df_CitiesAvailable)
    write_dataframe_to_csv('citiesAvailable.csv', df_CitiesAvailable)



def main():
    # Lista de ciudades que queremos visitar
    cities = ['barcelona', 'madrid', 'sevilla', 'malaga']
    cities_urls = [get_city_url(city) for city in cities]
    data = pd.DataFrame()

    df_list = list()
    # retrieve data
    for city_url in cities_urls:
        response = make_request(city_url)

        print ("response status" , response.status_code)
        # parse data
        df = parse_data(response)
        # data.append(df)
        data = pd.concat([data, df])
    # print("data : ", data)
    write_dataframe_to_csv('data.csv',data)

    #funcion para generar listado de ciudades disponibles en la web.
    get_CitiesAvailable()

    # Degree Celcius symbol removed
    data['temperatura'] = data['temperatura'].str.split(r"°", expand=True)
    # Temperatures set as numeric
    data['temperatura'] = pd.to_numeric(data['temperatura'])

    # km/h Removed
    data['velViento'] = data['velViento'].str.split(r"km/h", expand=True)
    # Wind Speed set as numeric
    data['velViento'] = pd.to_numeric(data['velViento'])

    # l/m2 Removed (precipitaciones)
    data['precipitaciones'] = data['precipitaciones'].str.split(r"l/m2", expand=True)
    data['precipitaciones'] = data['precipitaciones'].str.replace(',', '.')

    # Precipiaciones set as numeric
    data['precipitaciones'] = pd.to_numeric(data['precipitaciones'], errors='ignore')

    # # horas Removed (h)
    data['hora'] = data['hora'].str.split(r"h", expand=True)

    data['fechaHora'] = data['fecha'] + '-' + data['hora']

    print ("data, despues de cambios unidades: ",data)

    # data group by and sum of temperature
    df = data.groupby(['fechaHora', 'locality']).sum()['temperatura']

    # plot the result
    df.unstack().plot()
    plt.xticks(rotation=60)
    #plt.xticks(range(len(df.index)), df.index)
    plt.suptitle("Temperatura (ºC) prevista por hora")
    plt.tight_layout()
    plt.show()

    # data group by and sum of speed of wind
    df = data.groupby(['fechaHora', 'locality']).sum()['velViento']

    # plot the result
    df.unstack().plot()
    plt.xticks(rotation=60)
    plt.suptitle("Velocidad del viento (km/h) prevista por hora")
    plt.tight_layout()
    plt.show()

    # data group by and sum of rain
    df = data.groupby(['fechaHora', 'locality']).sum()['precipitaciones']

    # plot the result
    df.unstack().plot()
    plt.xticks(rotation=60)
    plt.suptitle("Precipitaciones (l/m2) prevista por hora")
    plt.tight_layout()
    plt.show()




if __name__ == '__main__':
    main()
