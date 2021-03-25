# Importar módulos
import requests
import csv
from bs4 import BeautifulSoup
from datetime import date
from datetime import timedelta, date
import datetime
import pandas as pd

# Dirección de la página web
url = "https://www.meteosat.com/tiempo/sevilla/tiempo-sevilla.html"
# Ejecutar GET-Request
response = requests.get(url)
# Analizar sintácticamente el archivo HTML de BeautifulSoup del texto fuente
soap = BeautifulSoup(response.text, 'html.parser')

tablaDatos = soap.find_all(id='forecast') # generamos un objeto soap con los datos de la "forecast"

###auxiliares para conteo
numRegistros = 10 #numero de registros que vamos a recuperar.
countHead = 0
countBody = 0
countReg = 0
countCol2 = 0 #horas
countPlusCol4 = 0 #temp
countCol5 = 0 #viento
countCol6 = 0 #Velocidad viento
countCol7 = 0 #precipitaciones

#listas para almacenar los valores.
horaList= list()
tempList= list()
dirVientoList= list()
velVientoList= list()
precipitacionesList= list()


# recorremos el objeto forecast
for d in soap.find_all(id='forecast'):
    print ("estamos en el forecast")

    ## -cabecera
    # table_thead = soap.find('thead')
    # for th in table_thead.find_all ('th'):
    #     print ("table_Head :::", countHead)
    #
    #     print (th)
    #     countHead +=1
    #     if (countHead > numRegistros):
    #         break

    #### Datos del dia.
    # table_body = soap.find('tbody')
    # for thB in table_body.find_all ('th'):
    #     print ("table_body :::", countBody)
    #
    #     print (thB)
    #     countBody +=1
    #     if (countBody > numRegistros):
    #         break

    # Recorremos  los objetos de la clase "col2" en el que almacena la hora
    for col2 in d.find_all ('th', class_ = 'col2'):
        # print ("th col2 :::", countCol2)
        #
        # print (col2)
        if 'colspan' not in col2.attrs: #Se observa que existen registros que contienen el atributo colspan, que no se quieren recurperar
            # print ("Sin collspan")
            # print(col2.text)
            horaList.append(col2.text) #vamos incluyendo los valores en una lista

            countCol2 +=1
            if (countCol2 > numRegistros): #cuando superamos el numero de registros deseados salimos del loop
                break

         ###ojo se trae la cabecera.
         ##quitar: <th class="col2" colspan="2">Temperatura</th>

    print ("horaList:::", horaList)

    #countPlusCol4

    for plusCol4 in d.find_all ('td', class_ = 'pluss col4'):
        # print ("td pluss Col4 :::", countPlusCol4)
        #
        # print (plusCol4)
        tempList.append(plusCol4.text) #vamos incluyendo los valores en una lista

        countPlusCol4 +=1
        if (countPlusCol4 > numRegistros): #cuando superamos el numero de registros deseados salimos del loop
            break

    print("tempList:::", tempList)

    # ###countCol5 = 0  # viento
    #
    for col5 in d.find_all('td', class_='v col5'):
        # print("td col 5 :::", countCol5)
        #
        # print(col5)
        dirVientoList.append(col5.text) #vamos incluyendo los valores en una lista
        countCol5 += 1

        if (countCol5 > numRegistros): #cuando superamos el numero de registros deseados salimos del loop
            break
    print("dirVientoList",dirVientoList)

    # ###countCol6 = 0 #Velocidad viento

    for col6 in d.find_all('td', class_='col6'):
        # print("td col 6 :::", countCol6)
        #
        # print(col6)
        velVientoList.append(col6.text) #vamos incluyendo los valores en una lista
        countCol6 += 1
        if (countCol6 > numRegistros): #cuando superamos el numero de registros deseados salimos del loop
            break

    print ("velVientoList",velVientoList)

    #
    # ####countCol7 = 0 #precipitaciones
    #
    for col7 in d.find_all('td', class_='col7'):
        # print("td col 7 :::", countCol7)
        #
        # print(col7)
        precipitacionesList.append(col7.text) #vamos incluyendo los valores en una lista
        countCol7 += 1
        if (countCol7 > numRegistros): #cuando superamos el numero de registros deseados salimos del loop
            break

    print ("precipitacionesList", precipitacionesList)


# print ("Preparar fechas")
fechaList= list() #lista para las fechas
diaAux= date.today() #variable para fechas.

#recorremos la lista de horas para informar la lista de fechas
#cuando llegamos a la hora 00 , añadimos un dia
for h in horaList:
    print(h)
    if '00' in h:
        # print ("Son las doce cambiamos de dia")
        diaAux = diaAux + timedelta(days=1)

    fechaList.append(diaAux.strftime("%d/%m/%Y")) #vamos incluyendo los valores en una lista


# print ("fechaList", fechaList)
# generamos un dataframe con las listas de valores recuperadas.
df = pd.DataFrame({
    'fecha': fechaList,
    'hora': horaList,
    'temperatura': tempList,
    'dirViento' : dirVientoList,
    'velViento' : velVientoList,
    'precipitaciones' : precipitacionesList

                       })



# print (df)

#Creamos un csv con los datos recuperdados.
df.to_csv(r'C:\Users\usuario\output_prediccion.csv', index = False)
