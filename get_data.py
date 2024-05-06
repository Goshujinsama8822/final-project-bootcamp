#Codigo escrito por Guadalupe Fernando Escutia Rodriguez como parte del proyecto final del Bootcamp de Ingeniería en datos de CodigoFacilito. Este codigo es open source y es distribuido bajo una licencia GLP3.

#Importamos nuestras librerías
import requests
from bs4 import BeautifulSoup as bso
import pandas as pd
import numpy as np
import os
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector as sf
import datetime

#Creamos la conexión necesaria a la base de datos
conn = sf.connect(
    user = os.environ['USER'],
    password = os.environ['PASSWORD'],
    account = os.environ['ACCOUNT'],
    role = os.environ['ROLE']
)

if not conn:
    print("No se ha podido establecer la conección con snowflake.")
    exit()

#Definimos nuestra URL con la que trabajaremos en un principio
url = 'https://sih.conagua.gob.mx'

#Hacemos petición a URL y creamos nuestra "sopa"
print('Tratando de realizar la petición a la URL...\n')
try:
    r = requests.get(url, verify=True)
    soup = bso(r.text, 'lxml')
except requests.exceptions.RequestException as e:
    print('No se ha podido obtener la URL de la búsqueda o información de la misma.\n')
    raise SystemExit(e)

#Intentamos obtener el total de enlaces en la página
print('Intentando obtener el total de enlaces en la página...\n')
try:
    tPages = soup.find_all("a", href=True)
    nPages = len(tPages)
except:
    print('No se ha podido obtener el número de enlaces.\n')
    exit()

#Iniciamos el bucle para hacer loop en las diferentes direcciones de la página
for page in tPages:
    
    #Si la dirección del enlace en cuestión coincide con el nombre del enlace de climas, hidros o presas ejecutamos el siguiente código
    if page['href'] == "climas.html" or page['href'] == "hidros.html" or page['href'] == "presas.html":
        
        #Intentamos hacer petición a la nueva URL
        print("Tratando de realizar petición a la nueva URL...\n")
        
        try:
            rs = requests.get(url + "/" + page['href'], verify=True)
            bsoup = bso(rs.text, "lxml")
        except requests.exceptions.RequestException as e:
            print("No se ha podido obtener la URL de la búsqueda o información de la misma.\n")
            raise SystemExit(e)
            
        #Intentamos obtener el total de enlaces de la nueva página
        print("Intentando obtener los enlaces de la pagina: " + url + "/" + page['href'] + "\n")
        
        try:
            newPages = bsoup.find_all("a", href=True)
        except:
            print("No se ha podido obtener los enlaces de descarga de la pagina: " + url + "/" + page['href'] + "\n")
            exit()

        #Creamos la base de datos correspondiente si no existe(Cuando se trabaja con snowflake)
        conn.cursor().execute("CREATE DATABASE IF NOT EXISTS " + page['href'][:-5].upper())

        #Seleccionamos la base de datos a utilizar
        conn.cursor().execute("USE DATABASE " + page['href'][:-5].upper())

        #Creamos el nuevo schema si no existe
        conn.cursor().execute("CREATE SCHEMA IF NOT EXISTS demo")
            
        i = 0
        yearmin = datetime.date.today().year
        yearmax = 0

        print(url + "/" + page['href'])
        
        #Iniciamos el segundo bucle de nuestro script
        for npage in newPages:
            
            #Si el enlace de descarga corresponde a un csv ejecutamos el siguiente código
            if str(npage['href'][-4:]) == ".csv":
                durl = url + "/" + npage['href']
                durl = durl.replace(" ", "%20")
                
                if i == 0:
                    try:
                        mData = pd.DataFrame()
                        mData = pd.read_csv(durl, encoding = "ISO-8859-1")
                        mData["Mínimo Historico"] = ""
                        mData["Máximo Historico"] = ""
                        mData["Promedio Historico"] = ""
                        mData["Temperatura Mínima"] = ""
                        mData['Temperatura Máxima'] = ""
                        if " Clave" in mData.columns:
                            mData.rename(columns={" Clave" : "Clave"}, inplace=True)
                        if "Clave " in mData.columns:
                            mData.rename(columns={"Clave " : "Clave"}, inplace=True)
                        i = i + 1
                        continue
                    except pd.errors.ParserError as e:
                        raise SystemExit(e)
                else:
                    try:
                        data = pd.DataFrame()
                        data = pd.read_csv(durl, encoding="utf8", header = 6)
                        if " Precipitación(mm)" in data.columns:
                            data.rename(columns={" Precipitación(mm)" : "Precipitación(mm)"}, inplace=True)
                        if "Gasto(m³/s)" in data.columns:
                            data.rename(columns={"Gasto(m³/s)" : "Precipitación(mm)"}, inplace=True)
                        if " Gasto(m³/s)" in data.columns:
                            data.rename(columns={" Gasto(m³/s)" : "Precipitación(mm)"}, inplace=True)
                        if " Temperatura Máxima(°C)" in data.columns:
                            data.rename(columns={" Temperatura Máxima(°C)" : "Temperatura Máxima(°C)"}, inplace=True)
                        if "Temperatura Máxima(°C) " in data.columns:
                            data.rename(columns={"Temperatura Máxima(°C) " : "Temperatura Máxima(°C)"}, inplace=True)
                        if " Temperatura Mínima(°C)" in data.columns:
                            data.rename(columns={" Temperatura Mínima(°C)" : "Temperatura Mínima(°C)"}, inplace=True)
                        if "Temperatura Mínima(°C) " in data.columns:
                            data.rename(columns={"Temperatura Mínima(°C) " : "Temperatura Mínima(°C)"}, inplace=True)
                        i = i + 1
                    except pd.errors.ParserError as e:
                        raise SystemExit(e)
                
                j = 0
                
                for j in range(0, len(mData.iloc[:,1])):
                    if mData.loc[mData.index[j], "Clave"] == str(npage['href'][17:]).replace(".csv", ""):
                        print("Encontrado " + str(npage['href'][17:]).replace(".csv", "") + ", procesando...")
                        data['Precipitación(mm)'] = data['Precipitación(mm)'].replace("-", np.nan)
                        data['Precipitación(mm)'] = pd.to_numeric(data['Precipitación(mm)'], errors="coerce")
                        mData.loc[mData.index[j], "Mínimo Historico"] = data[data['Precipitación(mm)'] > 0.01]["Precipitación(mm)"].min()
                        mData.loc[mData.index[j], "Máximo Historico"] = data['Precipitación(mm)'].max()
                        mData.loc[mData.index[j], "Promedio Historico"] = data['Precipitación(mm)'].mean()

                        if "Temperatura Mínima(°C)" in data.columns:
                            data["Temperatura Mínima(°C)"] = data["Temperatura Mínima(°C)"].replace("-", np.nan)
                            data["Temperatura Mínima(°C)"] = pd.to_numeric(data["Temperatura Mínima(°C)"], errors="coerce")
                            mData.loc[mData.index[j], "Temperatura Mínima"] = data[data["Temperatura Mínima(°C)"] != 0]["Temperatura Mínima(°C)"].min()
                        
                        if "Temperatura Máxima(°C)" in data.columns:
                            data["Temperatura Máxima(°C)"] = data["Temperatura Máxima(°C)"].replace("-", np.nan)
                            data["Temperatura Máxima(°C)"] = pd.to_numeric(data["Temperatura Máxima(°C)"], errors="coerce")
                            mData.loc[mData.index[j], "Temperatura Máxima"] = data["Temperatura Máxima(°C)"].max()

                        if "Temperatura Mínima(°C)" in data.columns:
                            datos = pd.DataFrame(columns=["Fecha", "Mínimo precipitación(mm)", "Máximo precipitación(mm)", "Promedio precipitación(mm)", "Temperatura Mínima", "Temperatura Máxima"])
                        else:
                            datos = pd.DataFrame(columns=["Fecha", "Mínimo precipitación(mm)", "Máximo precipitación(mm)", "Promedio precipitación(mm)"])
                        
                        datos["Fecha"] = pd.to_datetime(datos["Fecha"])

                        data['Fecha'] = pd.to_datetime(data["Fecha"])

                        x = 0
                        miny = data['Fecha'].dt.year.min()
                        maxy = data['Fecha'].dt.year.max()

                        if miny < yearmin:
                            yearmin =  miny
                        if maxy > yearmax:
                            yearmax = maxy

                        year = miny

                        while year <= maxy:
                            corte1 = data.loc[data["Fecha"].dt.year == year]

                            minmes = corte1['Fecha'].dt.month.min()
                            maxmes = corte1['Fecha'].dt.month.max()

                            mes = minmes

                            while mes <= maxmes:

                                corte2 = corte1.loc[corte1["Fecha"].dt.month == mes]

                                pmin = corte2[corte2["Precipitación(mm)"] > 0.01]["Precipitación(mm)"].min()
                                pmax = corte2["Precipitación(mm)"].max()
                                ppro = corte2["Precipitación(mm)"].mean()

                                if "Temperatura Mínima(°C)" in corte2.columns:
                                    tmin = corte2["Temperatura Mínima(°C)"].min()

                                if "Temperatura Máxima(°C)" in data.columns:
                                    tmax = corte2["Temperatura Máxima(°C)"].max()

                                fech = str(year) + '/' + str(mes) + '/01'

                                if "Temperatura Mínima(°C)" in corte2.columns:
                                    datos.loc[len(datos.index)] = [fech, pmin, pmax, ppro, tmin, tmax]
                                else:
                                    datos.loc[len(datos.index)] = [fech, pmin, pmax, ppro]

                                mes = mes + 1
                                x = x + 1

                            year = year + 1

                        success, nchunks, nrows, _ = write_pandas(
                            conn = conn,
                            df = datos,
                            table_name = "estacion-" + npage['href'][17:].replace(".csv", ""),
                            database = page['href'][:-5].upper(),
                            schema = "DEMO",
                            auto_create_table = True,
                            overwrite = True,
                            use_logical_type= True
                        )

                        continue

            #Si solo queremos obtener una cantidad determinada de estaciones por categoría, cambiamos el valor de i en la siguiente linea, o podemos comentar directamente ambas lineas si queremos obtener los datos de todas las estaciones.    
            #if i == 20:
            #    break
            
        k = 0
        dlist = []
        y = 1
            
        for k in range(0, len(mData["Clave"])):
            if str(mData.loc[mData.index[k], "Mínimo Historico"]) == "":
                dlist.append(k)
                
        mData.drop(dlist, axis="index", inplace=True)

        years = pd.DataFrame(columns=["Fecha"])
        years["Fecha"] = pd.to_datetime(years['Fecha'])

        while yearmin <= yearmax:
            y = 1
            while y <= 12:
                years.loc[len(years.index)] = [str(yearmin) + '/' + str(y) + '/01']
                y = y + 1
            yearmin = yearmin + 1

        success, nchunks, nrows, _ = write_pandas(
            conn = conn,
            df = mData,
            table_name = "concentrado-estaciones-" + page['href'][:-5],
            database = page['href'][:-5].upper(),
            schema = "DEMO",
            auto_create_table = True,
            overwrite = True,
            use_logical_type= True
        )

        success, nchunks, nrows, _ = write_pandas(
            conn = conn,
            df = years,
            table_name = "years-" + page['href'][:-5],
            database = page['href'][:-5].upper(),
            schema = "DEMO",
            auto_create_table = True,
            overwrite = True,
            use_logical_type= True
        )

