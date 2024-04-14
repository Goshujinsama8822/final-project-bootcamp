#Codigo escrito por Guadalupe Fernando Escutia Rodriguez como parte del proyecto final del Bootcamp de Ingeniería en datos de CodigoFacilito. Este codigo es open source y es distribuido bajo una licencia GLP3.

#Importamos nuestras librerías
import requests
from bs4 import BeautifulSoup as bso
import pandas as pd
import numpy as np
import os

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
            
        #Creamos el directorio correspondiente si no existe
        if not os.path.exists(page['href'][:-5]):
            os.makedirs(page['href'][:-5])
            
        i = 0

        print(url + "/" + page['href'])
        
        #Iniciamos el segundo bucle de nuestro script
        for npage in newPages:
            
            #Si el enlace de descarga corresponde a un csv ejecutamos el siguiente código
            if str(npage['href'][-4:]) == ".csv":
                durl = url + "/" + npage['href']
                durl = durl.replace(" ", "%20")
                
                #Descomentar la siguiente sección si se quiere descargar los datos en bruto, solo tener en consideración que se trata de varios GBs de información (En un principio tenía pensado transformar los datos en local, pero al pensarlo un poco al final no me hizo sentido)
                
                #Intentamos obtener la información del archivo a descargar
                # print("Intentando obtener información del archivo: " + npage['href'] + "\n")
                
                # try:
                #     respuesta = requests.get(durl, verify=True)
                # except:
                #     print("No se ha podido obtener información del archivo: " + npage['href'] + "\n")
                #     exit()
                    
                #Intentamos escribir la información del archivo a descargar
                
                # print("Intentando guardar la información del archivo: " + npage['href'] + "\n")
                
                # try:
                #     with open(page['href'][:-5] + "/" + npage['href'][17:], mode="wb") as file:
                #         file.write(respuesta.content)
                # except:
                #     print("No se ha podido escribir la información del archivo: " + npage['href'] + "\n")
                #     exit()
                
                print(durl)
                
                if i == 0:
                    try:
                        mData = pd.DataFrame()
                        mData = pd.read_csv(durl, encoding = "ISO-8859-1")
                        mData["Minimo Historico"] = ""
                        mData["Máximo Historico"] = ""
                        mData["Promedio Historico"] = ""
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
                        i = i + 1
                    except pd.errors.ParserError as e:
                        raise SystemExit(e)
                
                j = 0
                
                for j in range(0, len(mData.iloc[:,1])):
                    if mData.iloc[j,1] == str(npage['href'][17:]).replace(".csv", ""):
                        print("Encontrado" + str(npage['href'][17:]).replace(".csv", "") + ", procesando...")
                        data['Precipitación(mm)'] = data['Precipitación(mm)'].replace("-", np.nan)
                        data['Precipitación(mm)'] = pd.to_numeric(data['Precipitación(mm)'], errors="coerce")
                        mData.iloc[j,12] = data['Precipitación(mm)'].min()
                        mData.iloc[j,13] = data['Precipitación(mm)'].max()
                        mData.iloc[j,14] = data['Precipitación(mm)'].mean()
                        continue
                
            # if i == 5:
            #     break
            
        k = 0
        dlist = []
            
        for k in range(0, len(mData["Minimo Historico"])):
            if mData.loc[mData.index[k], "Minimo Historico"] == "":
                dlist.append(k)
                
        mData.drop(dlist)
                        
        mData.to_csv(page['href'][:-5] + "-concentrado-" + page['href'][:-5] + ".csv")
