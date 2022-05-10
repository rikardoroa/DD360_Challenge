#!/usr/bin/pyfile
import requests
import gzip
import json
import warnings
import pandas as pd
import os
from functools import wraps
from datetime import datetime
from requests.exceptions import ChunkedEncodingError


class geographic_data:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    url_d = 'https://smn.conagua.gob.mx/webservices/?method=1'

    df2 = pd.DataFrame()


    #inicializo variables
    def __init__(self, url_d=url_d, headers_=headers, payload_=[], df2 = df2):
        self.url_d = url_d
        self.headers_ = headers_
        self.df1 = pd.DataFrame()
        self.df2 = df2
        self.payload = payload_



    #decorador de la clase que hace el request de la api
    def response(self,**context):
        try:
            #se hace la llamada a la api y se descomprime el archivo y se carga en un json
            r = requests.get(url=self.url_d, headers=self.headers_, verify=False, stream=True)
            data = gzip.decompress(r.content).decode('utf-8').encode()
            #se carga la informacion del json en una lista para luego ser escritos en disco(I/O)
            data_dict = json.loads(data)
            self.payload.append(data_dict)
            context["ti"].xcom_push(key="self.payload", value=self.payload)
        except (ChunkedEncodingError, OSError):
            print("Connection Broken, Can not request information from the server!")
        

    @classmethod
    def write_dirs(cls):
        try:
            #se crean los directorios
            directories = ["dailydata", "Average_temp", "Current"]
            path = os.path.abspath("dags/")
            for index, item in enumerate(directories):
                if not os.path.exists(os.path.join(path, directories[index])):
                    os.mkdir(os.path.join(path, directories[index]))
        except FileExistsError:
            print("Directory already created!")
        return "all Directories created"


    def write_files(self,**context):
        try:
            self.payload = context.get("ti").xcom_pull(key="self.payload")
            if len(self.payload) == 0:
                return ("empy  list")
            
            if len(self.payload) > 0:
                # se accede a la lista que contiene los datos del json para ser escritos en Disco(I/O)
                self.df1 = self.payload[0]
                now = datetime.now()
                dt_string = now.strftime("%Y%d%H%M%S")

                #se establece la ruta en disco (directorio) en donde se escribiran los archivos json
                #el formato de salida del archivo tendra como nombre daily_data mas la fecha y hora de escritura

                path1 = os.path.abspath("dags/dailydata/daily_data" + dt_string + ".json")
                with open(path1, 'w') as d_json_file:
                    json.dump(self.df1, d_json_file)
        except (IndexError , OSError):
            print("The Files Can not be saved!")
        finally:
            print("all Files were saved!")

    #funcion decoradora que toma los archivos de las dos ultimas horas
    # creando un dataframe que calcula el promedio de las temperaturas minimas y maximas
    #@classmethod
    def processing_data(self,**context):
        try:
            #se guarda en una lista los archivos de las dos ultimas horas
            #que contienen toda la informacion del clima de los municipios
            files_ = []
            root_ = []
            path1 = os.path.abspath("dags/dailydata")
            for root, dirs, files in os.walk(path1, topdown=False):
                root_.append(root)
                for name in files:
                    files_.append(name)
            files_ = files_[-2:]

            #se lee la informacion de los dos ultimos archivos y se guarda en un solo Dataframe
            json_files = []
            for i in range(len(files_)):
                path2 = os.path.join(path1, files_[i])
                with open(path2, 'rt') as read_file:
                    json_text = json.load(read_file)
                    json_files.append(pd.DataFrame(json_text))
                    df2 = pd.concat(json_files).reset_index(drop=True)
                    self.df2 = df2
            context["ti"].xcom_push(key="self.df2", value=self.df2)
        except FileNotFoundError:
            print("File not found!")
        finally:
            print("Data processed successfully!")
        
        #return self.df2


    def merging_df_data(self, **context):
        try:
            self.df2 = context.get("ti").xcom_pull(key="self.df2")
            print(self.df2)
            # se eliminan las filas duplicadas del dataframe
            cols = []
            for col in self.df2.columns:
                cols.append(col)
            self.df2 = self.df2.drop_duplicates(subset=cols)
    
            for col in self.df2.columns:
                if 'tmin' and 'tmax' in col:
                    # se convierten las columnas tmax y tmin a float para realizar el calculo del promedio
                    self.df2[['tmin', 'tmax']] = self.df2[['tmin', 'tmax']].astype(str).apply(
                        lambda x: x.replace(',', '.')).astype(float)
            
                    # se hacen las transformaciones con group by y la funcion promedio(mean) para el calculo
                    self.df2 = self.df2.groupby(['nmun']).agg({'tmax': [('tmax', 'mean')], 'tmin': [('tmin', 'mean')]},
                                                    ascending=True). \
                        rename(columns={'tmax': 'tmax_avg', 'tmin': 'tmin_avg'}).reset_index()
                        
                    self.df2.columns = self.df2.columns.droplevel()
                    self.df2 = self.df2.rename(columns={'': 'nmun'})
                    # el dataframe resultante  del la operacion de los promedios
                    # se guarda en un archivo excel dentro de la carpeta "Average_Temp"
            
                else:
                    print("error")


            
            path = os.path.abspath("dags")
            now = datetime.now()
            dt_string = now.strftime("%Y%d%H%M%S")
            fullpath = os.path.join(path, "Average_temp")
            self.df2.to_excel(fullpath + "//" + dt_string + "_data.xlsx", header=True, index=False)

            
                    
        
        except FileNotFoundError:
            print("File not found!")
        finally:
            print("Data merged!")


    #funcion para unir los archivos en uno solo
    # en esta funcion se crea un dataframe para las transformaciones
    # el dataframe es exportado en un archivo excel y en json dentro de la carpeta Current
    @classmethod
    def join_data(cls):
        try:
            # listamos los archivos y directorios necesarios para procesar los datos
            path1 = os.path.abspath("dags/requestapp/data_municipios")
            path0 = os.path.abspath("dags/dailydata")
            dir_name = [dir_ for dir_ in os.listdir(path1)][-1]
            path2 = os.path.join(path1, dir_name)
            file_name = [file for file in os.listdir(path2)][-1]
            
            if len([file for file in os.listdir(path0)][-1]) == -1:
                print("error")

            else:
                
                json_file = [file for file in os.listdir(path0)][-1]
            
                # leemos los datos de los municipios
                path3 = os.path.join(path2, file_name)
                with open(path3, 'rt') as read_file:
                    df1 = pd.read_csv(read_file)

                # leemos los datos del ultimo json actualizado
                path4 = os.path.join(path0, json_file)
                with open(path4, 'rt') as json_file:
                    df2 = pd.read_json(json_file)

                # realizamos la union de los dos archivos en un DataFrame
                df3 = df1.merge(df2, left_on='Cve_Mun', right_on='idmun', how='inner')
                now = datetime.now()
                dt_string = now.strftime("%Y%d%H%M%S")

                # creamos el directorio Current para guardar el archivo final
                path = os.path.abspath("dags")
                fullpath = os.path.join(path, "Current")

                # escribimos el json final
                result = df3.to_json(orient="records")
                parsed = json.loads(result)
                path4 = os.path.join(fullpath + "//" + dt_string + "_mun_data.json")
                with open(path4, "w") as j_file:
                    json.dump(parsed, j_file)

                # escribimos el archivo excel
                df3.to_excel(fullpath + "//" + dt_string + "_mun_data.xlsx", header=True, index=False)
                return("all saved!")
        except FileNotFoundError:
            print("File not found")
        finally:
            print("Data merged!")