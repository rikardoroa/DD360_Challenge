## DD360 Challenge

###### Airflow Docker Container

## Requisitos Previos

* 1 -> Descargar e instalar Docker Destop para windows o mac https://www.docker.com/products/docker-desktop/

## Instalacion del Container

* 1 -> para ejecutar la imagen de Airflow en docker utilizar el siguiente comando en consola **_docker-compose up airflow-init_**

* 2 -> luego de ejecutar el comando se crearan cuatro carpetas dentro de la carpeta de la app las cuales seran **_config, dags, logs, plugins_**

* 3 -> luego entrar a la carpeta **_config_** y editar el archivo de configuracion de Airflow **_airflow.cfg_** cambiando los valores de las siguientes variables:

     * 1. dagbag_import_timeout = 300
     * 2. enable_xcom_pickling = True

* 4 -> luego se debe copiar la carpeta **_requestapp_** y pegarla en la carpeta **_dags_** 

## Nota
* no se debe copiar la carpeta **_requestapp_**  dentro de la ruta **_config / dags_** , no se debe confundir con esta ruta
* por ningun motivo borrar las carpetas que se crean de manera automatica

* 5 -> luego se deben ejecutar los siguientes comandos en consola:
     * 1. **_docker-compose up airflow-webserver_**
     * 2. **_docker-compose up airflow-scheduler_** 
     * 3. **_docker-compose up airflow-worker_**

* 6 -> abrir el navegador en la direccion localhost:8080 y iniciar el dag **_request_data_**

* 7 -> el dag se ejecuta cada hora, en la primera ejecucion crea tres carpetas: 
      * 1. **_Current_** : en esta carpeta se crearan los archivos con la actualizacion mas reciente de la union con la tabla clima y municipio
      * 2. **_Average_temp_** : en esta carpeta se crearan los archivos con el promedio de las temperaturas maximas y minimas de las dos ultimas horas
      * 2. **_dailydata_** : en esta carpeta se crearan los archivos con la informacion historica


## Recomendaciones

* si se desean instalar paquetes adicionales se puede utilizar el comando **_docker exec -it container_id /bin/bash y luego ejecutar pip install paquete**
* otra manera de instalar paquetes adicionales es tambien utilizando la variable de entorno **__PIP_ADDITIONAL_REQUIREMENTS_** del archivo docker-compose

###### enjoy!
