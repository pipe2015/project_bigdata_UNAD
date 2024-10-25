# Proyecto Big Data UNAD - Aplicacion

Este projecto es para fines de practtica usando tecnologias y herramientas para procesar grandes volumenes de datos (Big Data)

# 1. Proyecto Python - Instalación y Configuración en Windows

Este documento proporciona los pasos necesarios para la instalación de Python y la configuración del entorno de desarrollo en Windows, utilizando un archivo `requirements.txt` para instalar las dependencias necesarias del proyecto.

## Instalar python
Este es el sistema operativo con el que estoy trabajando
- **Python 3.x**: Asegúrate de que tienes instalada una versión reciente de Python 3. Puedes descargar Python desde el sitio oficial: [https://www.python.org/downloads/](https://www.python.org/downloads/).
- **pip**: `pip` es el gestor de paquetes de Python. Generalmente, se instala automáticamente junto con Python. Verifica que esté disponible después de instalar Python.

## Pasos de instalación

### 1. Verificar la instalación de Python

Después de instalar Python, verifica que está correctamente instalado abriendo el **Símbolo del sistema** (cmd) y ejecutando los siguientes comandos:

```bash
python --version
```

### 2. Verificar la instalación del Gestor de paquetes de Python
---

Esto permite instalar y administrar paquetes de software escritos en Python.

```bash
pip --version
```

### 3. Instalar dependencias usando `requirements.txt`
---

 Instala las dependencias especificadas usadas del proyesto en el archivo `requirements.txt` utilizando el siguiente comando.

```bash
pip install -r requirements.txt
```

Lista de las dependencias instaladas.

```python
colorama==0.4.6
numpy==2.1.2
pandas==2.2.3
py4j==0.10.9.7
pyspark==3.5.3
python-dateutil==2.9.0.post0
pytz==2024.2
six==1.16.0
tqdm==4.66.5
tzdata==2024.2
```

### 4. Ejecutar el proyecto
---

Una vez que todo esté configurado, puedes ejecutar el proyecto utilizando el comando de Python.

```bash
python generate_datablognews_csv.py
```
---
Y Listo.

## Muestra la ejecucion del Proyecto para generar el archivo `.csv`

### Run Test Image
![alt text](https://github.com/pipe2015/project_bigdata_UNAD/blob/master/Images_project/03.png)

# Proyecto Python - Muestra y lectura de Datos

A continuacion, se muestra la generacion de los datos usando el Archivo `test_processing_file.csv`

![alt text](https://github.com/pipe2015/project_bigdata_UNAD/blob/master/Images_project/05.png)

# 2. Proyecto Python HDFS Apache Spark - Implementacion Fase 3

A continuación, se Muestra la ejecución de archivo `ckickstream_blognews_05.csv`  con todos los análisis de datos mostrados:

```bash
•	Número total de usuarios
•	Sesiones por dispositivo
•	Número total de cliks por usuario
•	Número total de clikss por artículo
•	Tiempo promedio que los usuarios pasan en las páginas
•	Videos vistos por categoria
•	Numero de articulos leídos por categoria
•	Analisis de la ubicacion geografica
•	Obtener la primera y última interacción por usuario en 2024
```

### Vemos las estadisticas básicas
---
![alt text](https://github.com/pipe2015/project_bigdata_UNAD/blob/master/Images_project/05-1.png)


### Run Test Analisis #1
---
![alt text](https://github.com/pipe2015/project_bigdata_UNAD/blob/master/Images_project/06-2.png)


### Run Test Analisis #2
---
![alt text](https://github.com/pipe2015/project_bigdata_UNAD/blob/master/Images_project/07-3.png)


### Run Test Analisis #3
---
![alt text](https://github.com/pipe2015/project_bigdata_UNAD/blob/master/Images_project/08-4.png)
