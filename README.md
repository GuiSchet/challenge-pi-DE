## challenge-pi-DE

### Proceso de Integración de Datos

#### Introducción
Este proyecto tiene como objetivo desarrollar un proceso automatizado para descargar e insertar datos en una base de datos SQL Server.

#### Solución Implementada
El proyecto se podía resolver por varios caminos, la optima en mi opinion sería: Cloud con Data Factory, Databricks y pypsark. Pero como no cuento con una cuenta personal de Azure opte por lo siguiente:

##### 1. **Montaje SQL server en Docker**
Monte una imagen de SQL server en Docker.
Copie el backup al container.
Cree una base de datos y restaure las tablas. Utilice el script [restaurarBackup.py](restaurarBackup.py)
En primera intancia utilice la libreria SQLAlchemy pero tuve varios errores con la conexión y termine utilizando la librería "pyodbc" que la recomendaban en stackoverflow.

##### 2. **Chequeo de tablas en la base de datos**
Revise si se cargaron las tablas correcamente con la jupyter notebook: [challenge_pi_consulting_notebook.ipynb](challenge_pi_consulting_notebook.ipynb)

##### 3. **Descarga, procesamiento e inserción a la bd**
Se desarrolla en el script principal. [main.py](main.py)

###### 1. **Descarga del Archivo CSV**
Se utilizó la biblioteca "requests" para descargar el archivo CSV desde un enlace proporcionado. El archivo se guarda localmente para su posterior procesamiento.

###### 2. **Procesamiento del Archivo CSV**
Se utilizó la biblioteca "pandas" para leer el archivo CSV y agregar una columna "FECHA_COPIA" con la fecha y hora actuales, indicando el momento de la carga de los datos.

###### 3. **Inserción de Datos en la Base de Datos**
Se utilizó "SQLAlchemy" para conectarse a la base de datos y realizar la inserción de datos. El script también incluye un registro de la cantidad de filas agregadas y una verificación por cantidad de filas (bastante simple pero me pareción los más practico).

###### 4. **Automatización del Proceso**
Como solución a este punto se planteó en el archivo el us de la biblioteca "schedule" para programar la ejecución del proceso todos los lunes. Que lo deje comentado.
Otra posible solución sería pasar esto a una notebook de Databricks y crear un pipeline con un Trigger en ADF.
También se podría utilizar crontab en linux.

###### 5. **Creación de Logger**
Utilizo la librería "logging" para guardar logs en cada ejecución, con distintas banderas para saber en que momento hay una falla.