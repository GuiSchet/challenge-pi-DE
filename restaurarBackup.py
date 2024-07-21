import pyodbc
#Para este script termine utilizando la libreria pyodbc porque SQLalchemy me da un error de certificado que no pude resolver.
#El script genera una nueva base y restaura el backup Testing_ETL.bak que primero se copio en el container de Docker con los siguientes comandos:
#docker run --name sqlserver-container -e 'ACCEPT_EULA=Y' -e 'SA_PASSWORD=YourStrong!Passw0rd' -p 1433:1433 -d mcr.microsoft.com/mssql/server:2019-latest
#docker cp ./Testing_ETL.bak sqlserver-container:/var/opt/mssql/data/Testing_ETL.bak

# Datos de conexión.
server = 'localhost'
database = 'master'
username = 'sa'
password = 'YourStrong!Passw0rd'
backup_file = '/var/opt/mssql/data/Testing_ETL.bak'
new_database = 'NuevaDatabase'

# Crear la conexión.
connection_string = (
    f'DRIVER={{ODBC Driver 18 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password};'
    'TrustServerCertificate=yes;'
)
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

# Obtener nombres lógicos del archivo de respaldo
cursor.execute(f"RESTORE FILELISTONLY FROM DISK = '{backup_file}'")
file_list = cursor.fetchall()
print(file_list)

# Extraer nombres lógicos
logical_name_data = file_list[0].LogicalName
logical_name_log = file_list[1].LogicalName

# Restaurar la base de datos fuera de una transacción explícita
cursor.execute("USE master")  # Asegúrate de que estás en la base de datos master
connection.autocommit = True  # Deshabilita la autocommit para ejecutar la restauración fuera de una transacción explícita
restore_sql = f"""
RESTORE DATABASE [{new_database}] 
FROM DISK = '{backup_file}' 
WITH MOVE '{logical_name_data}' TO '/var/opt/mssql/data/{new_database}.mdf', 
MOVE '{logical_name_log}' TO '/var/opt/mssql/data/{new_database}_log.ldf'
"""
cursor.execute(restore_sql)

print(f"La base de datos {new_database} se ha restaurado (sin errores).")