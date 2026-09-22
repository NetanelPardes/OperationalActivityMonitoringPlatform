import json
import mysql.connector
import pandas as pd

with open("appsettings.json" , "r" , encoding="UTF-8") as file:
    settings = json.load(file)

sql_settings = settings["Mysql"]
stations_path = settings["Files"]["StationsPath"]

connection = mysql.connector.connect(
    host=sql_settings["Host"],
    port=int(sql_settings["Port"]),
    user=sql_settings["User"],
    password=sql_settings["Password"]
)

mycursor = connection.cursor()

mycursor.execute("CREATE DATABASE IF NOT EXISTS Activity_monitoring_platform_Db")

mycursor.execute("USE Activity_monitoring_platform_Db")

mycursor.execute("""
                CREATE TABLE IF NOT EXISTS Stations (
                Id VARCHAR(50) PRIMARY KEY,
                Name VARCHAR(100) NOT NULL,
                Sector VARCHAR(100) NOT NULL,
                Status VARCHAR(20) NOT NULL,
                CreatedAt DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """)

stations = pd.read_csv(stations_path)

query = """
    INSERT INTO Stations (Id, Name, Sector, Status)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        Name = VALUES(Name),
        Sector = VALUES(Sector),
        Status = VALUES(Status)
"""

for station in stations.itertuples(index=False):
    mycursor.execute( query,(station.station_id,station.name,station.sector,station.status))

connection.commit()

mycursor.close()
connection.close()

print(f"Successfully loaded {len(stations)} stations")
