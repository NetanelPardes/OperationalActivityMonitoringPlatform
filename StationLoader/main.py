import json
import time
import mysql.connector
import pandas as pd

with open("appsettings.json", "r", encoding="utf-8") as file:
    settings = json.load(file)


sql_settings = settings["Mysql"]
stations_path = settings["Files"]["StationsPath"]

connection = None

for attempt in range(1, 21):
    try:
        connection = mysql.connector.connect(
            host=sql_settings["Host"],
            port=int(sql_settings["Port"]),
            database=sql_settings["Database"],
            user=sql_settings["User"],
            password=sql_settings["Password"]
        )

        print("Successfully connected to MySQL")
        break

    except mysql.connector.Error as error:
        print(f"MySQL is not ready. Attempt {attempt}/20: {error}")
        time.sleep(5)


if connection is None:
    raise RuntimeError("Could not connect to MySQL after 20 attempts")


cursor = connection.cursor()

cursor.execute("""
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
    cursor.execute(query,(station.station_id,station.name,station.sector,station.status))


connection.commit()
cursor.close()
connection.close()

print(f"Successfully loaded {len(stations)} stations")