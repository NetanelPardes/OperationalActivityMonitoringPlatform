FROM python:3.13-slim

WORKDIR /app

COPY StationLoader/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY StationLoader/main.py .
COPY StationLoader/appsettings.json .

COPY Data/stations.csv ./Data/stations.csv

CMD ["python", "main.py"]