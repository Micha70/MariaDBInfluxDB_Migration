import sys
import io
import pymysql
from influxdb import InfluxDBClient
from datetime import datetime
import logging

# Standardausgabe auf UTF-8 einstellen
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Logging einrichten
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MariaDB-Verbindung
mariadb_connection = pymysql.connect(
    host="localhost",
    port=3307,
    user="...",
    password="...",
    database="..."
)
cursor = mariadb_connection.cursor()

# InfluxDB-Verbindung
influx_client = InfluxDBClient(
    host="localhost",
    port=8086,
    username="...",
    password="...",
    database="..."
)

# Sicherstellen, dass die Datenbank existiert
influx_client.create_database("Test_temp")

# SQL-Abfrage, um die Measurements und Einheiten aus statistics_meta zu holen
unit_query = """
    SELECT
        statistic_id,
        unit_of_measurement
    FROM
        statistics_meta
"""
cursor.execute(unit_query)
unit_mapping = {row[0]: row[1] for row in cursor.fetchall()}  # Mapping von statistic_id auf unit_of_measurement

batch_size = 20000  # Anzahl der Datensätze pro Batch
offset = 0

while True:
# Daten aus MariaDB lesen
# original query = "SELECT entity_id, state, last_updated FROM states"
# SQL-Abfrage mit Join
# logging.info("In while loop")

    query = f"""
        SELECT 
            statistics_meta.statistic_id,
            statistics_meta.unit_of_measurement,
            statistics_meta.has_mean,
            statistics_meta.has_sum,
            statistics.state,
            statistics.mean,
            statistics.start_ts
        FROM 
            statistics
        JOIN 
            statistics_meta ON 
            statistics.metadata_id = statistics_meta.id
        WHERE 
            statistics_meta.statistic_id IS NOT NULL
        ORDER BY statistics.start_ts DESC
        LIMIT {batch_size} OFFSET {offset};
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()

    if not rows:  # Wenn keine weiteren Daten vorhanden sind, Schleife beenden
        logging.info("No more data available")
        break


    #        statistics_meta.statistic_id IS NOT NULL



    # Daten in InfluxDB übertragen
    points = []
    invalid_points = []
    for row in rows:
        #entity_id, state, last_updated_ts, shared_attrs = row
        #statistic_id, unit, has_mean, has_sum, state, mean, start_ts = row
        
        logging.debug(f"Gefundener Datensatz: {row}")


        try:
            statistic_id, unit, has_mean, has_sum, state, mean, start_ts = row
            value = state if has_sum == 1 else mean if has_mean == 1 else None
            if value is None or statistic_id is None or start_ts is None:
                logging.warning(f"Ungültiger Datensatz: {row}")
                continue


             # Zeitstempel umwandeln
            time = datetime.utcfromtimestamp(start_ts).strftime('%Y-%m-%dT%H:%M:%SZ')
            # Maßeinheit aus unit_mapping basierend auf statistic_id extrahieren 
            unit = unit_mapping.get(statistic_id, "unknown")
            if unit == "unknown":
                logging.warnig(f"WARNUNG: Measurement ist `unknown` für statistic_id={statistic_id}. Überprüfe die Zuordnung von `unit_of_measurement`.")
                
            # Split statistic_id in domain und entity_id
            domain, entity_id = statistic_id.split('.', 1)
            
            # Punkt für InfluxDB erstellen
            point = {
                "measurement": unit if unit else "unknown",
                "tags": {
                    "domain": domain,
                    "entity_id": entity_id,
                    "friendly_name": " ",
                    "source": "HA"
                },
                "fields": {
                    "value": float(value)
                },
                "time": time
            }
            logging.debug(f"Measurement: {unit}, Tags: {{'statistic_id': {statistic_id}}}, Fields: {point['fields']}")

            points.append(point)
        except Exception as e:
            logging.error(f"Fehler beim Verarbeiten von Datensatz {row}: {e}")
            invalid_points.append(row)

    if points:
        # Punkte in InfluxDB schreiben (chunkweise)
        try:
            CHUNK_SIZE = 500
            for i in range(0, len(points), CHUNK_SIZE):
                chunk = points[i:i + CHUNK_SIZE]
                influx_client.write_points(chunk)
                logging.info(f"{len(chunk)} Punkte [{i}:{i + CHUNK_SIZE}] aus Batch {offset // batch_size + 1} migriert.")
        except Exception as e:
            logging.error(f"Fehler beim Schreiben in InfluxDB: {e}")
            for point in invalid_points:
                logging.error(f"Fehlerhafter Punkt: {point}")

    offset += batch_size  # Nächsten Batch verarbeiten

logging.info("Migration abgeschlossen!")

# Verbindungen schließen
cursor.close()
mariadb_connection.close()
influx_client.close()