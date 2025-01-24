# MariaDBInfluxDB_Migration

This tool will help to migrate data from Maria DB to an InfluxDB used by Homeassistant.
I have used / tested with versions:
* Homeassistant Verison 2025.1.x
* InfluxDB 5.0.1
* MariaDB 2.7.2




Setup: 
1. `git clone <this repository> MariaDBInfluxDB_Migration`
2. `cd MariaDBInfluxDB_Migration`

3. Update now database name an credentials in script my_influxdbmigration.py for MariaDB and InfluxDB
     in mariadb_connection use, password, database
     in influx_client username, password, database
   
5. `python3 -m venv .venv`
6. `. .venv/bin/activate`
7. `pip install pymysql influxdb`
8. `python my_influxdbmigration.py`

Be patient ;-). Conversion is done automatically in batches, takes a while ....
