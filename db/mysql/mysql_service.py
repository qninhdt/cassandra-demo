import os
import mysql.connector
from db.base_service import BaseService


class MySQLService(BaseService):

    def connect(self):
        self.db = mysql.connector.connect(
            database="cassandra_demo",
            user="root",
            password=os.getenv("MYSQL_ROOT_PASSWORD"),
            host="mysql",
            port=3306,
        )

        self.cursor = self.db.cursor()

        self.reset()

        print("Connected to MySQL database")

    def disconnect(self):
        print("Disconnected from MySQL database")
        self.db.close()

    def get_time_range(self):
        query = "SELECT MIN(date), MAX(date) FROM reports"

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        return result[0]

    def get_reports_by_sensor(self, sensor, date):
        query = "SELECT * FROM reports WHERE sensor = %s AND date = %s"

        self.cursor.execute(query, (sensor, date))
        result = self.cursor.fetchall()

        return result

    def get_reports_by_location(self, location, date):
        query = "SELECT * FROM reports WHERE location = %s AND date = %s"

        self.cursor.execute(query, (location, date))
        result = self.cursor.fetchall()

        return result

    def get_reports_by_environment(self, environment, date):
        query = "SELECT * FROM reports WHERE environment = %s AND date = %s"

        self.cursor.execute(query, (environment, date))
        result = self.cursor.fetchall()

        return result

    def create_report(self, report):
        query = (
            "INSERT INTO reports (sensor, location, environment, date, time, "
            "temperature, humidity, air_quality, pressure, wind_speed, "
            "wind_direction, gust_speed, dew_point, cloud_cover, visibility, "
            "precipitation_intensity, precipitation_type, uv_index, "
            "solar_radiation, soil_temperature, soil_moisture) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

        report["date"] = report["time"].strftime("%Y-%m-%d")

        self.cursor.execute(
            query,
            (
                report["sensor"],
                report["location"],
                report["environment"],
                report["date"],
                report["time"],
                report["temperature"],
                report["humidity"],
                report["air_quality"],
                report["pressure"],
                report["wind_speed"],
                report["wind_direction"],
                report["gust_speed"],
                report["dew_point"],
                report["cloud_cover"],
                report["visibility"],
                report["precipitation_intensity"],
                report["precipitation_type"],
                report["uv_index"],
                report["solar_radiation"],
                report["soil_temperature"],
                report["soil_moisture"],
            ),
        )
        self.db.commit()

    def reset(self):
        # drop reports table
        self.cursor.execute("DROP TABLE IF EXISTS reports")

        self.db.commit()

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                sensor VARCHAR(255),
                location VARCHAR(255),
                environment VARCHAR(255),
                date DATE,
                time TIMESTAMP,
                temperature DECIMAL(5,2),
                humidity DECIMAL(5,2),
                air_quality DECIMAL(5,2),
                pressure DECIMAL(7,2),
                wind_speed DECIMAL(5,2),
                wind_direction VARCHAR(50),
                gust_speed DECIMAL(5,2),
                dew_point DECIMAL(5,2),
                cloud_cover DECIMAL(5,2),
                visibility DECIMAL(5,2),
                precipitation_intensity DECIMAL(5,2),
                precipitation_type VARCHAR(50),
                uv_index DECIMAL(5,2),
                solar_radiation DECIMAL(7,2),
                soil_temperature DECIMAL(5,2),
                soil_moisture DECIMAL(5,2),
                INDEX (sensor)
            );
            """
        )

        self.db.commit()
